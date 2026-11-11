//! T12: project creation is staged, hashed from the imported bytes, and leaves
//! no empty or partially completed project behind on failure.

use std::fs;
use std::path::{Path, PathBuf};

use polars::prelude::AnyValue;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use trivium_engine::ops::projects::{create_project, CreateProjectPayload};
use trivium_engine::project_io::read_project_dataframe;
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-project-create-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn write_file(root: &Path, name: &str, body: &str) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn payload(path: &Path) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().to_string(),
        description: None,
        delimiter: None,
        restore_annotations: None,
    }
}

fn entries(path: &Path) -> Vec<PathBuf> {
    if !path.exists() {
        return Vec::new();
    }
    fs::read_dir(path)
        .expect("failed to read directory")
        .map(|entry| entry.expect("failed to read directory entry").path())
        .collect()
}

fn assert_no_import_leftovers(root: &Path, store: &ProjectsStore) {
    assert!(
        store.all().is_empty(),
        "a failed import must not register a project"
    );
    assert!(
        entries(&root.join("projects")).is_empty(),
        "a failed import must not leave a project directory"
    );
    assert!(
        entries(&store.staging_root()).is_empty(),
        "a failed import must not leave a staging directory"
    );
}

#[test]
fn broken_json_leaves_no_registered_project_or_directories() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(&root, "broken.json", "{\"a\": 1,");

    let err = create_project(&store, payload(&path)).expect_err("broken JSON must fail");
    assert!(
        err.to_string().starts_with("Failed to parse JSON:"),
        "unexpected error: {}",
        err
    );

    assert_no_import_leftovers(&root, &store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn broken_csv_leaves_no_registered_project_or_directories() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(&root, "broken.csv", "a,b\n1,2\n3\n");

    let err = create_project(&store, payload(&path)).expect_err("broken CSV must fail");
    assert!(
        err.to_string().contains("Failed to parse the CSV data"),
        "unexpected error: {}",
        err
    );

    assert_no_import_leftovers(&root, &store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn successful_import_records_source_metadata_and_zero_based_row_ids() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let source_bytes = b"ts,msg\n2024-01-01T00:00:01,first\n2024-01-01T00:00:02,second\n2024-01-01T00:00:03,third\n";
    let path = root.join("events.csv");
    fs::write(&path, source_bytes).expect("failed to write fixture");

    let summary = create_project(&store, payload(&path)).expect("create failed");
    let meta = &summary.meta;

    let expected_sha256 = format!("{:x}", Sha256::digest(source_bytes));
    assert_eq!(meta.source_sha256.as_deref(), Some(expected_sha256.as_str()));
    assert_eq!(meta.source_file_name.as_deref(), Some("events.csv"));
    assert_eq!(meta.source_format.as_deref(), Some("csv"));
    assert_eq!(meta.source_size, Some(source_bytes.len() as u64));
    assert_eq!(meta.total_records, 3);

    // The source is opened read-only and is not rewritten or moved.
    assert_eq!(fs::read(&path).expect("failed to reread source"), source_bytes);

    let project_dir = store.project_dir(&meta.id);
    assert!(project_dir.join("data.parquet").exists());
    assert!(
        entries(&store.staging_root()).is_empty(),
        "a successful import must remove its staging directory"
    );

    // Source record numbers are the zero-based internal row ids.
    let df = read_project_dataframe(&project_dir.join("data.parquet"))
        .expect("failed to read project data");
    let row_ids = df.column("__rowid").expect("missing __rowid column");
    let row_ids: Vec<i64> = (0..row_ids.len())
        .map(|index| match row_ids.get(index).expect("missing row id") {
            AnyValue::Int64(value) => value,
            other => panic!("unexpected row id value: {:?}", other),
        })
        .collect();
    assert_eq!(row_ids, vec![0, 1, 2]);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn the_same_input_can_be_imported_again() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(
        &root,
        "events.jsonl",
        "{\"ts\": \"t1\"}\n{\"ts\": \"t2\"}\n{\"ts\": \"t3\"}\n",
    );

    let first = create_project(&store, payload(&path)).expect("first create failed");
    let second = create_project(&store, payload(&path)).expect("second create failed");

    assert_ne!(first.meta.id, second.meta.id);
    assert_eq!(store.all().len(), 2);
    assert_eq!(first.meta.source_sha256, second.meta.source_sha256);
    assert_eq!(first.meta.source_format.as_deref(), Some("jsonl"));
    assert_eq!(second.meta.source_format.as_deref(), Some("jsonl"));
    assert!(entries(&store.staging_root()).is_empty());

    let _ = fs::remove_dir_all(root);
}

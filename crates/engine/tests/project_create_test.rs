//! T12: project creation is staged, hashed from the imported bytes, and leaves
//! no empty or partially completed project behind on failure.

use std::fs;
use std::path::{Path, PathBuf};

use polars::prelude::AnyValue;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use trivium_engine::ops::projects::{
    create_project, delete_project, CreateProjectPayload, ProjectRequest,
};
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

    // `__rowid` stays zero-based and contiguous; `__trivium_source_record` points at
    // the physical source line (header is line 1, first data record line 2).
    let df = read_project_dataframe(&project_dir.join("data.parquet"))
        .expect("failed to read project data");
    let row_ids: Vec<i64> = (0..df.height())
        .map(|index| match df.column("__rowid").expect("__rowid").get(index) {
            Ok(AnyValue::Int64(value)) => value,
            other => panic!("unexpected row id value: {:?}", other),
        })
        .collect();
    assert_eq!(row_ids, vec![0, 1, 2]);
    let source_records: Vec<i64> = (0..df.height())
        .map(|index| match df.column("__trivium_source_record").expect("__trivium_source_record").get(index) {
            Ok(AnyValue::Int64(value)) => value,
            other => panic!("unexpected source record value: {:?}", other),
        })
        .collect();
    assert_eq!(source_records, vec![2, 3, 4]);

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn metadata_write_failure_keeps_the_existing_registry_intact() {
    use std::os::unix::fs::PermissionsExt;

    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(&root, "events.csv", "id,msg\n1,first\n");
    let summary = create_project(&store, payload(&path)).expect("create failed");

    let mut extra = summary.meta.clone();
    extra.id = Uuid::new_v4();
    extra.name = "unregistered".to_string();

    let original_permissions = fs::metadata(&root).expect("root metadata").permissions();
    let mut read_only = original_permissions.clone();
    read_only.set_mode(0o555);
    fs::set_permissions(&root, read_only).expect("make root read-only");
    let result = store.insert(extra);
    fs::set_permissions(&root, original_permissions).expect("restore root permissions");

    result.expect_err("insert must fail while the registry is read-only");
    assert_eq!(
        store.all().len(),
        1,
        "a failed registration must be rolled back in memory"
    );
    let registry: Vec<trivium_engine::models::ProjectMeta> = serde_json::from_slice(
        &fs::read(root.join("projects.json")).expect("read projects.json"),
    )
    .expect("projects.json must remain valid JSON");
    assert_eq!(registry.len(), 1);
    assert_eq!(registry[0].id, summary.meta.id);
    assert!(
        entries(&root)
            .iter()
            .all(|path| !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("projects.json.tmp."))),
        "a failed metadata write must remove its temporary file"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn failed_metadata_update_does_not_change_in_memory_state() {
    use std::os::unix::fs::PermissionsExt;

    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(&root, "events.csv", "id,msg\n1,first\n");
    let summary = create_project(&store, payload(&path)).expect("create failed");

    let original_permissions = fs::metadata(&root).expect("root metadata").permissions();
    let mut read_only = original_permissions.clone();
    read_only.set_mode(0o555);
    fs::set_permissions(&root, read_only).expect("make root read-only");
    let result = store.update_hidden_columns(&summary.meta.id, vec!["c000001".to_string()]);
    fs::set_permissions(&root, original_permissions).expect("restore root permissions");

    result.expect_err("metadata update must fail while the registry is read-only");
    assert!(
        store
            .find(&summary.meta.id)
            .expect("project")
            .hidden_columns
            .is_empty(),
        "a failed write must not commit the change in memory"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn failed_delete_keeps_the_project_and_its_directory() {
    use std::os::unix::fs::PermissionsExt;

    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(&root, "events.csv", "id,msg\n1,first\n");
    let summary = create_project(&store, payload(&path)).expect("create failed");
    let project_dir = store.project_dir(&summary.meta.id);

    let original_permissions = fs::metadata(&root).expect("root metadata").permissions();
    let mut read_only = original_permissions.clone();
    read_only.set_mode(0o555);
    fs::set_permissions(&root, read_only).expect("make root read-only");
    let result = delete_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    );
    fs::set_permissions(&root, original_permissions).expect("restore root permissions");

    result.expect_err("delete must fail while the registry is read-only");
    assert!(
        store.find(&summary.meta.id).is_some(),
        "a failed registry write must keep the project listed"
    );
    assert!(
        project_dir.join("data.parquet").exists(),
        "a failed registry write must keep the project directory"
    );

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

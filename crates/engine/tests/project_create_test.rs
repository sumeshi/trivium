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
use trivium_engine::storage::{load_setting_bytes, project_database_path};
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
    assert_eq!(
        meta.source_sha256.as_deref(),
        Some(expected_sha256.as_str())
    );
    assert_eq!(meta.source_file_name.as_deref(), Some("events.csv"));
    assert_eq!(meta.source_format.as_deref(), Some("csv"));
    assert_eq!(meta.source_size, Some(source_bytes.len() as u64));
    assert_eq!(meta.total_records, 3);

    // The source is opened read-only and is not rewritten or moved.
    assert_eq!(
        fs::read(&path).expect("failed to reread source"),
        source_bytes
    );

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
        .map(
            |index| match df.column("__rowid").expect("__rowid").get(index) {
                Ok(AnyValue::Int64(value)) => value,
                other => panic!("unexpected row id value: {:?}", other),
            },
        )
        .collect();
    assert_eq!(row_ids, vec![0, 1, 2]);
    let source_records: Vec<i64> = (0..df.height())
        .map(|index| {
            match df
                .column("__trivium_source_record")
                .expect("__trivium_source_record")
                .get(index)
            {
                Ok(AnyValue::Int64(value)) => value,
                other => panic!("unexpected source record value: {:?}", other),
            }
        })
        .collect();
    assert_eq!(source_records, vec![2, 3, 4]);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn corrupt_legacy_registry_reports_failure_and_preserves_original_and_backup() {
    let root = temp_root();
    fs::write(root.join("projects.json"), b"{ this is not json").expect("write corrupt registry");

    let error = match ProjectsStore::new(root.clone()) {
        Ok(_) => panic!("an unreadable registry must not be presented as an empty workspace"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("unreadable"),
        "unexpected error: {error}"
    );
    assert_eq!(
        fs::read(root.join("projects.json")).unwrap(),
        b"{ this is not json"
    );
    let backup_exists = entries(&root).iter().any(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("projects.json.corrupt."))
    });
    assert!(backup_exists, "the damaged registry must be preserved");
    for backup in entries(&root).into_iter().filter(|path| {
        path.file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("projects.json.corrupt.")
    }) {
        assert_eq!(fs::read(backup).unwrap(), b"{ this is not json");
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn failed_registration_keeps_existing_project_metadata_intact() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(&root, "events.csv", "id,msg\n1,first\n");
    let summary = create_project(&store, payload(&path)).expect("create failed");
    let mut extra = summary.meta.clone();
    extra.id = Uuid::new_v4();
    extra.name = "unregistered".to_string();
    let extra_id = extra.id;
    let directory = store.project_dir(&extra_id);
    load_setting_bytes(&directory, "fixture").expect("initialize a settings-only database");
    let connection = rusqlite::Connection::open(project_database_path(&directory)).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_registration BEFORE UPDATE OF metadata ON project_state
         BEGIN SELECT RAISE(ABORT, 'injected registration failure'); END;",
        )
        .expect("inject registration failure");

    store
        .insert(extra)
        .expect_err("registration must report the failed transaction");
    assert_eq!(store.try_all().expect("existing case list").len(), 1);
    assert_eq!(
        serde_json::to_value(store.try_find(&summary.meta.id).unwrap()).unwrap(),
        serde_json::to_value(Some(&summary.meta)).unwrap()
    );
    assert!(store.try_find(&extra_id).unwrap().is_none());
    drop(connection);
    drop(store);
    let reopened = ProjectsStore::new(root.clone())
        .expect("failed registration must not corrupt existing cases");
    assert_eq!(reopened.try_all().unwrap().len(), 1);
    assert_eq!(
        serde_json::to_value(reopened.try_find(&summary.meta.id).unwrap()).unwrap(),
        serde_json::to_value(Some(&summary.meta)).unwrap()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn failed_metadata_update_does_not_change_saved_state() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(&root, "events.csv", "id,msg\n1,first\n");
    let summary = create_project(&store, payload(&path)).expect("create failed");
    let connection =
        rusqlite::Connection::open(project_database_path(&store.project_dir(&summary.meta.id)))
            .unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_metadata BEFORE UPDATE OF metadata ON project_state
         BEGIN SELECT RAISE(ABORT, 'injected metadata failure'); END;",
        )
        .expect("inject metadata failure");
    store
        .update_hidden_columns(&summary.meta.id, vec!["c000001".to_string()])
        .expect_err("metadata update must report the failed transaction");
    assert_eq!(
        serde_json::to_value(store.try_find(&summary.meta.id).unwrap()).unwrap(),
        serde_json::to_value(Some(&summary.meta)).unwrap()
    );
    drop(connection);
    drop(store);
    let reopened = ProjectsStore::new(root.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(reopened.try_find(&summary.meta.id).unwrap()).unwrap(),
        serde_json::to_value(Some(&summary.meta)).unwrap()
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
    let parent = project_dir.parent().expect("project parent");
    let original = fs::metadata(parent).unwrap().permissions();
    let mut read_only = original.clone();
    read_only.set_mode(0o555);
    fs::set_permissions(parent, read_only).expect("make project parent read-only");
    let probe = parent.join(".delete-permission-probe");
    let enforced = fs::write(&probe, b"probe").is_err();
    if !enforced {
        fs::set_permissions(parent, original).expect("restore parent permissions");
        fs::remove_file(probe).unwrap();
        eprintln!("skipping rename failure injection: directory permissions are not enforced");
        let _ = fs::remove_dir_all(root);
        return;
    }
    let result = delete_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    );
    fs::set_permissions(parent, original).expect("restore parent permissions");
    result.expect_err("delete must fail if the original case cannot be renamed to trash");
    assert!(
        store.try_find(&summary.meta.id).unwrap().is_some(),
        "the case must remain listed"
    );
    assert!(
        project_dir.join("data.parquet").exists(),
        "the evidence must remain intact"
    );
    assert!(
        project_database_path(&project_dir).exists(),
        "the canonical metadata must remain intact"
    );
    drop(store);
    let reopened = ProjectsStore::new(root.clone()).unwrap();
    assert!(reopened.try_find(&summary.meta.id).unwrap().is_some());
    assert!(project_dir.join("data.parquet").exists());
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

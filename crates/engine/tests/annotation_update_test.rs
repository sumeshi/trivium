//! T13: a single annotation update does not read project data.
//!
//! The update path reads only the target row's annotation, updates the
//! whole-project count by the difference for that row, and returns the entry
//! plus a process-local revision.

use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::models::UpdateFlagResponse;
use trivium_engine::ops;
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::project_io::{project_data_reads, reset_project_data_reads};
use trivium_engine::storage::{close_flags_db, load_flag};
use trivium_engine::store::ProjectsStore;

fn write_fixture(root: &Path, rows: usize) -> PathBuf {
    let path = root.join("annotations.csv");
    let mut body = String::from("id,value\n");
    for index in 0..rows {
        body.push_str(&format!("{index},value-{index}\n"));
    }
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn setup(rows: usize) -> (PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-annotation-update-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_fixture(&root, rows);
    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project");
    (root, store, summary.meta.id)
}

fn update(
    store: &ProjectsStore,
    id: Uuid,
    row_index: usize,
    flag: Option<&str>,
    memo: Option<Option<&str>>,
    tags: Option<Vec<&str>>,
    bookmarked: Option<bool>,
) -> Result<UpdateFlagResponse, trivium_engine::error::AppError> {
    update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index,
            flag: flag.map(str::to_string),
            memo: memo.map(|value| value.map(str::to_string)),
            tags: tags.map(|values| values.into_iter().map(str::to_string).collect()),
            bookmarked,
        },
    )
}

fn flags_path(store: &ProjectsStore, id: Uuid) -> PathBuf {
    store.project_dir(&id).join("flags.json")
}

#[test]
fn flag_memo_and_bookmark_updates_do_not_read_project_data() {
    let (root, store, id) = setup(20);

    reset_project_data_reads();
    update(&store, id, 0, Some("safe"), None, None, None).expect("flag update failed");
    assert_eq!(
        project_data_reads(),
        0,
        "a flag update must not read project data"
    );

    update(&store, id, 1, None, Some(Some("note")), None, None).expect("memo update failed");
    assert_eq!(
        project_data_reads(),
        0,
        "a memo update must not read project data"
    );

    update(&store, id, 2, None, None, None, Some(true)).expect("bookmark update failed");
    assert_eq!(
        project_data_reads(),
        0,
        "a bookmark update must not read project data"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn updating_one_row_does_not_touch_other_annotations() {
    let (root, store, id) = setup(20);
    let path = flags_path(&store, id);

    update(&store, id, 0, Some("suspicious"), None, None, None).expect("row 0 update failed");
    update(&store, id, 1, Some("critical"), None, None, None).expect("row 1 update failed");
    let before = load_flag(&path, 1)
        .expect("failed to read row 1")
        .expect("row 1 entry missing");

    update(&store, id, 0, None, Some(Some("changed")), None, None)
        .expect("row 0 memo update failed");

    let after = load_flag(&path, 1)
        .expect("failed to read row 1")
        .expect("row 1 entry missing");
    assert_eq!(before, after, "the other row's entry must be unchanged");

    let updated = load_flag(&path, 0)
        .expect("failed to read row 0")
        .expect("row 0 entry missing");
    assert_eq!(updated.flag, "suspicious");
    assert_eq!(updated.memo.as_deref(), Some("changed"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn flagged_count_changes_by_difference_and_revision_increases() {
    let (root, store, id) = setup(10);

    let first = update(&store, id, 0, Some("safe"), None, None, None).expect("update failed");
    let second = update(&store, id, 1, Some("suspicious"), None, None, None).expect("update failed");
    let third = update(&store, id, 2, Some("critical"), None, None, None).expect("update failed");
    assert_eq!(first.flagged_records, 1);
    assert_eq!(second.flagged_records, 2);
    assert_eq!(third.flagged_records, 3);

    let added = update(&store, id, 3, Some("safe"), None, None, None).expect("update failed");
    assert_eq!(added.flagged_records, 4);
    assert!(added.revision > third.revision);

    let cleared = update(&store, id, 3, Some(""), None, None, None).expect("update failed");
    assert!(cleared.entry.is_none(), "the cleared row had no other annotation");
    assert_eq!(cleared.flagged_records, 3);
    assert!(cleared.revision > added.revision);

    let memo_only =
        update(&store, id, 4, None, Some(Some("memo")), None, None).expect("update failed");
    assert_eq!(memo_only.flagged_records, 3);
    assert!(memo_only.revision > cleared.revision);

    let meta = store.find(&id).expect("project metadata missing");
    assert_eq!(meta.flagged_records, 3);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn row_index_outside_the_project_is_rejected() {
    let (root, store, id) = setup(10);

    let error = update(&store, id, 10, Some("safe"), None, None, None)
        .expect_err("row 10 must be out of range");
    assert!(
        error.to_string().contains("Row index out of range."),
        "unexpected error: {error}"
    );
    assert_eq!(
        load_flag(&flags_path(&store, id), 10).expect("failed to read row 10"),
        None
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn annotations_and_flagged_count_survive_reopening_the_store() {
    let (root, store, id) = setup(20);

    update(
        &store,
        id,
        0,
        Some("suspicious"),
        Some(Some("note")),
        Some(vec!["pc"]),
        Some(true),
    )
    .expect("row 0 update failed");
    update(&store, id, 1, Some("safe"), None, None, None).expect("row 1 update failed");

    let project_dir = store.project_dir(&id);
    let path = flags_path(&store, id);
    let expected = load_flag(&path, 0)
        .expect("failed to read row 0")
        .expect("row 0 entry missing");
    close_flags_db(&project_dir);
    drop(store);

    let reopened = ProjectsStore::new(root.clone()).expect("failed to reopen store");
    let meta = reopened.find(&id).expect("project metadata missing after reopen");
    assert_eq!(meta.flagged_records, 2);
    assert_eq!(
        load_flag(&flags_path(&reopened, id), 0).expect("failed to read row 0"),
        Some(expected)
    );
    assert_eq!(
        load_flag(&flags_path(&reopened, id), 1)
            .expect("failed to read row 1")
            .expect("row 1 entry missing")
            .flag,
        "safe"
    );

    let _ = fs::remove_dir_all(root);
}

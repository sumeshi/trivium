//! Per-project view state: independent from annotations, atomic on disk.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::json;
use uuid::Uuid;

use trivium_engine::models::{ViewState, VIEW_STATE_VERSION};
use trivium_engine::ops;
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::projects::{self, CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::view::{
    load_view_state, save_view_state, SaveViewStatePayload, ViewStateRequest,
};
use trivium_engine::storage::{load_setting_bytes, project_database_path, save_setting_bytes};
use trivium_engine::store::ProjectsStore;

mod common;

fn create_project(store: &ProjectsStore, path: &Path) -> Uuid {
    ops::projects::create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id
}

fn write_csv(path: &Path, rows: usize) {
    let mut text = String::from("host,user,proc\n");
    for index in 0..rows {
        text.push_str(&format!(
            "WS{:02},user{},proc{}\n",
            index + 1,
            index % 3,
            index
        ));
    }
    fs::write(path, text).expect("failed to write fixture");
}

fn setup() -> (PathBuf, ProjectsStore, Uuid, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-view-state-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");

    let path_a = root.join("a.csv");
    write_csv(&path_a, 6);
    let path_b = root.join("b.csv");
    write_csv(&path_b, 4);

    let id_a = create_project(&store, &path_a);
    let id_b = create_project(&store, &path_b);
    (root, store, id_a, id_b)
}

fn load(store: &ProjectsStore, project_id: Uuid) -> Option<ViewState> {
    load_view_state(store, ViewStateRequest { project_id })
        .expect("failed to load view state")
        .state
}

fn load_status(store: &ProjectsStore, project_id: Uuid) -> String {
    load_view_state(store, ViewStateRequest { project_id })
        .expect("failed to load view state")
        .status
}

fn save(store: &ProjectsStore, project_id: Uuid, state: &ViewState) {
    save_view_state(
        store,
        SaveViewStatePayload {
            project_id,
            state: state.clone(),
        },
    )
    .expect("failed to save view state");
}

fn sample_state() -> ViewState {
    let mut widths = HashMap::new();
    widths.insert("c000001".to_string(), 176.0);
    widths.insert("c000002".to_string(), 324.5);
    ViewState {
        version: VIEW_STATE_VERSION,
        search: "powershell".to_string(),
        search_columns: vec!["c000002".to_string()],
        condition: Some(json!({
            "type": "group",
            "id": "root",
            "kind": "and",
            "collapsed": false,
            "children": [{
                "type": "condition",
                "id": "leaf-1",
                "column": "c000003",
                "op": "eq",
                "value": "proc1",
                "negated": true
            }]
        })),
        flag_filter: "suspicious".to_string(),
        bookmarked_only: true,
        sort_key: Some("c000001".to_string()),
        sort_direction: "desc".to_string(),
        hidden_columns: vec!["c000004".to_string()],
        column_widths: widths,
        column_order: Some(vec!["c000002".to_string(), "c000001".to_string(), "c000003".to_string()]),
        column_formats: Some(HashMap::from([("c000003".to_string(), "iso".to_string())])),
        frozen_through: None,
        logical_row: 17,
    }
}

#[test]
fn view_state_round_trips_all_fields() {
    let (root, store, id, _) = setup();
    let state = sample_state();

    save(&store, id, &state);
    let loaded = load(&store, id).expect("expected saved view state");

    assert_eq!(loaded, state);
    assert_eq!(loaded.version, VIEW_STATE_VERSION);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn broken_view_state_does_not_block_project_or_annotations() {
    let (root, store, id, _) = setup();
    let flags = ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: Some("safe".to_string()),
            memo: Some(Some("keep me".to_string())),
            tags: None,
            bookmarked: Some(true),
        },
    )
    .expect("failed to annotate row");
    assert!(flags.entry.is_some());

    save_setting_bytes(&store.project_dir(&id), "view_state", b"{ this is not json")
        .expect("inject malformed canonical view state");

    assert!(load(&store, id).is_none());
    assert_eq!(load_status(&store, id), "unreadable");
    let loaded = projects::load_project(&store, ProjectRequest { project_id: id })
        .expect("a broken view state must not block loading the project");
    assert_eq!(loaded.project.meta.total_records, 6);
    assert_eq!(loaded.initial_rows.len(), 6);
    assert_eq!(loaded.initial_rows[0].flag, "safe");
    assert_eq!(loaded.initial_rows[0].memo.as_deref(), Some("keep me"));
    assert!(loaded.initial_rows[0].bookmarked);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn newer_view_state_is_ignored_and_not_overwritten() {
    let (root, store, id, _) = setup();
    let project_dir = store.project_dir(&id);
    let content = serde_json::to_vec(&json!({
        "version": VIEW_STATE_VERSION + 1,
        "search": "future",
        "logicalRow": 99
    }))
    .expect("failed to serialize");
    save_setting_bytes(&project_dir, "view_state", &content)
        .expect("write future canonical view state");

    assert!(load(&store, id).is_none());
    assert_eq!(load_status(&store, id), "newer");
    // A save from an old frontend must not replace the newer file either.
    let error = save_view_state(
        &store,
        SaveViewStatePayload {
            project_id: id,
            state: sample_state(),
        },
    )
    .expect_err("an older version must not overwrite a newer view state");
    assert!(
        error.to_string().contains("newer version"),
        "unexpected error: {error}"
    );
    let after = load_setting_bytes(&project_dir, "view_state")
        .expect("read canonical view state")
        .expect("future view state should remain");
    assert_eq!(after, content);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn atomic_save_replaces_canonical_content_without_legacy_sidecars() {
    let (root, store, id, _) = setup();
    let mut first = sample_state();
    first.search = "first".to_string();
    save(&store, id, &first);

    let project_dir = store.project_dir(&id);
    assert!(project_database_path(&project_dir).exists());
    assert!(!project_dir.join("view_state.json").exists());
    assert_eq!(
        load_setting_bytes(&project_dir, "view_state")
            .unwrap()
            .unwrap(),
        serde_json::to_vec(&first).unwrap()
    );

    let mut second = sample_state();
    second.search = "second".to_string();
    save(&store, id, &second);
    assert_eq!(
        load(&store, id).expect("expected view state").search,
        "second"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn failed_save_keeps_existing_canonical_view_state() {
    let (root, store, id, _) = setup();
    let mut original = sample_state();
    original.search = "keep".to_string();
    save(&store, id, &original);
    let project_dir = store.project_dir(&id);
    let connection = rusqlite::Connection::open(project_database_path(&project_dir)).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_view BEFORE UPDATE OF value ON settings
         WHEN OLD.key='view_state'
         BEGIN SELECT RAISE(ABORT, 'injected view state failure'); END;",
        )
        .expect("inject view state write failure");
    let mut changed = original.clone();
    changed.search = "cannot-write".to_string();
    save_view_state(
        &store,
        SaveViewStatePayload {
            project_id: id,
            state: changed,
        },
    )
    .expect_err("save must report the failed transaction");
    assert_eq!(
        load(&store, id).expect("original view state must remain"),
        original
    );
    assert_eq!(
        load_setting_bytes(&project_dir, "view_state")
            .unwrap()
            .unwrap(),
        serde_json::to_vec(&original).unwrap()
    );
    assert!(!project_dir.join("view_state.json").exists());
    drop(connection);
    drop(store);
    let reopened = ProjectsStore::new(root.clone()).unwrap();
    assert_eq!(
        load(&reopened, id).expect("original view after reopen"),
        original
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn legacy_view_state_migrates_once_and_preserves_the_source_file() {
    let (root, store, id, _) = setup();
    let path = store.project_dir(&id).join("view_state.json");
    let original = sample_state();
    let bytes = serde_json::to_vec(&original).unwrap();
    fs::write(&path, &bytes).expect("write legacy view");
    assert_eq!(load(&store, id).expect("migrated view"), original);
    assert_eq!(fs::read(&path).unwrap(), bytes);

    let mut changed = original.clone();
    changed.search = "canonical changes".to_string();
    save(&store, id, &changed);
    assert_eq!(load(&store, id).unwrap(), changed);
    assert_eq!(
        fs::read(&path).unwrap(),
        bytes,
        "migration source must remain untouched"
    );
    drop(store);
    let reopened = ProjectsStore::new(root.clone()).unwrap();
    assert_eq!(
        load(&reopened, id).unwrap(),
        changed,
        "legacy sidecar must not replace canonical edits"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn projects_keep_independent_view_state() {
    let (root, store, id_a, id_b) = setup();
    let mut state_a = sample_state();
    state_a.search = "only-a".to_string();
    state_a.logical_row = 3;
    let mut state_b = sample_state();
    state_b.search = "only-b".to_string();
    state_b.flag_filter = "critical".to_string();
    state_b.logical_row = 1;

    save(&store, id_a, &state_a);
    save(&store, id_b, &state_b);

    assert_eq!(load(&store, id_a).expect("view A").search, "only-a");
    assert_eq!(load(&store, id_b).expect("view B").search, "only-b");
    assert_eq!(load(&store, id_a).expect("view A").logical_row, 3);
    assert_eq!(load(&store, id_b).expect("view B").logical_row, 1);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn deleting_a_project_removes_its_view_state() {
    let (root, store, id, _) = setup();
    save(&store, id, &sample_state());
    let project_dir = store.project_dir(&id);
    let view_path = project_database_path(&project_dir);
    assert!(view_path.exists());
    assert!(load_setting_bytes(&project_dir, "view_state")
        .unwrap()
        .is_some());

    projects::delete_project(&store, ProjectRequest { project_id: id })
        .expect("failed to delete project");

    assert!(!view_path.exists());
    assert!(!project_dir.exists());
    assert!(load_view_state(&store, ViewStateRequest { project_id: id }).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn column_ids_are_stable_for_saved_widths() {
    // Saved widths and search scopes use the same internal column ids as query
    // and sort keys; this guards a legacy-name regression.
    let (root, store, id, _) = setup();
    let ids = common::column_ids(&store, id);
    let host = common::id(&ids, "host");
    let mut state = sample_state();
    state.search_columns = vec![host.clone()];
    state.sort_key = Some(host.clone());
    state.column_widths.insert(host.clone(), 222.0);
    save(&store, id, &state);

    let loaded = load(&store, id).expect("view state");
    assert!(loaded.search_columns.contains(&host));
    assert_eq!(loaded.sort_key.as_deref(), Some(host.as_str()));
    assert_eq!(loaded.column_widths.get(&host), Some(&222.0));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn frozen_boundary_and_explicit_unfreeze_round_trip_without_changing_legacy_default() {
    let (root, store, id_a, id_b) = setup();
    let legacy: ViewState = serde_json::from_value(json!({"version": 1})).unwrap();
    assert_eq!(legacy.frozen_through, None);
    assert!(serde_json::to_value(&legacy)
        .unwrap()
        .get("frozenThrough")
        .is_none());

    let mut state = sample_state();
    state.frozen_through = Some("c000003".to_string());
    state.hidden_columns = vec!["c000002".into(), "c000003".into()];
    save(&store, id_a, &state);
    assert_eq!(load(&store, id_a), Some(state.clone()));
    assert!(
        load(&store, id_b).is_none(),
        "frozen columns are case-specific"
    );
    drop(store);
    let reopened = ProjectsStore::new(root.clone()).unwrap();
    assert_eq!(
        load(&reopened, id_a).unwrap().frozen_through.as_deref(),
        Some("c000003")
    );
    state.frozen_through = Some(String::new());
    save(&reopened, id_a, &state);
    assert_eq!(
        load(&reopened, id_a).unwrap().frozen_through.as_deref(),
        Some("")
    );
    assert_eq!(
        serde_json::to_value(&state).unwrap()["frozenThrough"],
        json!("")
    );
    let _ = fs::remove_dir_all(root);
}

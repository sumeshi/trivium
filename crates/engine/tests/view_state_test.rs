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

    fs::write(
        store.project_dir(&id).join("view_state.json"),
        b"{ this is not json",
    )
    .expect("failed to corrupt view state");

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
    let path = store.project_dir(&id).join("view_state.json");
    let content = serde_json::to_vec(&json!({
        "version": VIEW_STATE_VERSION + 1,
        "search": "future",
        "logicalRow": 99
    }))
    .expect("failed to serialize");
    fs::write(&path, &content).expect("failed to write future view state");

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
    let after = fs::read(&path).expect("future view state should remain");
    assert_eq!(after, content);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn atomic_save_replaces_content_and_leaves_no_temp_file() {
    let (root, store, id, _) = setup();
    let mut first = sample_state();
    first.search = "first".to_string();
    save(&store, id, &first);

    let project_dir = store.project_dir(&id);
    let temp_prefix = "view_state.json.tmp.";
    let leftovers: Vec<String> = fs::read_dir(&project_dir)
        .expect("failed to list project dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(temp_prefix))
        .collect();
    assert!(leftovers.is_empty(), "temporary files left: {leftovers:?}");

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
fn failed_save_keeps_existing_view_state_and_removes_temp() {
    let (root, store, id, _) = setup();
    let mut original = sample_state();
    original.search = "keep".to_string();
    save(&store, id, &original);

    let project_dir = store.project_dir(&id);
    let view_path = project_dir.join("view_state.json");

    // A read-only project directory makes the temporary write fail. On a
    // system that ignores the permission (for example, root) the check below
    // decides whether this scenario is testable.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let original_permissions = fs::metadata(&project_dir)
            .expect("failed to read project dir metadata")
            .permissions();
        let mut read_only = original_permissions.clone();
        read_only.set_mode(0o555);
        fs::set_permissions(&project_dir, read_only).expect("failed to make project dir read-only");

        let probe = project_dir.join(".write-probe");
        let enforced = fs::write(&probe, b"x").is_err();
        if enforced {
            let mut changed = original.clone();
            changed.search = "cannot-write".to_string();
            let result = save_view_state(
                &store,
                SaveViewStatePayload {
                    project_id: id,
                    state: changed,
                },
            );
            assert!(result.is_err(), "save should fail in a read-only directory");
            assert_eq!(
                load(&store, id).expect("existing view state should remain"),
                original
            );
            let leftovers: Vec<String> = fs::read_dir(&project_dir)
                .expect("failed to list project dir")
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.file_name().to_string_lossy().to_string())
                .filter(|name| name.starts_with("view_state.json.tmp."))
                .collect();
            assert!(leftovers.is_empty(), "temporary files left: {leftovers:?}");
            assert!(view_path.exists());
        } else {
            let _ = fs::remove_file(&probe);
            eprintln!("skipping read-only failure check: permissions not enforced");
        }

        fs::set_permissions(&project_dir, original_permissions)
            .expect("failed to restore project dir permissions");
    }

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
    let view_path = store.project_dir(&id).join("view_state.json");
    assert!(view_path.exists());

    projects::delete_project(&store, ProjectRequest { project_id: id })
        .expect("failed to delete project");

    assert!(!view_path.exists());
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

//! Differential comparison by key columns, preserving duplicate counts.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use csv::ReaderBuilder;
use uuid::Uuid;

use trivium_engine::ops;
use trivium_engine::ops::diff::{diff_projects, DiffProjectsPayload, DiffResult};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::store::ProjectsStore;

mod common;

fn create_project(store: &ProjectsStore, path: &std::path::Path) -> Uuid {
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

fn setup() -> (PathBuf, ProjectsStore, Uuid, Uuid, std::collections::HashMap<String, String>) {
    let root = std::env::temp_dir().join(format!("trivium-diff-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");

    let path_a = root.join("a.csv");
    let a = "ts,host,user,proc\n\
2024-01-01T00:00:01,WS01,alice,powershell\n\
2024-01-01T00:00:02,WS02,alice,powershell\n\
2024-01-01T00:00:03,WS03,alice,powershell\n\
2024-01-01T00:00:04,WS04,bob,cmd\n\
2024-01-01T00:00:05,WS05,carol,explorer\n";
    fs::write(&path_a, a).expect("failed to write fixture A");

    let path_b = root.join("b.csv");
    let b = "ts,host,user,proc\n\
2024-01-01T00:00:11,WS01,alice,powershell\n\
2024-01-01T00:00:12,WS02,bob,cmd\n\
2024-01-01T00:00:13,WS03,dave,powershell\n";
    fs::write(&path_b, b).expect("failed to write fixture B");

    let id_a = create_project(&store, &path_a);
    let id_b = create_project(&store, &path_b);
    let ids = common::column_ids(&store, id_a);
    (root, store, id_a, id_b, ids)
}

fn payload(
    id_a: Uuid,
    id_b: Uuid,
    ids: &std::collections::HashMap<String, String>,
    destination: Option<PathBuf>,
) -> DiffProjectsPayload {
    DiffProjectsPayload {
        project_id_a: id_a,
        project_id_b: id_b,
        key_columns: vec![common::id(ids, "user"), common::id(ids, "proc")],
        destination: destination.map(|path| path.to_string_lossy().to_string()),
    }
}

fn find_entry<'a>(
    result: &'a DiffResult,
    ids: &std::collections::HashMap<String, String>,
    user: &str,
    proc: &str,
) -> &'a trivium_engine::ops::diff::DiffEntry {
    let user_id = common::id(ids, "user");
    let proc_id = common::id(ids, "proc");
    result
        .entries
        .iter()
        .find(|entry| {
            entry.key.get(&user_id).and_then(|value| value.as_str()) == Some(user)
                && entry.key.get(&proc_id).and_then(|value| value.as_str()) == Some(proc)
        })
        .unwrap_or_else(|| panic!("entry not found: {user}/{proc}"))
}

#[test]
fn diff_preserves_duplicate_counts() {
    let (_root, store, id_a, id_b, ids) = setup();
    let result = diff_projects(&store, payload(id_a, id_b, &ids, None)).expect("diff failed");

    let expected = HashMap::from([
        ("only_a".to_string(), 1usize),
        ("only_b".to_string(), 1usize),
        ("changed".to_string(), 1usize),
        ("same".to_string(), 1usize),
    ]);
    assert_eq!(result.status_counts, expected);

    let alice = find_entry(&result, &ids, "alice", "powershell");
    assert_eq!(alice.count_a, 3);
    assert_eq!(alice.count_b, 1);
    assert_eq!(alice.status, "changed");

    let carol = find_entry(&result, &ids, "carol", "explorer");
    assert_eq!(carol.status, "only_a");
    assert_eq!((carol.count_a, carol.count_b), (1, 0));

    let dave = find_entry(&result, &ids, "dave", "powershell");
    assert_eq!(dave.status, "only_b");
    assert_eq!((dave.count_a, dave.count_b), (0, 1));

    let bob = find_entry(&result, &ids, "bob", "cmd");
    assert_eq!(bob.status, "same");
    assert_eq!((bob.count_a, bob.count_b), (1, 1));

    let _ = fs::remove_dir_all(_root);
}

#[test]
fn diff_writes_csv_in_status_order() {
    let (root, store, id_a, id_b, ids) = setup();
    let destination = root.join("diff.csv");
    diff_projects(&store, payload(id_a, id_b, &ids, Some(destination.clone()))).expect("diff failed");
    assert!(destination.exists(), "diff CSV was not written");

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(&destination)
        .expect("failed to open diff CSV");
    let headers: Vec<&str> = reader
        .headers()
        .expect("failed to read headers")
        .iter()
        .collect();
    let user_id = common::id(&ids, "user");
    let proc_id = common::id(&ids, "proc");
    assert_eq!(
        headers,
        vec!["status", "count_a", "count_b", user_id.as_str(), proc_id.as_str()]
    );

    let rows: Vec<Vec<String>> = reader
        .records()
        .map(|record| {
            record
                .expect("failed to read record")
                .iter()
                .map(|field| field.to_string())
                .collect()
        })
        .collect();
    assert_eq!(rows.len(), 4, "header plus four entries");
    assert!(
        rows.iter().any(|row| {
            row == &vec![
                "changed".to_string(),
                "3".to_string(),
                "1".to_string(),
                "alice".to_string(),
                "powershell".to_string(),
            ]
        }),
        "changed row with preserved counts is missing: {rows:?}"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn diff_reports_missing_key_column() {
    let (root, store, id_a, id_b, ids) = setup();
    let mut request = payload(id_a, id_b, &ids, None);
    request.key_columns.push("not_exists".to_string());
    let error = diff_projects(&store, request).expect_err("missing column must fail");
    assert!(
        error.to_string().contains("not_exists"),
        "unexpected error: {error}"
    );

    let _ = fs::remove_dir_all(root);
}

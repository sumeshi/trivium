//! Portable preset files: save and load across projects.

use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::condition::{Condition, Group, GroupKind, Node, Op};
use trivium_engine::error::AppError;
use trivium_engine::ops;
use trivium_engine::ops::preset::{load_preset, save_preset, LoadPresetPayload, SavePresetPayload};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::store::ProjectsStore;

fn create_project(store: &ProjectsStore, path: &Path) -> Uuid {
    ops::projects::create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id
}

fn write_csv(path: &Path, rows: usize) {
    let mut text = String::from("EventID,host,user,proc\n");
    for index in 0..rows {
        text.push_str(&format!(
            "{},WS{:02},user{},{}\n",
            index + 1,
            index + 1,
            index % 3,
            if index % 2 == 0 { "powershell" } else { "cmd" }
        ));
    }
    fs::write(path, text).expect("failed to write fixture");
}

fn setup() -> (PathBuf, ProjectsStore, Uuid, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-preset-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");

    let path_a = root.join("a.csv");
    write_csv(&path_a, 12);
    let path_b = root.join("b.csv");
    write_csv(&path_b, 8);

    let id_a = create_project(&store, &path_a);
    let id_b = create_project(&store, &path_b);
    (root, store, id_a, id_b)
}

fn nested_condition() -> Node {
    Node::Group(Group {
        kind: GroupKind::And,
        children: vec![
            Node::Condition(Condition {
                column: "host".into(),
                op: Op::Contains,
                values: vec!["WS".into()],
                negated: false,
            }),
            Node::Group(Group {
                kind: GroupKind::Or,
                children: vec![
                    Node::Condition(Condition {
                        column: "user".into(),
                        op: Op::Eq,
                        values: vec!["alice".into()],
                        negated: false,
                    }),
                    Node::Condition(Condition {
                        column: "proc".into(),
                        op: Op::Eq,
                        values: vec!["cmd".into()],
                        negated: true,
                    }),
                ],
            }),
        ],
    })
}

fn save_payload(project_id: Uuid, destination: &Path) -> SavePresetPayload {
    SavePresetPayload {
        project_id,
        name: Some("Hunt".into()),
        condition: Some(nested_condition()),
        hidden_columns: vec!["host".into()],
        sort_key: Some("EventID".into()),
        sort_direction: Some("desc".into()),
        destination: destination.to_string_lossy().to_string(),
    }
}

fn load_payload(project_id: Option<Uuid>, source: &Path) -> LoadPresetPayload {
    LoadPresetPayload {
        project_id,
        source: source.to_string_lossy().to_string(),
    }
}

#[test]
fn preset_round_trips_between_projects() {
    let (root, store, id_a, id_b) = setup();
    let destination = root.join("preset.json");
    save_preset(&store, save_payload(id_a, &destination)).expect("save failed");

    let loaded = load_preset(&store, load_payload(Some(id_b), &destination)).expect("load failed");
    assert_eq!(loaded.name.as_deref(), Some("Hunt"));
    assert_eq!(loaded.condition, Some(nested_condition()));
    assert_eq!(loaded.hidden_columns, vec!["host".to_string()]);
    assert_eq!(loaded.sort_key.as_deref(), Some("EventID"));
    assert_eq!(loaded.sort_direction.as_deref(), Some("desc"));
    assert_eq!(loaded.unknown_columns, Vec::<String>::new());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_reports_unknown_columns_case_insensitively() {
    let (root, store, id_a, id_b) = setup();
    let destination = root.join("preset.json");
    let condition = Node::Group(Group {
        kind: GroupKind::And,
        children: vec![
            Node::Condition(Condition {
                column: "HOST".into(),
                op: Op::Contains,
                values: vec!["WS".into()],
                negated: false,
            }),
            Node::Condition(Condition {
                column: "orwell".into(),
                op: Op::Eq,
                values: vec!["1984".into()],
                negated: false,
            }),
        ],
    });
    save_preset(
        &store,
        SavePresetPayload {
            project_id: id_a,
            name: None,
            condition: Some(condition),
            hidden_columns: Vec::new(),
            sort_key: None,
            sort_direction: None,
            destination: destination.to_string_lossy().to_string(),
        },
    )
    .expect("save failed");

    let loaded = load_preset(&store, load_payload(Some(id_b), &destination)).expect("load failed");
    assert_eq!(loaded.unknown_columns, vec!["orwell".to_string()]);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_rejects_unsupported_version() {
    let (root, store, _id_a, id_b) = setup();
    let destination = root.join("preset.json");
    fs::write(&destination, r#"{"version":2,"hiddenColumns":[]}"#).expect("failed to write");
    let error = load_preset(&store, load_payload(Some(id_b), &destination))
        .expect_err("version 2 must fail");
    match error {
        AppError::Message(message) => assert_eq!(message, "Unsupported preset version."),
        other => panic!("unexpected error: {other:?}"),
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_without_condition_loads_sort_and_columns() {
    let (root, store, id_a, id_b) = setup();
    let destination = root.join("preset.json");
    save_preset(
        &store,
        SavePresetPayload {
            project_id: id_a,
            name: None,
            condition: None,
            hidden_columns: vec!["host".into()],
            sort_key: Some("EventID".into()),
            sort_direction: Some("asc".into()),
            destination: destination.to_string_lossy().to_string(),
        },
    )
    .expect("save failed");

    let loaded = load_preset(&store, load_payload(Some(id_b), &destination)).expect("load failed");
    assert_eq!(loaded.condition, None);
    assert_eq!(loaded.hidden_columns, vec!["host".to_string()]);
    assert_eq!(loaded.sort_key.as_deref(), Some("EventID"));
    assert_eq!(loaded.sort_direction.as_deref(), Some("asc"));
    assert_eq!(loaded.unknown_columns, Vec::<String>::new());

    let _ = fs::remove_dir_all(root);
}

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

fn setup() -> (
    PathBuf,
    ProjectsStore,
    Uuid,
    Uuid,
    std::collections::HashMap<String, String>,
) {
    let root = std::env::temp_dir().join(format!("trivium-preset-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");

    let path_a = root.join("a.csv");
    write_csv(&path_a, 12);
    let path_b = root.join("b.csv");
    write_csv(&path_b, 8);

    let id_a = create_project(&store, &path_a);
    let id_b = create_project(&store, &path_b);
    let ids = common::column_ids(&store, id_a);
    (root, store, id_a, id_b, ids)
}

fn nested_condition(ids: &std::collections::HashMap<String, String>) -> Node {
    Node::Group(Group {
        kind: GroupKind::And,
        children: vec![
            Node::Condition(Condition {
                column: common::id(ids, "host"),
                op: Op::Contains,
                values: vec!["WS".into()],
                negated: false,
            }),
            Node::Group(Group {
                kind: GroupKind::Or,
                children: vec![
                    Node::Condition(Condition {
                        column: common::id(ids, "user"),
                        op: Op::Eq,
                        values: vec!["alice".into()],
                        negated: false,
                    }),
                    Node::Condition(Condition {
                        column: common::id(ids, "proc"),
                        op: Op::Eq,
                        values: vec!["cmd".into()],
                        negated: true,
                    }),
                ],
            }),
        ],
    })
}

fn save_payload(
    project_id: Uuid,
    ids: &std::collections::HashMap<String, String>,
    destination: &Path,
) -> SavePresetPayload {
    SavePresetPayload {
        project_id,
        name: Some("Hunt".into()),
        condition: Some(nested_condition(ids)),
        hidden_columns: vec![common::id(ids, "host")],
        sort_key: Some(common::id(ids, "EventID")),
        sort_direction: Some("desc".into()),
        column_order: None,
        column_widths: None,
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
    let (root, store, id_a, id_b, ids) = setup();
    let destination = root.join("preset.json");
    save_preset(&store, save_payload(id_a, &ids, &destination)).expect("save failed");

    let loaded = load_preset(&store, load_payload(Some(id_b), &destination)).expect("load failed");
    assert_eq!(loaded.name.as_deref(), Some("Hunt"));
    assert_eq!(loaded.condition, Some(nested_condition(&ids)));
    assert_eq!(loaded.hidden_columns, vec![common::id(&ids, "host")]);
    assert_eq!(loaded.sort_key, Some(common::id(&ids, "EventID")));
    assert_eq!(loaded.sort_direction.as_deref(), Some("desc"));
    assert_eq!(loaded.unknown_columns, Vec::<String>::new());
    assert!(loaded.column_order.is_none());
    assert!(loaded.column_widths.is_none());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_reports_unknown_columns_case_insensitively() {
    let (root, store, id_a, id_b, ids) = setup();
    let destination = root.join("preset.json");
    let condition = Node::Group(Group {
        kind: GroupKind::And,
        children: vec![
            Node::Condition(Condition {
                column: common::id(&ids, "host").to_uppercase(),
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
            column_order: None,
            column_widths: None,
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
    let (root, store, _id_a, id_b, _ids) = setup();
    let destination = root.join("preset.json");
    fs::write(&destination, r#"{"version":99,"hiddenColumns":[]}"#).expect("failed to write");
    let error = load_preset(&store, load_payload(Some(id_b), &destination))
        .expect_err("unknown versions must fail");
    match error {
        AppError::Message(message) => assert_eq!(message, "Unsupported preset version."),
        other => panic!("unexpected error: {other:?}"),
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_without_condition_loads_sort_and_columns() {
    let (root, store, id_a, id_b, ids) = setup();
    let destination = root.join("preset.json");
    save_preset(
        &store,
        SavePresetPayload {
            project_id: id_a,
            name: None,
            condition: None,
            hidden_columns: vec![common::id(&ids, "host")],
            sort_key: Some(common::id(&ids, "EventID")),
            sort_direction: Some("asc".into()),
            column_order: None,
            column_widths: None,
            destination: destination.to_string_lossy().to_string(),
        },
    )
    .expect("save failed");

    let loaded = load_preset(&store, load_payload(Some(id_b), &destination)).expect("load failed");
    assert_eq!(loaded.condition, None);
    assert_eq!(loaded.hidden_columns, vec![common::id(&ids, "host")]);
    assert_eq!(loaded.sort_key, Some(common::id(&ids, "EventID")));
    assert_eq!(loaded.sort_direction.as_deref(), Some("asc"));
    assert_eq!(loaded.unknown_columns, Vec::<String>::new());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_maps_rules_hidden_columns_and_sort_after_source_reordering() {
    let (root, store, id_a, _id_b, ids) = setup();
    let reordered = root.join("reordered.csv");
    fs::write(
        &reordered,
        "proc,user,EventID,host\npowershell,alice,1,WS01\n",
    )
    .unwrap();
    let target = create_project(&store, &reordered);
    let target_ids = common::column_ids(&store, target);
    let destination = root.join("preset.json");
    save_preset(&store, save_payload(id_a, &ids, &destination)).unwrap();
    let file: serde_json::Value = serde_json::from_slice(&fs::read(&destination).unwrap()).unwrap();
    assert_eq!(file["version"], 2);
    assert_eq!(file["columns"][1]["source"], "host");
    let loaded = load_preset(&store, load_payload(Some(target), &destination)).unwrap();
    assert_ne!(ids["host"], target_ids["host"]);
    assert_eq!(loaded.condition, Some(nested_condition(&target_ids)));
    assert_eq!(loaded.hidden_columns, vec![target_ids["host"].clone()]);
    assert_eq!(loaded.sort_key, Some(target_ids["EventID"].clone()));
    assert!(loaded.unknown_columns.is_empty());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_rejects_missing_or_ambiguous_source_names_in_a_different_schema() {
    let (root, store, id_a, _id_b, ids) = setup();
    let destination = root.join("preset.json");
    save_preset(&store, save_payload(id_a, &ids, &destination)).unwrap();
    for (name, header) in [
        ("missing", "EventID,other,user,proc"),
        ("ambiguous", "host,EventID,user,proc,host"),
    ] {
        let input = root.join(format!("{name}.csv"));
        let values = if name == "ambiguous" {
            "WS01,1,alice,cmd,WS02"
        } else {
            "1,WS01,alice,cmd"
        };
        fs::write(&input, format!("{header}\n{values}\n")).unwrap();
        let target = create_project(&store, &input);
        let error = load_preset(&store, load_payload(Some(target), &destination)).unwrap_err();
        assert!(
            String::from(error).contains("missing or ambiguous: host"),
            "{name}"
        );
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn matching_duplicate_header_schemas_preserve_their_source_positions() {
    let (root, store, _id_a, _id_b, _ids) = setup();
    let mut projects = Vec::new();
    for name in ["first", "second"] {
        let input = root.join(format!("duplicate-{name}.csv"));
        fs::write(&input, "host,host,score\nleft,right,1\n").unwrap();
        projects.push(create_project(&store, &input));
    }
    let source = store.try_find(&projects[0]).unwrap().unwrap();
    let target = store.try_find(&projects[1]).unwrap().unwrap();
    let destination = root.join("duplicate.json");
    save_preset(
        &store,
        SavePresetPayload {
            project_id: projects[0],
            name: None,
            condition: None,
            hidden_columns: vec![source.columns[1].id.clone()],
            sort_key: Some(source.columns[0].id.clone()),
            sort_direction: Some("asc".into()),
            column_order: None,
            column_widths: None,
            destination: destination.to_string_lossy().to_string(),
        },
    )
    .unwrap();
    let loaded = load_preset(&store, load_payload(Some(projects[1]), &destination)).unwrap();
    assert_eq!(loaded.hidden_columns, vec![target.columns[1].id.clone()]);
    assert_eq!(loaded.sort_key, Some(target.columns[0].id.clone()));
    assert!(loaded.unknown_columns.is_empty());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn old_presets_still_load_by_internal_source_position() {
    let (root, store, _id_a, id_b, ids) = setup();
    let destination = root.join("old-preset.json");
    fs::write(
        &destination,
        serde_json::to_vec(&serde_json::json!({
            "version": 1, "condition": nested_condition(&ids),
            "hiddenColumns": [ids["host"]], "sortKey": ids["EventID"], "sortDirection": "desc",
        }))
        .unwrap(),
    )
    .unwrap();
    let loaded = load_preset(&store, load_payload(Some(id_b), &destination)).unwrap();
    assert_eq!(loaded.condition, Some(nested_condition(&ids)));
    assert_eq!(loaded.hidden_columns, vec![ids["host"].clone()]);
    assert_eq!(loaded.sort_key, Some(ids["EventID"].clone()));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn an_unmapped_id_never_silently_binds_to_an_extra_target_column() {
    let (root, store, id_a, _id_b, ids) = setup();
    let destination = root.join("preset.json");
    let mut payload = save_payload(id_a, &ids, &destination);
    payload.hidden_columns = vec!["c000005".into()];
    save_preset(&store, payload).unwrap();
    let input = root.join("extra.csv");
    fs::write(
        &input,
        "EventID,host,user,proc,extra\n1,WS01,alice,cmd,data\n",
    )
    .unwrap();
    let target = create_project(&store, &input);
    let loaded = load_preset(&store, load_payload(Some(target), &destination)).unwrap();
    assert_eq!(loaded.unknown_columns, vec!["c000005"]);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_output_cannot_replace_source_evidence_or_case_storage() {
    let (root, store, id_a, _id_b, ids) = setup();
    let source = root.join("a.csv");
    let parquet = store.project_dir(&id_a).join("data.parquet");
    let database = store.project_dir(&id_a).join("project.sqlite");
    for destination in [&source, &parquet, &database] {
        let before = fs::read(destination).unwrap();
        let error = save_preset(&store, save_payload(id_a, &ids, destination)).unwrap_err();
        let message = String::from(error);
        assert!(
            message.contains("source evidence") || message.contains("managed"),
            "{message}"
        );
        assert_eq!(fs::read(destination).unwrap(), before);
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_layout_remaps_reordered_sources_and_preserves_omission_and_resets() {
    let (root, store, source, _other, ids) = setup();
    let input = root.join("reordered-layout.csv");
    fs::write(&input, "proc,user,EventID,host\ncmd,alice,1,WS01\n").unwrap();
    let target = create_project(&store, &input);
    let target_ids = common::column_ids(&store, target);
    let destination = root.join("layout.json");
    let mut payload = save_payload(source, &ids, &destination);
    payload.column_order = Some(vec![ids["proc"].clone(), ids["host"].clone()]);
    payload.column_widths = Some([(ids["host"].clone(), 347.0)].into());
    save_preset(&store, payload).unwrap();
    let loaded = load_preset(&store, load_payload(Some(target), &destination)).unwrap();
    assert_eq!(
        loaded.column_order,
        Some(vec![target_ids["proc"].clone(), target_ids["host"].clone()])
    );
    assert_eq!(
        loaded.column_widths,
        Some([(target_ids["host"].clone(), 347.0)].into())
    );
    assert!(loaded.unknown_columns.is_empty());

    let mut payload = save_payload(source, &ids, &destination);
    payload.column_order = Some(Vec::new());
    payload.column_widths = Some(Default::default());
    save_preset(&store, payload).unwrap();
    let loaded = load_preset(&store, load_payload(Some(target), &destination)).unwrap();
    assert_eq!(loaded.column_order, Some(Vec::new()));
    assert_eq!(loaded.column_widths, Some(Default::default()));

    save_preset(&store, save_payload(source, &ids, &destination)).unwrap();
    let file: serde_json::Value = serde_json::from_slice(&fs::read(&destination).unwrap()).unwrap();
    assert!(file.get("columnOrder").is_none());
    assert!(file.get("columnWidths").is_none());
    let loaded = load_preset(&store, load_payload(Some(target), &destination)).unwrap();
    assert!(loaded.column_order.is_none());
    assert!(loaded.column_widths.is_none());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn invalid_preset_layout_is_rejected_before_replacing_a_file_or_applying() {
    let (root, store, source, target, ids) = setup();
    let destination = root.join("invalid-layout.json");
    fs::write(&destination, "keep existing output").unwrap();
    let mut invalid_order = save_payload(source, &ids, &destination);
    invalid_order.column_order = Some(vec![ids["host"].clone(), ids["host"].to_uppercase()]);
    assert!(save_preset(&store, invalid_order).is_err());
    assert_eq!(
        fs::read_to_string(&destination).unwrap(),
        "keep existing output"
    );
    for width in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut invalid = save_payload(source, &ids, &destination);
        invalid.column_widths = Some([(ids["host"].clone(), width)].into());
        assert!(save_preset(&store, invalid).is_err(), "{width}");
        assert_eq!(
            fs::read_to_string(&destination).unwrap(),
            "keep existing output"
        );
    }
    for layout in [
        serde_json::json!({"columnOrder": [ids["host"], ids["host"]]}),
        serde_json::json!({"columnWidths": {ids["host"].clone(): 0}}),
        serde_json::json!({"columnWidths": {"": 200}}),
    ] {
        let mut file = serde_json::json!({"version": 1, "hiddenColumns": []});
        file.as_object_mut()
            .unwrap()
            .extend(layout.as_object().unwrap().clone());
        fs::write(&destination, serde_json::to_vec(&file).unwrap()).unwrap();
        assert!(load_preset(&store, load_payload(Some(target), &destination)).is_err());
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn preset_layout_unknown_ids_and_ambiguous_names_cannot_bind_silently() {
    let (root, store, source, target, ids) = setup();
    let destination = root.join("unknown-layout.json");
    let mut payload = save_payload(source, &ids, &destination);
    payload.column_order = Some(vec!["missing-order".into()]);
    payload.column_widths = Some([("missing-width".into(), 347.0)].into());
    save_preset(&store, payload).unwrap();
    let loaded = load_preset(&store, load_payload(Some(target), &destination)).unwrap();
    assert_eq!(
        loaded.unknown_columns,
        vec!["missing-order", "missing-width"]
    );

    let mut payload = save_payload(source, &ids, &destination);
    payload.condition = None;
    payload.hidden_columns.clear();
    payload.sort_key = None;
    payload.column_order = Some(vec![ids["host"].clone()]);
    payload.column_widths = Some([(ids["host"].clone(), 347.0)].into());
    save_preset(&store, payload).unwrap();
    let input = root.join("duplicate-layout.csv");
    fs::write(&input, "proc,host,user,host\ncmd,left,alice,right\n").unwrap();
    let ambiguous = create_project(&store, &input);
    let error = load_preset(&store, load_payload(Some(ambiguous), &destination)).unwrap_err();
    assert!(String::from(error).contains("missing or ambiguous: host"));
    let _ = fs::remove_dir_all(root);
}

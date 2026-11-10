//! Conditional row colors: persistence, query coloring, single-row coloring,
//! validation, and clearing.

use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use trivium_engine::condition::{Condition, Node, Op};
use trivium_engine::models::ColorRule;
use trivium_engine::ops;
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QueryRowsResponse};
use trivium_engine::ops::rules::{color_rules, save_color_rules, SaveColorRulesPayload};
use trivium_engine::store::ProjectsStore;

// 12 rows: first 6 are alice, last 6 are bob. EventID varies per row.
fn write_fixture(root: &std::path::Path) -> PathBuf {
    let path = root.join("colors.csv");
    let body = "ts,EventID,host,user,proc\n\
2024-01-01T00:00:01,4688,WS01,alice,powershell\n\
2024-01-01T00:00:02,4624,WS02,alice,explorer\n\
2024-01-01T00:00:03,4625,WS03,alice,cmd\n\
2024-01-01T00:00:04,4624,WS04,alice,explorer\n\
2024-01-01T00:00:05,4625,WS05,alice,cmd\n\
2024-01-01T00:00:06,4688,WS06,alice,powershell\n\
2024-01-01T00:00:07,4624,WS07,bob,explorer\n\
2024-01-01T00:00:08,4625,WS08,bob,cmd\n\
2024-01-01T00:00:09,4688,WS09,bob,powershell\n\
2024-01-01T00:00:10,4624,WS10,bob,explorer\n\
2024-01-01T00:00:11,4625,WS11,bob,cmd\n\
2024-01-01T00:00:12,4688,WS12,bob,powershell\n";
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn setup() -> (PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-colors-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_fixture(&root);
    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
        },
    )
    .expect("failed to create project");
    (root, store, summary.meta.id)
}

fn condition(column: &str, op: Op, values: &[&str]) -> Node {
    Node::Condition(Condition {
        column: column.to_string(),
        op,
        values: values.iter().map(|value| value.to_string()).collect(),
        negated: false,
    })
}

fn rule(name: &str, condition: Node, color: &str) -> ColorRule {
    ColorRule {
        name: name.to_string(),
        condition,
        color: color.to_string(),
    }
}

fn save(store: &ProjectsStore, id: Uuid, rules: Vec<ColorRule>) -> Vec<ColorRule> {
    save_color_rules(
        store,
        SaveColorRulesPayload {
            project_id: id,
            rules,
        },
    )
    .expect("save color rules failed")
    .rules
}

fn load(store: &ProjectsStore, id: Uuid) -> Vec<ColorRule> {
    color_rules(store, ProjectRequest { project_id: id })
        .expect("load color rules failed")
        .rules
}

fn query(store: &ProjectsStore, id: Uuid) -> QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: None,
            bookmarked_only: None,
            offset: None,
            limit: None,
            sort_key: None,
            sort_direction: None,
            condition: None,
        },
    )
    .expect("query failed")
}

fn sorted_query(store: &ProjectsStore, id: Uuid) -> QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: None,
            bookmarked_only: None,
            offset: None,
            limit: None,
            sort_key: Some("EventID".to_string()),
            sort_direction: Some("asc".to_string()),
            condition: None,
        },
    )
    .expect("sorted query failed")
}

fn event_ids(rows: &[trivium_engine::models::ProjectRow]) -> Vec<String> {
    rows.iter()
        .map(|row| row.data["EventID"].to_string())
        .collect()
}

#[test]
fn color_rules_save_and_load_roundtrip() {
    let (root, store, id) = setup();
    let rules = vec![
        rule(
            "alice-red",
            condition("user", Op::Contains, &["alice"]),
            "#FF0000",
        ),
        rule(
            "bob-short",
            condition("user", Op::Contains, &["bob"]),
            "#FF5",
        ),
    ];

    let saved = save(&store, id, rules.clone());
    assert_eq!(saved.len(), 2);
    assert_eq!(saved[0].color, "#ff0000");
    assert_eq!(saved[1].color, "#ff5");

    let loaded = load(&store, id);
    assert_eq!(loaded, saved);
    assert_eq!(loaded[0].name, "alice-red");
    assert_eq!(loaded[0].condition, rules[0].condition);
    assert_eq!(loaded[1].name, "bob-short");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn query_colors_matching_rows_and_keeps_order_and_counts() {
    let (root, store, id) = setup();
    let before = sorted_query(&store, id);
    assert!(before.rows.iter().all(|row| row.color.is_none()));

    save(
        &store,
        id,
        vec![rule(
            "alice-red",
            condition("user", Op::Contains, &["alice"]),
            "#ff0000",
        )],
    );

    let after = sorted_query(&store, id);
    assert_eq!(after.total_filtered_rows, before.total_filtered_rows);
    assert_eq!(event_ids(&after.rows), event_ids(&before.rows));
    assert_eq!(
        after
            .rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        before
            .rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>()
    );

    for row in &after.rows {
        let user = row.data["user"].as_str().unwrap_or_default();
        if user == "alice" {
            assert_eq!(row.color.as_deref(), Some("#ff0000"), "row {:?}", row.row_index);
        } else {
            assert!(row.color.is_none(), "row {:?}", row.row_index);
        }
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn update_flag_returns_matching_row_color() {
    let (root, store, id) = setup();
    save(
        &store,
        id,
        vec![rule(
            "alice-red",
            condition("user", Op::Contains, &["alice"]),
            "#ff0000",
        )],
    );

    let updated = ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: "suspicious".to_string(),
            memo: None,
            tags: None,
            bookmarked: None,
        },
    )
    .expect("update flag failed");
    assert_eq!(updated.data["user"], serde_json::json!("alice"));
    assert_eq!(updated.color.as_deref(), Some("#ff0000"));

    let bob = ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 6,
            flag: "safe".to_string(),
            memo: None,
            tags: None,
            bookmarked: None,
        },
    )
    .expect("update flag failed");
    assert_eq!(bob.color, None);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn invalid_colors_and_too_many_rules_fail_and_missing_columns_are_ignored() {
    let (root, store, id) = setup();

    let invalid = save_color_rules(
        &store,
        SaveColorRulesPayload {
            project_id: id,
            rules: vec![rule(
                "bad",
                condition("user", Op::Contains, &["alice"]),
                "#red",
            )],
        },
    )
    .expect_err("invalid color must fail");
    assert!(
        invalid.to_string().contains("Invalid color"),
        "unexpected error: {invalid}"
    );

    let too_many: Vec<ColorRule> = (0..51)
        .map(|index| {
            rule(
                &format!("rule-{index}"),
                condition("user", Op::Contains, &["alice"]),
                "#ff0000",
            )
        })
        .collect();
    let overflow = save_color_rules(
        &store,
        SaveColorRulesPayload {
            project_id: id,
            rules: too_many,
        },
    )
    .expect_err("51 rules must fail");
    assert!(
        overflow.to_string().contains("Too many color rules (max 50)."),
        "unexpected error: {overflow}"
    );

    save(
        &store,
        id,
        vec![rule(
            "missing-column",
            condition("not_exists", Op::Contains, &["anything"]),
            "#00ff00",
        )],
    );
    let response = query(&store, id);
    assert_eq!(response.total_filtered_rows, 12);
    assert!(response.rows.iter().all(|row| row.color.is_none()));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn clearing_rules_removes_colors_from_query_results() {
    let (root, store, id) = setup();
    save(
        &store,
        id,
        vec![rule(
            "alice-red",
            condition("user", Op::Contains, &["alice"]),
            "#ff0000",
        )],
    );
    assert!(query(&store, id).rows.iter().any(|row| row.color.is_some()));

    let cleared = save(&store, id, Vec::new());
    assert!(cleared.is_empty());
    assert!(load(&store, id).is_empty());
    assert!(query(&store, id).rows.iter().all(|row| row.color.is_none()));

    let _ = fs::remove_dir_all(root);
}

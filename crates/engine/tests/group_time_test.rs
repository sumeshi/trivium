//! Calendar grouping shares query scopes and produces exact datetime drill-downs.
use std::{collections::HashMap, fs, path::PathBuf};

use serde_json::{json, Value};
use trivium_engine::{
    analysis::group_time::GroupGranularity,
    condition::{Condition, Group, GroupKind, Node, Op},
    ops::{
        self,
        flags::UpdateFlagPayload,
        group::{GroupTreePayload, GroupTreeResponse},
        projects::CreateProjectPayload,
        rows::{QueryRowsPayload, QuerySpec},
    },
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

fn setup(rows: &[Value]) -> (PathBuf, ProjectsStore, Uuid, HashMap<String, String>) {
    let root = std::env::temp_dir().join(format!("trivium-time-groups-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let path = root.join("events.jsonl");
    fs::write(
        &path,
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();
    let store = ProjectsStore::new(root.clone()).unwrap();
    let project = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().into_owned(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    let ids = common::column_ids(&store, project.meta.id);
    (root, store, project.meta.id, ids)
}

fn payload(
    id: Uuid,
    columns: Vec<String>,
    granularities: Vec<GroupGranularity>,
) -> GroupTreePayload {
    GroupTreePayload {
        project_id: id,
        search: None,
        columns: None,
        search_columns: None,
        flag_filter: None,
        bookmarked_only: None,
        condition: None,
        group_columns: columns,
        group_granularities: granularities,
    }
}

fn predicate(column: &str, op: Op, value: &str) -> Node {
    Node::Condition(Condition {
        column: column.into(),
        op,
        values: vec![value.into()],
        negated: false,
    })
}

fn query_total(store: &ProjectsStore, id: Uuid, condition: Option<Node>) -> usize {
    ops::rows::query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            offset: None,
            limit: None,
            spec: QuerySpec {
                condition,
                search: None,
                search_columns: None,
                flag_filter: None,
                bookmarked_only: None,
                sort_key: None,
                sort_direction: None,
            },
        },
    )
    .unwrap()
    .total_filtered_rows
}

fn fixture() -> Vec<Value> {
    vec![
        json!({"parent":"alpha", "ts":"2026-11-14T23:59:59.999999999Z"}),
        json!({"parent":"alpha", "ts":"2026-11-15T08:59:59.999999999+09:00"}),
        json!({"parent":"alpha", "ts":"2026-11-15T00:00:00Z"}),
        json!({"parent":"beta", "ts":"2026-11-15T00:00:00.000000001Z"}),
        json!({"parent":"alpha", "ts":null}),
        json!({"parent":"alpha"}),
        json!({"parent":"alpha", "ts":""}),
        json!({"parent":"alpha", "ts":" \t "}),
        json!({"parent":"beta", "ts":"2026/11/14 02:03:04.123456789"}),
    ]
}

#[test]
fn nested_day_groups_share_offsets_and_drill_down_across_nanosecond_edges() {
    let rows = fixture();
    let (root, store, id, ids) = setup(&rows);
    let parent = common::id(&ids, "/parent");
    let timestamp = common::id(&ids, "/ts");
    let response = ops::group::group_tree(
        &store,
        payload(
            id,
            vec![parent.clone(), timestamp.clone()],
            vec![GroupGranularity::Exact, GroupGranularity::Day],
        ),
    )
    .unwrap();
    assert_eq!(response.total, 9);
    assert_eq!(
        response.nodes.iter().map(|node| node.count).sum::<usize>(),
        9
    );
    for node in &response.nodes {
        assert_eq!(
            node.children.iter().map(|child| child.count).sum::<usize>(),
            node.count
        );
        for child in node.children.iter().filter(|child| child.range.is_some()) {
            let range = child.range.as_ref().unwrap();
            let conditions = Node::Group(Group {
                kind: GroupKind::And,
                children: vec![
                    predicate(&parent, Op::Eq, &node.label),
                    // Blank/null source cells must short-circuit before comparisons.
                    predicate(&timestamp, Op::Regex, "\\S"),
                    predicate(&timestamp, Op::Ge, &range.start),
                    predicate(&timestamp, Op::Lt, &range.end),
                ],
            });
            assert_eq!(
                query_total(&store, id, Some(conditions)),
                child.count,
                "{} / {}",
                node.label,
                child.label
            );
        }
    }
    let alpha = response
        .nodes
        .iter()
        .find(|node| node.label == "alpha")
        .unwrap();
    assert_eq!(alpha.count, 7);
    assert_eq!(
        alpha
            .children
            .iter()
            .find(|child| child.label == "2026-11-14")
            .unwrap()
            .count,
        2
    );
    assert_eq!(
        alpha
            .children
            .iter()
            .find(|child| child.label == "2026-11-15")
            .unwrap()
            .count,
        1
    );
    assert_eq!(
        alpha
            .children
            .iter()
            .find(|child| child.value == Value::Null)
            .unwrap()
            .count,
        2
    );
    assert_eq!(
        alpha
            .children
            .iter()
            .filter(|child| child.label.is_empty())
            .count(),
        2,
        "null and empty stay distinct"
    );
    let original = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            offset: Some(0),
            limit: Some(2),
            spec: QuerySpec::default(),
        },
    )
    .unwrap();
    assert_eq!(
        common::cell_text(&original.rows[0], &timestamp),
        rows[0]["ts"].as_str()
    );
    assert_eq!(
        common::cell_text(&original.rows[1], &timestamp),
        rows[1]["ts"].as_str()
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn hour_groups_respect_search_flag_bookmark_and_condition_together() {
    let (root, store, id, ids) = setup(&fixture());
    let parent = common::id(&ids, "/parent");
    let timestamp = common::id(&ids, "/ts");
    for row_index in [0, 1] {
        ops::flags::update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index,
                flag: Some("critical".into()),
                memo: None,
                tags: None,
                bookmarked: Some(row_index == 0),
            },
        )
        .unwrap();
    }
    let mut request = payload(id, vec![timestamp.clone()], vec![GroupGranularity::Hour]);
    request.search = Some("alpha".into());
    request.search_columns = Some(vec![parent]);
    request.flag_filter = Some("critical".into());
    request.bookmarked_only = Some(true);
    request.condition = Some(predicate(&timestamp, Op::Regex, "\\S"));
    let grouped = ops::group::group_tree(&store, request).unwrap();
    assert_eq!(grouped.total, 1);
    assert_eq!(grouped.nodes.len(), 1);
    assert_eq!(grouped.nodes[0].label, "2026-11-14T23:00Z");
    assert_eq!(grouped.nodes[0].count, 1);
    assert_eq!(
        grouped.nodes[0].range.as_ref().unwrap().end,
        "2026-11-15T00:00:00Z"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_times_are_reported_and_existing_filters_can_exclude_them() {
    let (root, store, id, ids) = setup(&[
        json!({"ts":"2026-11-14T12:00:00Z"}),
        json!({"ts":"not-a-date"}),
    ]);
    let timestamp = common::id(&ids, "/ts");
    let exact: GroupTreeResponse =
        ops::group::group_tree(&store, payload(id, vec![timestamp.clone()], vec![])).unwrap();
    assert_eq!(exact.total, 2);
    assert!(exact.nodes.iter().all(|node| node.range.is_none()));
    let error = ops::group::group_tree(
        &store,
        payload(id, vec![timestamp.clone()], vec![GroupGranularity::Day]),
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("row 2") && error.contains("recognized datetime"),
        "{error}"
    );
    assert!(
        !error.contains("not-a-date"),
        "do not dump source values into diagnostics"
    );
    let mut filtered = payload(id, vec![timestamp.clone()], vec![GroupGranularity::Day]);
    filtered.condition = Some(predicate(&timestamp, Op::Ne, "not-a-date"));
    let response = ops::group::group_tree(&store, filtered).unwrap();
    assert_eq!(response.total, 1);
    assert_eq!(response.nodes[0].label, "2026-11-14");
    let mismatch = ops::group::group_tree(
        &store,
        payload(
            id,
            vec![timestamp],
            vec![GroupGranularity::Day, GroupGranularity::Hour],
        ),
    )
    .unwrap_err()
    .to_string();
    assert!(mismatch.contains("matching granularity"), "{mismatch}");
    fs::remove_dir_all(root).unwrap();
}

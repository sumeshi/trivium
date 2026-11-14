//! Hierarchical grouping over matched rows: counts, query parity, and ordering.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use trivium_engine::ops;
use trivium_engine::ops::group::{group_tree, GroupTreeNode, GroupTreePayload, GroupTreeResponse};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::store::ProjectsStore;

mod common;

// 12 rows: the first 6 are alice, the last 6 are bob.
// EventID is spread evenly: 4624 / 4625 / 4688 four rows each.
fn write_fixture(root: &std::path::Path) -> PathBuf {
    let path = root.join("group.csv");
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

fn setup() -> (PathBuf, ProjectsStore, Uuid, std::collections::HashMap<String, String>) {
    let root = std::env::temp_dir().join(format!("trivium-group-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_fixture(&root);
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
    let ids = common::column_ids(&store, summary.meta.id);
    (root, store, summary.meta.id, ids)
}

fn payload(id: Uuid, group_columns: Vec<&str>, search: Option<&str>) -> GroupTreePayload {
    GroupTreePayload {
        project_id: id,
        search: search.map(|value| value.to_string()),
        columns: None,
        search_columns: None,
        bookmarked_only: None,
        flag_filter: None,
        condition: None,
        group_columns: group_columns
            .into_iter()
            .map(|name| name.to_string())
            .collect(),
        group_granularities: Vec::new(),
    }
}

fn run(
    store: &ProjectsStore,
    id: Uuid,
    group_columns: Vec<&str>,
    search: Option<&str>,
) -> GroupTreeResponse {
    group_tree(store, payload(id, group_columns, search)).expect("grouping failed")
}

fn counts(nodes: &[GroupTreeNode]) -> HashMap<String, usize> {
    nodes
        .iter()
        .map(|node| (node.label.clone(), node.count))
        .collect()
}

fn find<'a>(nodes: &'a [GroupTreeNode], label: &str) -> &'a GroupTreeNode {
    nodes
        .iter()
        .find(|node| node.label == label)
        .unwrap_or_else(|| panic!("node not found: {label}"))
}

fn query_total(store: &ProjectsStore, id: Uuid, search: Option<&str>) -> usize {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: search.map(|value| value.to_string()),
                search_columns: None,
                flag_filter: None,
                sort_key: None,
                sort_direction: None,
                condition: None,
                bookmarked_only: None,
            },
            offset: None,
            limit: None,
        },
    )
    .expect("query failed")
    .total_filtered_rows
}

#[test]
fn grouping_without_filters_matches_query_parity() {
    let (root, store, id, ids) = setup();
    let event_id = common::id(&ids, "EventID");
    let response = run(&store, id, vec![event_id.as_str()], None);

    assert_eq!(response.columns, vec![event_id]);
    assert_eq!(response.total, 12);
    assert_eq!(
        counts(&response.nodes),
        HashMap::from([
            ("4624".to_string(), 4),
            ("4625".to_string(), 4),
            ("4688".to_string(), 4),
        ])
    );
    let sum: usize = response.nodes.iter().map(|node| node.count).sum();
    assert_eq!(sum, response.total, "level-1 counts must add up to total");
    assert_eq!(response.total, query_total(&store, id, None));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn grouping_respects_search_filters() {
    let (root, store, id, ids) = setup();
    let event_id = common::id(&ids, "EventID");
    let or_search = run(
        &store,
        id,
        vec![event_id.as_str()],
        Some(&common::search(&ids, "user", "alice|bob")),
    );
    assert_eq!(or_search.total, 12, "OR search covers every fixture row");
    assert_eq!(
        counts(&or_search.nodes),
        HashMap::from([
            ("4624".to_string(), 4),
            ("4625".to_string(), 4),
            ("4688".to_string(), 4),
        ])
    );
    assert_eq!(
        or_search.total,
        query_total(&store, id, Some(&common::search(&ids, "user", "alice|bob")))
    );

    let narrowed = run(&store, id, vec![event_id.as_str()], Some("4625"));
    assert_eq!(narrowed.total, 4);
    assert_eq!(narrowed.nodes.len(), 1);
    assert_eq!(narrowed.nodes[0].label, "4625");
    assert_eq!(narrowed.nodes[0].count, 4);
    assert_eq!(narrowed.total, query_total(&store, id, Some("4625")));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn grouping_stacks_second_level_counts() {
    let (root, store, id, ids) = setup();
    let user_id = common::id(&ids, "user");
    let proc_id = common::id(&ids, "proc");
    let response = run(&store, id, vec![user_id.as_str(), proc_id.as_str()], None);

    assert_eq!(response.columns, vec![user_id, proc_id]);
    assert_eq!(
        counts(&response.nodes),
        HashMap::from([("alice".to_string(), 6), ("bob".to_string(), 6),])
    );

    let alice = find(&response.nodes, "alice");
    assert_eq!(
        counts(&alice.children),
        HashMap::from([
            ("cmd".to_string(), 2),
            ("explorer".to_string(), 2),
            ("powershell".to_string(), 2),
        ])
    );
    let level_two_sum: usize = alice.children.iter().map(|node| node.count).sum();
    assert_eq!(level_two_sum, alice.count);

    let bob = find(&response.nodes, "bob");
    assert_eq!(counts(&bob.children), counts(&alice.children));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn grouping_orders_children_by_label() {
    let (root, store, id, ids) = setup();
    let event_id = common::id(&ids, "EventID");
    let response = run(&store, id, vec![event_id.as_str()], None);
    let labels: Vec<&str> = response
        .nodes
        .iter()
        .map(|node| node.label.as_str())
        .collect();
    assert_eq!(labels, ["4624", "4625", "4688"]);

    let nested = run(
        &store,
        id,
        vec![common::id(&ids, "user").as_str(), common::id(&ids, "proc").as_str()],
        None,
    );
    let alice = find(&nested.nodes, "alice");
    let proc_labels: Vec<&str> = alice
        .children
        .iter()
        .map(|node| node.label.as_str())
        .collect();
    assert_eq!(proc_labels, ["cmd", "explorer", "powershell"]);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn grouping_rejects_empty_columns() {
    let (root, store, id, _ids) = setup();
    let error = group_tree(&store, payload(id, vec!["  ", ""], None))
        .expect_err("empty group columns must fail");
    assert!(
        error.to_string().contains("must not be empty"),
        "unexpected error: {error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn grouping_rejects_missing_columns() {
    let (root, store, id, _ids) = setup();
    let error = group_tree(&store, payload(id, vec!["not_exists"], None))
        .expect_err("missing group column must fail");
    assert!(
        error.to_string().contains("not_exists"),
        "unexpected error: {error}"
    );

    let _ = fs::remove_dir_all(root);
}

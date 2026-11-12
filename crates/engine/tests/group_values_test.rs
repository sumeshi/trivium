//! Exact grouping identities, drill-down and bounded result construction.
use std::{fs, path::PathBuf};

use serde_json::{json, Value};
use trivium_engine::{
    condition::{Condition, Group, GroupKind, Node, Op},
    ops::{
        self,
        group::{GroupTreePayload, GroupTreeResponse},
        projects::CreateProjectPayload,
    },
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

fn setup(body: &str, extension: &str) -> (PathBuf, ProjectsStore, Uuid, Vec<String>) {
    let root = std::env::temp_dir().join(format!("trivium-exact-groups-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let path = root.join(format!("groups.{extension}"));
    fs::write(&path, body).unwrap();
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
    let columns = ["parent", "value"]
        .iter()
        .filter_map(|name| {
            ids.get(*name)
                .or_else(|| ids.get(&format!("/{name}")))
                .cloned()
        })
        .collect();
    (root, store, project.meta.id, columns)
}

fn payload(id: Uuid, columns: &[String]) -> GroupTreePayload {
    GroupTreePayload {
        project_id: id,
        search: None,
        columns: None,
        search_columns: None,
        flag_filter: None,
        bookmarked_only: None,
        condition: None,
        group_columns: columns.to_vec(),
    }
}

fn grouped(store: &ProjectsStore, payload: GroupTreePayload) -> GroupTreeResponse {
    ops::group::group_tree(store, payload).unwrap()
}

fn condition(column: &str, op: Op, value: &str) -> Node {
    Node::Condition(Condition {
        column: column.into(),
        op,
        values: vec![value.into()],
        negated: false,
    })
}

#[test]
fn nested_null_empty_whitespace_and_large_integer_text_drill_down_exactly() {
    let body = [
        json!({"parent":"p", "value":null}),
        json!({"parent":"p"}),
        json!({"parent":"p", "value":""}),
        json!({"parent":"p", "value":" "}),
        json!({"parent":"p", "value":"a"}),
        json!({"parent":"p", "value":" a "}),
        json!({"parent":"p", "value":"9007199254740993"}),
    ]
    .map(|row| row.to_string())
    .join("\n");
    let (root, store, id, columns) = setup(&body, "jsonl");
    assert_eq!(columns.len(), 2);
    let response = grouped(&store, payload(id, &columns));
    assert_eq!(response.total, 7);
    assert_eq!(response.nodes.len(), 1);
    let parent = &response.nodes[0];
    assert_eq!(parent.value, json!("p"));
    assert_eq!(parent.count, 7);
    assert_eq!(parent.children.len(), 6);
    assert_eq!(
        parent
            .children
            .iter()
            .map(|child| child.count)
            .sum::<usize>(),
        parent.count
    );
    for child in &parent.children {
        let mut predicates = vec![condition(&columns[0], Op::Eq, "p")];
        if child.value == Value::Null && child.label.is_empty() {
            assert_eq!(child.count, 2, "missing and explicit null share a group");
            predicates.push(condition(&columns[1], Op::Ne, ""));
            predicates.push(condition(&columns[1], Op::Regex, "\\A\\z"));
        } else {
            assert_eq!(child.value, Value::String(child.label.clone()));
            predicates.push(condition(&columns[1], Op::Eq, &child.label));
        }
        let mut query = payload(id, &columns);
        query.condition = Some(Node::Group(Group {
            kind: GroupKind::And,
            children: predicates,
        }));
        let narrowed = grouped(&store, query);
        assert_eq!(narrowed.total, child.count, "exact path {:?}", child.value);
        assert_eq!(narrowed.nodes[0].children.len(), 1);
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn rejects_excessive_cardinality_depth_and_duplicate_grouping_columns() {
    let mut body = String::from("value\n");
    for row in 0..10_001 {
        body.push_str(&format!("item-{row}\n"));
    }
    let (root, store, id, columns) = setup(&body, "csv");
    assert_eq!(columns.len(), 1);
    let error = ops::group::group_tree(&store, payload(id, &columns))
        .unwrap_err()
        .to_string();
    assert!(error.contains("10,000"), "{error}");
    let repeated = vec![columns[0].clone(); 2];
    let error = ops::group::group_tree(&store, payload(id, &repeated))
        .unwrap_err()
        .to_string();
    assert!(error.contains("only once"), "{error}");
    let repeated = vec![columns[0].clone(); 9];
    let error = ops::group::group_tree(&store, payload(id, &repeated))
        .unwrap_err()
        .to_string();
    assert!(error.contains("at most 8"), "{error}");
    fs::remove_dir_all(root).unwrap();
}

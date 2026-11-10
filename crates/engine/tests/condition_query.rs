use std::fs;

use serde_json::json;
use uuid::Uuid;

use trivium_engine::condition::{Condition, Node, Op};
use trivium_engine::ops;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::QueryRowsPayload;
use trivium_engine::store::ProjectsStore;

fn payload(id: Uuid, search: Option<String>, condition: Option<Node>) -> QueryRowsPayload {
    QueryRowsPayload {
        project_id: id,
        search,
        search_columns: None,
        flag_filter: None,
        offset: None,
        limit: None,
        sort_key: None,
        sort_direction: None,
        condition,
        bookmarked_only: None,
    }
}

#[test]
fn query_accepts_condition_and_keeps_search_syntax() {
    let root = std::env::temp_dir().join(format!("trivium-cond-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let csv = root.join("rows.csv");
    fs::write(
        &csv,
        "name,n\na,1\nb,1700000000123456789\nc,3\n",
    )
    .unwrap();
    let store = ProjectsStore::new(root.clone()).unwrap();
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
        },
    )
    .unwrap()
    .meta
    .id;

    let gt = ops::rows::query_project_rows(
        &store,
        payload(
            id,
            None,
            Some(Node::Condition(Condition {
                column: "n".into(),
                op: Op::Gt,
                values: vec!["2".into()],
                negated: false,
            })),
        ),
    )
    .unwrap();
    assert_eq!(gt.total_filtered_rows, 2);

    let exact = ops::rows::query_project_rows(
        &store,
        payload(
            id,
            None,
            Some(Node::Condition(Condition {
                column: "n".into(),
                op: Op::Eq,
                values: vec!["1700000000123456789".into()],
                negated: false,
            })),
        ),
    )
    .unwrap();
    assert_eq!(exact.total_filtered_rows, 1);
    assert_eq!(exact.rows[0].data["name"], json!("b"));

    let search_only = ops::rows::query_project_rows(
        &store,
        payload(id, Some("name:a|c".into()), None),
    )
    .unwrap();
    assert_eq!(search_only.total_filtered_rows, 2);

    let both = ops::rows::query_project_rows(
        &store,
        payload(
            id,
            Some("name:a|b|c".into()),
            Some(Node::Condition(Condition {
                column: "name".into(),
                op: Op::Eq,
                values: vec!["a".into()],
                negated: false,
            })),
        ),
    )
    .unwrap();
    assert_eq!(both.total_filtered_rows, 1);
    assert_eq!(both.rows[0].data["name"], json!("a"));

    let _ = fs::remove_dir_all(root);
}

//! T09: CSV and JSON values are stored exactly; numeric comparisons never
//! round through floating point.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::json;
use uuid::Uuid;

use trivium_engine::condition::{Condition, Group, GroupKind, Node, Op};
use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::projects::{create_project, load_project, CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::project_io::read_original_records;
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-values-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn create(store: &ProjectsStore, path: &Path) -> Uuid {
    create_project(
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

fn column_ids(store: &ProjectsStore, id: Uuid) -> std::collections::HashMap<String, String> {
    load_project(store, ProjectRequest { project_id: id })
        .expect("load project")
        .columns_meta
        .into_iter()
        .map(|column| (column.source, column.id))
        .collect()
}

fn query(store: &ProjectsStore, id: Uuid, spec: QuerySpec) -> trivium_engine::ops::rows::QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec,
            offset: None,
            limit: None,
        },
    )
    .expect("query failed")
}

fn condition(column: &str, op: Op, value: &str) -> Node {
    Node::Condition(Condition {
        column: column.to_string(),
        op,
        values: vec![value.to_string()],
        negated: false,
    })
}

fn cell(response: &trivium_engine::ops::rows::QueryRowsResponse, row: usize, column: &str) -> serde_json::Value {
    response.rows[row]
        .data
        .get(column)
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

#[test]
fn csv_cell_text_is_stored_and_exported_verbatim() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("store");
    let csv = root.join("values.csv");
    fs::write(
        &csv,
        "id,n,ts,note\n\
         00123,9007199254740993,2024-01-01T00:00:01.123456789,\"a,b\"\n\
         00000,18446744073709551615,2024-01-02,  padded  \n\
         ,-9223372036854775808,2024-01-03,\"line1\nline2\"\n",
    )
    .expect("write csv");
    let id = create(&store, &csv);
    let ids = column_ids(&store, id);

    let response = query(&store, id, QuerySpec::default());
    assert_eq!(response.total_rows, 3);

    let id_column = ids.get("id").expect("id column");
    let n_column = ids.get("n").expect("n column");
    let ts_column = ids.get("ts").expect("ts column");
    let note_column = ids.get("note").expect("note column");

    assert_eq!(cell(&response, 0, id_column), json!("00123"));
    assert_eq!(cell(&response, 1, id_column), json!("00000"));
    assert_eq!(cell(&response, 2, id_column), json!(""));
    assert_eq!(cell(&response, 0, n_column), json!("9007199254740993"));
    assert_eq!(cell(&response, 1, n_column), json!("18446744073709551615"));
    assert_eq!(cell(&response, 2, n_column), json!("-9223372036854775808"));
    assert_eq!(
        cell(&response, 0, ts_column),
        json!("2024-01-01T00:00:01.123456789")
    );
    assert_eq!(cell(&response, 1, note_column), json!("  padded  "));
    assert_eq!(cell(&response, 2, note_column), json!("line1\nline2"));

    let destination = root.join("out.csv");
    export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: None,
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: None,
        },
    )
    .expect("export");
    let exported = fs::read_to_string(&destination).expect("read export");
    assert!(exported.contains("00123"), "leading zeros survive export");
    assert!(exported.contains("9007199254740993"));
    assert!(exported.contains("18446744073709551615"));
    assert!(exported.contains("2024-01-01T00:00:01.123456789"));
    assert!(exported.contains("  padded  "));
}

#[test]
fn json_scalars_keep_exact_text_and_null_stays_null() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("store");
    let jsonl = root.join("values.jsonl");
    fs::write(
        &jsonl,
        concat!(
            "{\"n\": 9007199254740993, \"u\": 18446744073709551615, \"small\": -9223372036854775808}\n",
            "{\"n\": 170141183460469231731687303715884105728, \"u\": 1.5, \"small\": null}\n",
            "{\"n\": \"\", \"u\": true, \"small\": [1, 2], \"only_later\": {}}\n",
        ),
    )
    .expect("write jsonl");
    let id = create(&store, &jsonl);
    let ids = column_ids(&store, id);
    let response = query(&store, id, QuerySpec::default());

    let n = ids.get("/n").expect("n");
    let u = ids.get("/u").expect("u");
    let small = ids.get("/small").expect("small");

    assert_eq!(cell(&response, 0, n), json!("9007199254740993"));
    assert_eq!(
        cell(&response, 1, n),
        json!("170141183460469231731687303715884105728")
    );
    assert_eq!(cell(&response, 2, n), json!(""));
    assert_eq!(cell(&response, 0, u), json!("18446744073709551615"));
    assert_eq!(cell(&response, 1, u), json!("1.5"));
    assert_eq!(cell(&response, 2, u), json!("true"));
    assert_eq!(cell(&response, 1, small), serde_json::Value::Null);
    assert_eq!(cell(&response, 2, small), json!("[1,2]"));
    // An empty object still counts as one record but adds no columns.
    assert_eq!(response.total_rows, 3);

    let originals = read_original_records(&store.project_dir(&id)).expect("originals");
    assert_eq!(originals[0]["n"], json!(9007199254740993_u64));
    assert_eq!(originals[1]["n"].to_string(), "170141183460469231731687303715884105728");
    assert_eq!(originals[1]["small"], serde_json::Value::Null);
    assert!(originals[2].get("small").is_some());
}

#[test]
fn adjacent_long_integers_compare_without_rounding() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("store");
    let csv = root.join("longs.csv");
    fs::write(
        &csv,
        "n\n9007199254740992\n9007199254740993\n18446744073709551614\n18446744073709551615\n",
    )
    .expect("write csv");
    let id = create(&store, &csv);
    let ids = column_ids(&store, id);
    let n = ids.get("n").expect("n");

    let hits = query(
        &store,
        id,
        QuerySpec {
            condition: Some(condition(n, Op::Gt, "9007199254740992")),
            ..Default::default()
        },
    );
    assert_eq!(hits.total_filtered_rows, 3);
    assert!(hits
        .rows
        .iter()
        .all(|row| row.data.get(n).and_then(|value| value.as_str()) != Some("9007199254740992")));

    let sorted = query(
        &store,
        id,
        QuerySpec {
            sort_key: Some(n.clone()),
            sort_direction: Some("asc".to_string()),
            ..Default::default()
        },
    );
    let ordered: Vec<&str> = sorted
        .rows
        .iter()
        .map(|row| row.data.get(n).and_then(|value| value.as_str()).unwrap())
        .collect();
    assert_eq!(
        ordered,
        vec![
            "9007199254740992",
            "9007199254740993",
            "18446744073709551614",
            "18446744073709551615"
        ],
        "adjacent long integers keep their own order"
    );
}

#[test]
fn decimal_or_out_of_range_numeric_comparison_is_an_error() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("store");
    let csv = root.join("mixed.csv");
    fs::write(&csv, "n\n1.5\n170141183460469231731687303715884105728\n").expect("write csv");
    let id = create(&store, &csv);
    let ids = column_ids(&store, id);
    let n = ids.get("n").expect("n");

    for op in [Op::Gt, Op::Lt, Op::Ge, Op::Le] {
        let result = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: id,
                spec: QuerySpec {
                    condition: Some(condition(n, op, "1")),
                    ..Default::default()
                },
                offset: None,
                limit: None,
            },
        );
        assert!(result.is_err(), "{:?} must reject decimals/out-of-range", op);
    }

    // Text equality still works without any numeric conversion.
    let by_text = query(
        &store,
        id,
        QuerySpec {
            condition: Some(Node::Group(Group {
                kind: GroupKind::Or,
                children: vec![
                    condition(n, Op::Eq, "1.5"),
                    condition(n, Op::Eq, "170141183460469231731687303715884105728"),
                ],
            })),
            ..Default::default()
        },
    );
    assert_eq!(by_text.total_filtered_rows, 2);
}

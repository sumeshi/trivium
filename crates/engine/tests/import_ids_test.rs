//! T08b: CSV and JSONL imports use internal column ids.
//!
//! Duplicate/empty CSV headers, reserved-looking source names, JSON Pointer
//! collisions, and the original record column are covered here.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use uuid::Uuid;

use trivium_engine::condition::{Condition, Node, Op};
use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::projects::{create_project, load_project, CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::project_io::read_original_records;
use trivium_engine::storage::load_flags;
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-import-ids-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn create(
    store: &ProjectsStore,
    path: &Path,
    restore_annotations: Option<bool>,
) -> trivium_engine::models::ProjectSummary {
    create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations,
        },
    )
    .expect("failed to create project")
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

fn condition(column: &str, value: &str) -> Node {
    Node::Condition(Condition {
        column: column.to_string(),
        op: Op::Eq,
        values: vec![value.to_string()],
        negated: false,
    })
}

fn export(store: &ProjectsStore, id: Uuid, destination: &Path, columns: Option<Vec<String>>) -> Vec<Vec<String>> {
    export_project(
        store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: None,
            spec: QuerySpec::default(),
            columns,
        },
    )
    .expect("export failed");

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(destination)
        .expect("failed to open export");
    reader
        .records()
        .map(|record| {
            record
                .expect("failed to read export record")
                .iter()
                .map(str::to_string)
                .collect()
        })
        .collect()
}

const CSV_BODY: &str = "msg,msg,,__rowid,trivium-memo,trivium-critical,zip,comma,quote,multiline,spaced,bigint\n\
first,second,,row-zero,memo one,1,00123,\"a,b\",\"say \"\"hi\"\"\",\"line1\nline2\",\"  padded  \",9007199254740993\n\
alpha,beta,,row-one,,0,00000,plain,quote,single line,space,9223372036854775807\n";

fn write_csv(root: &Path) -> PathBuf {
    let path = root.join("headers.csv");
    fs::write(&path, CSV_BODY).expect("failed to write csv fixture");
    path
}

#[test]
fn csv_duplicate_and_empty_headers_get_positional_ids() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_csv(&root);
    let summary = create(&store, &path, Some(false));
    let loaded = load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .expect("load failed");

    let sources: Vec<&str> = loaded
        .columns_meta
        .iter()
        .map(|column| column.source.as_str())
        .collect();
    assert_eq!(
        sources,
        vec![
            "msg",
            "msg",
            "",
            "__rowid",
            "trivium-memo",
            "trivium-critical",
            "zip",
            "comma",
            "quote",
            "multiline",
            "spaced",
            "bigint"
        ]
    );
    let ids: Vec<&str> = loaded.columns.iter().map(String::as_str).collect();
    assert_eq!(
        ids,
        vec![
            "c000001", "c000002", "c000003", "c000004", "c000005", "c000006", "c000007",
            "c000008", "c000009", "c000010", "c000011", "c000012"
        ],
        "every header position gets its own id, including duplicates and the empty header"
    );
    assert_eq!(
        loaded
            .columns_meta
            .iter()
            .map(|column| column.label.as_str())
            .collect::<Vec<_>>(),
        vec![
            "msg",
            "msg",
            "column 3",
            "__rowid",
            "trivium-memo",
            "trivium-critical",
            "zip",
            "comma",
            "quote",
            "multiline",
            "spaced",
            "bigint"
        ],
        "an empty header gets an auxiliary label while keeping its empty source"
    );
    assert!(!loaded.columns.iter().any(|id| id == "__original"));
    assert!(!loaded.initial_rows[0].data.contains_key("__original"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn csv_original_cells_and_duplicate_columns_keep_their_values() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_csv(&root);
    let summary = create(&store, &path, Some(false));
    let id = summary.meta.id;

    let records = read_original_records(&store.project_dir(&id)).expect("original records failed");
    assert_eq!(
        records[0],
        json!([
            "first",
            "second",
            "",
            "row-zero",
            "memo one",
            "1",
            "00123",
            "a,b",
            "say \"hi\"",
            "line1\nline2",
            "  padded  ",
            "9007199254740993"
        ])
    );
    assert_eq!(records[1][0], json!("alpha"));
    assert_eq!(records[1][6], json!("00000"));

    let first_msg = "c000001".to_string();
    let second_msg = "c000002".to_string();

    let second_condition = query(
        &store,
        id,
        QuerySpec {
            condition: Some(condition(&second_msg, "beta")),
            ..Default::default()
        },
    );
    assert_eq!(second_condition.total_filtered_rows, 1);
    assert_eq!(
        second_condition.rows[0].data.get(&second_msg),
        Some(&json!("beta"))
    );
    assert_eq!(
        second_condition.rows[0].data.get(&first_msg),
        Some(&json!("alpha"))
    );

    let first_scoped = query(
        &store,
        id,
        QuerySpec {
            search: Some("second".to_string()),
            search_columns: Some(vec![first_msg.clone()]),
            ..Default::default()
        },
    );
    assert_eq!(
        first_scoped.total_filtered_rows, 0,
        "a search scoped to the first duplicate must not read the second"
    );
    let second_scoped = query(
        &store,
        id,
        QuerySpec {
            search: Some("second".to_string()),
            search_columns: Some(vec![second_msg.clone()]),
            ..Default::default()
        },
    );
    assert_eq!(second_scoped.total_filtered_rows, 1);

    let sorted = query(
        &store,
        id,
        QuerySpec {
            sort_key: Some(second_msg.clone()),
            sort_direction: Some("asc".to_string()),
            ..Default::default()
        },
    );
    let sorted_second: Vec<Value> = sorted
        .rows
        .iter()
        .map(|row| row.data.get(&second_msg).cloned().unwrap_or(Value::Null))
        .collect();
    assert_eq!(sorted_second, vec![json!("beta"), json!("second")]);

    let destination = root.join("csv-export.csv");
    let rows = export(&store, id, &destination, None);
    assert_eq!(rows.len(), 3, "header plus two data rows");
    assert_eq!(
        rows[0],
        vec![
            "trivium-safe",
            "trivium-suspicious",
            "trivium-critical",
            "trivium-memo",
            "msg",
            "msg",
            "column 3",
            "__rowid",
            "trivium-memo",
            "trivium-critical",
            "zip",
            "comma",
            "quote",
            "multiline",
            "spaced",
            "bigint"
        ],
        "headers are labels and both duplicate msg columns survive"
    );
    assert!(
        !rows[0].iter().any(|header| header == "__original"),
        "the internal original record column must not be exported"
    );
    assert_eq!(rows[1][4], "first");
    assert_eq!(rows[1][5], "second");
    assert_eq!(rows[1][7], "row-zero");
    assert_eq!(rows[1][8], "memo one");
    assert_eq!(rows[1][11], "a,b");
    assert_eq!(rows[1][12], "say \"hi\"");
    assert_eq!(rows[1][13], "line1\nline2");
    assert_eq!(rows[1][14], "  padded  ");
    assert_eq!(rows[1][15], "9007199254740993");
    assert_eq!(rows[2][4], "alpha");
    assert_eq!(rows[2][5], "beta");
    assert_eq!(rows[2][7], "row-one");

    let msg_only = export(
        &store,
        id,
        &root.join("csv-msg.csv"),
        Some(vec![first_msg, second_msg]),
    );
    let selected_start = msg_only[0].len() - 2;
    assert_eq!(
        &msg_only[0][selected_start..],
        &["msg".to_string(), "msg".to_string()]
    );
    assert_eq!(msg_only[1][selected_start], "first");
    assert_eq!(msg_only[1][selected_start + 1], "second");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn restore_annotations_is_explicit_and_keeps_the_columns() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_csv(&root);

    let without = create(&store, &path, None);
    assert_eq!(without.meta.flagged_records, 0);
    let flags = load_flags(&store.project_dir(&without.meta.id).join("flags.json"))
        .expect("load flags");
    assert!(flags.is_empty(), "trivium-* must not become annotations by default");
    let loaded_without = load_project(
        &store,
        ProjectRequest {
            project_id: without.meta.id,
        },
    )
    .expect("load failed");
    assert!(loaded_without
        .columns_meta
        .iter()
        .any(|column| column.source == "trivium-memo"));
    assert!(loaded_without
        .columns_meta
        .iter()
        .any(|column| column.source == "trivium-critical"));

    let with = create(&store, &path, Some(true));
    assert_eq!(with.meta.flagged_records, 1);
    let flags = load_flags(&store.project_dir(&with.meta.id).join("flags.json"))
        .expect("load flags");
    let entry = flags.get(&0).expect("row 0 annotation");
    assert_eq!(entry.flag, "critical");
    assert_eq!(entry.memo.as_deref(), Some("memo one"));
    assert!(flags.get(&1).is_none(), "zero flag and blank memo is no entry");

    let loaded_with = load_project(
        &store,
        ProjectRequest {
            project_id: with.meta.id,
        },
    )
    .expect("load failed");
    assert!(
        loaded_with
            .columns_meta
            .iter()
            .any(|column| column.source == "trivium-memo"),
        "restoring annotations must not delete the source columns"
    );
    assert_eq!(
        loaded_with.columns.len(),
        loaded_without.columns.len(),
        "annotation restore must not change the data column set"
    );

    let _ = fs::remove_dir_all(root);
}

fn write_jsonl(root: &Path) -> PathBuf {
    let path = root.join("pointers.jsonl");
    let lines = [
        r#"{"a.b":1,"a":{"b":2},"__rowid":"source-row","trivium-memo":"source memo","only_later":null,"arr":[1,{"x":2}],"empty":""}"#,
        r#"{}"#,
        r#"{"a.b":3,"a":{"b":4},"only_later":"late","arr":[]}"#,
    ];
    fs::write(&path, lines.join("\n")).expect("failed to write jsonl fixture");
    path
}

#[test]
fn jsonl_pointer_paths_keep_collisions_apart() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_jsonl(&root);
    let summary = create(&store, &path, Some(false));
    let id = summary.meta.id;
    let loaded = load_project(&store, ProjectRequest { project_id: id }).expect("load failed");

    let sources: Vec<&str> = loaded
        .columns_meta
        .iter()
        .map(|column| column.source.as_str())
        .collect();
    assert_eq!(
        sources,
        vec![
            "/a.b",
            "/a/b",
            "/__rowid",
            "/trivium-memo",
            "/only_later",
            "/arr",
            "/empty"
        ]
    );
    let labels: Vec<&str> = loaded
        .columns_meta
        .iter()
        .map(|column| column.label.as_str())
        .collect();
    assert_eq!(
        labels,
        vec!["a.b", "a/b", "__rowid", "trivium-memo", "only_later", "arr", "empty"]
    );
    let ids: Vec<&str> = loaded.columns.iter().map(String::as_str).collect();
    assert_eq!(
        ids,
        vec![
            "c000001", "c000002", "c000003", "c000004", "c000005", "c000006", "c000007"
        ]
    );
    assert!(!loaded.columns.iter().any(|id| id == "__original"));
    assert!(!loaded.initial_rows[0].data.contains_key("__original"));

    let literal = "c000001".to_string();
    let nested = "c000002".to_string();
    assert_eq!(loaded.initial_rows.len(), 3, "the empty object is one row");
    assert_eq!(loaded.initial_rows[0].data.get(&literal), Some(&json!("1")));
    assert_eq!(loaded.initial_rows[0].data.get(&nested), Some(&json!("2")));
    assert_eq!(loaded.initial_rows[2].data.get(&literal), Some(&json!("3")));
    assert_eq!(loaded.initial_rows[2].data.get(&nested), Some(&json!("4")));

    let collision = query(
        &store,
        id,
        QuerySpec {
            condition: Some(condition(&literal, "3")),
            ..Default::default()
        },
    );
    assert_eq!(collision.total_filtered_rows, 1);
    assert_eq!(collision.rows[0].row_index, 2);
    assert_eq!(collision.rows[0].data.get(&nested), Some(&json!("4")));

    let sorted = query(
        &store,
        id,
        QuerySpec {
            sort_key: Some(nested.clone()),
            sort_direction: Some("desc".to_string()),
            ..Default::default()
        },
    );
    let sorted_nested: Vec<Value> = sorted
        .rows
        .iter()
        .map(|row| row.data.get(&nested).cloned().unwrap_or(Value::Null))
        .collect();
    let sorted_non_null: Vec<Value> = sorted_nested
        .into_iter()
        .filter(|value| !value.is_null())
        .collect();
    assert_eq!(sorted_non_null, vec![json!("4"), json!("2")]);

    let records = read_original_records(&store.project_dir(&id)).expect("original records failed");
    assert_eq!(records[1], json!({}));
    let first = records[0].as_object().expect("first record object");
    assert_eq!(first.get("a.b"), Some(&json!(1)));
    assert_eq!(first.get("a").and_then(|value| value.get("b")), Some(&json!(2)));
    assert_eq!(first.get("__rowid"), Some(&json!("source-row")));
    assert_eq!(first.get("trivium-memo"), Some(&json!("source memo")));
    assert_eq!(first.get("only_later"), Some(&Value::Null));
    assert_eq!(first.get("arr"), Some(&json!([1, {"x": 2}])));

    let destination = root.join("jsonl-export.csv");
    let rows = export(&store, id, &destination, None);
    assert_eq!(rows.len(), 4, "header plus three records");
    let header = &rows[0];
    assert!(header.contains(&"a.b".to_string()));
    assert!(header.contains(&"a/b".to_string()));
    assert!(
        !header.iter().any(|name| name == "__original"),
        "the internal original record column must not be exported"
    );
    let literal_index = header.iter().position(|name| name == "a.b").unwrap();
    let nested_index = header.iter().position(|name| name == "a/b").unwrap();
    assert_eq!(rows[1][literal_index], "1");
    assert_eq!(rows[1][nested_index], "2");

    let _ = fs::remove_dir_all(root);
}

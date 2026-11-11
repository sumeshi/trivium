//! T08a: new storage format with internal column ids and original records.
//!
//! The importer still writes the legacy format until T08b, so the new format
//! fixture is built by hand here.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use polars::prelude::{DataFrame, NamedFrom, Series};
use serde_json::{json, Value};
use uuid::Uuid;

use trivium_engine::condition::{Condition, Node, Op};
use trivium_engine::models::{ColumnMeta, ProjectMeta};
use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::projects::{load_project, ProjectRequest};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::project_io::{
    is_internal_column_for_version, read_original_records, write_project_dataframe,
};
use trivium_engine::store::ProjectsStore;

mod common;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-storage-v2-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn column(id: &str, label: &str, source: &str, order: usize) -> ColumnMeta {
    ColumnMeta {
        id: id.to_string(),
        label: label.to_string(),
        source: source.to_string(),
        order,
    }
}

fn create_new_format_project(root: &Path) -> (ProjectsStore, Uuid) {
    let store = ProjectsStore::new(root.to_path_buf()).expect("failed to create store");
    let id = Uuid::new_v4();
    let project_dir = store.project_dir(&id);
    fs::create_dir_all(&project_dir).expect("failed to create project dir");

    let originals = vec![
        json!({
            "a": { "b": 1 },
            "a.b": "literal",
            "n": 9007199254740993_i64,
            "z": null,
            "secret": "original-only"
        })
        .to_string(),
        json!({
            "a": { "b": 2 },
            "a.b": "other",
            "n": -1
        })
        .to_string(),
        json!(["00123", "a,b", "9007199254740993"]).to_string(),
    ];

    let mut df = DataFrame::new(vec![
        Series::new("c000001", &["alpha", "beta", "gamma"]),
        Series::new("c000002", &["x", "y", "z"]),
        // This source name matches an internal column, but its internal id is
        // c000003, so metadata must keep it as ordinary data.
        Series::new("c000003", &["reserved-a", "reserved-b", "reserved-c"]),
        Series::new("__rowid", &[0_i64, 1, 2]),
        Series::new("__original", originals),
    ])
    .expect("failed to build new format frame");
    write_project_dataframe(&project_dir.join("data.parquet"), &mut df)
        .expect("failed to write new format parquet");

    let meta = ProjectMeta {
        id,
        name: "storage-v2".to_string(),
        description: None,
        created_at: Utc::now(),
        total_records: 3,
        flagged_records: 0,
        ioc_applied_records: 0,
        source_sha256: None,
        source_file_name: None,
        source_format: None,
        source_size: None,
        source_path: None,
        hidden_columns: Vec::new(),
        storage_version: 2,
        columns: vec![
            column("c000001", "name", "name", 0),
            column("c000002", "name", "alias", 1),
            column("c000003", "__rowid", "__rowid", 2),
        ],
    };
    store.insert(meta).expect("failed to insert metadata");
    (store, id)
}

fn condition(column: &str, value: &str) -> Node {
    Node::Condition(Condition {
        column: column.to_string(),
        op: Op::Eq,
        values: vec![value.to_string()],
        negated: false,
    })
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

#[test]
fn load_returns_metadata_and_hides_internal_columns() {
    let root = temp_root();
    let (store, id) = create_new_format_project(&root);

    let loaded = load_project(&store, ProjectRequest { project_id: id }).expect("load failed");
    assert_eq!(loaded.columns, vec!["c000001", "c000002", "c000003"]);
    assert_eq!(
        loaded.columns_meta,
        vec![
            column("c000001", "name", "name", 0),
            column("c000002", "name", "alias", 1),
            column("c000003", "__rowid", "__rowid", 2)
        ]
    );
    assert_eq!(
        loaded.columns_meta[2].label, "__rowid",
        "a data column may share a name with an internal column"
    );
    assert!(!loaded.columns.iter().any(|name| name.starts_with("__")));
    assert_eq!(
        loaded.column_kinds.get("c000001").map(String::as_str),
        Some("string")
    );
    assert!(!loaded.column_kinds.contains_key("__original"));
    assert!(!loaded.column_max_chars.contains_key("__rowid"));
    assert!(!loaded.column_max_chars.contains_key("__original"));
    assert_eq!(
        common::cell_text(&loaded.initial_rows[0], "c000001"),
        Some("alpha")
    );
    assert!(!loaded.initial_rows[0].cells.contains_key("__original"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn same_label_columns_are_addressed_by_id() {
    let root = temp_root();
    let (store, id) = create_new_format_project(&root);

    let first = query(
        &store,
        id,
        QuerySpec {
            condition: Some(condition("c000001", "beta")),
            ..Default::default()
        },
    );
    assert_eq!(first.total_filtered_rows, 1);
    assert_eq!(common::cell_text(&first.rows[0], "c000001"), Some("beta"));
    assert_eq!(common::cell_text(&first.rows[0], "c000002"), Some("y"));

    let second = query(
        &store,
        id,
        QuerySpec {
            condition: Some(condition("c000002", "x")),
            ..Default::default()
        },
    );
    assert_eq!(second.total_filtered_rows, 1);
    assert_eq!(common::cell_text(&second.rows[0], "c000001"), Some("alpha"));
    assert_eq!(common::cell_text(&second.rows[0], "c000002"), Some("x"));

    let scoped = query(
        &store,
        id,
        QuerySpec {
            search: Some("z".to_string()),
            search_columns: Some(vec!["c000002".to_string()]),
            ..Default::default()
        },
    );
    assert_eq!(scoped.total_filtered_rows, 1);
    assert_eq!(common::cell_text(&scoped.rows[0], "c000001"), Some("gamma"));

    let reserved_name = query(
        &store,
        id,
        QuerySpec {
            condition: Some(condition("c000003", "reserved-b")),
            ..Default::default()
        },
    );
    assert_eq!(reserved_name.total_filtered_rows, 1);
    assert_eq!(
        common::cell_text(&reserved_name.rows[0], "c000003"),
        Some("reserved-b")
    );

    // The original record column is not part of the default search scope.
    let original_only = query(
        &store,
        id,
        QuerySpec {
            search: Some("original-only".to_string()),
            ..Default::default()
        },
    );
    assert_eq!(original_only.total_filtered_rows, 0);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn original_records_preserve_types_nulls_and_missing_keys() {
    let root = temp_root();
    let (store, id) = create_new_format_project(&root);

    let records = read_original_records(&store.project_dir(&id)).expect("original records failed");
    assert_eq!(records.len(), 3);

    let first = records[0].as_object().expect("first record object");
    assert_eq!(
        first.get("n").and_then(Value::as_i64),
        Some(9_007_199_254_740_993)
    );
    assert_eq!(first.get("z"), Some(&Value::Null));
    assert!(first.get("missing").is_none());
    assert_eq!(
        first.get("a").and_then(|value| value.get("b")),
        Some(&json!(1))
    );
    assert_eq!(first.get("a.b"), Some(&json!("literal")));

    let second = records[1].as_object().expect("second record object");
    assert!(second.get("z").is_none(), "missing stays missing");
    assert_eq!(
        second.get("a").and_then(|value| value.get("b")),
        Some(&json!(2))
    );

    assert_eq!(
        records[2],
        json!(["00123", "a,b", "9007199254740993"])
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn export_uses_labels_and_excludes_ids_and_internal_columns() {
    let root = temp_root();
    let (store, id) = create_new_format_project(&root);
    let destination = root.join("export.csv");

    export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: Some(vec!["c000002".to_string(), "c000001".to_string()]),
            row_indices: None,
            include_annotations: None,
        },
    )
    .expect("export failed");

    let text = fs::read_to_string(&destination).expect("failed to read export");
    assert!(!text.contains("c000001"));
    assert!(!text.contains("c000002"));
    assert!(!text.contains("__rowid"));
    assert!(!text.contains("__original"));

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(text.as_bytes());
    let records: Vec<csv::StringRecord> = reader
        .records()
        .collect::<Result<_, _>>()
        .expect("failed to parse exported csv");

    assert_eq!(
        records[0].iter().collect::<Vec<_>>(),
        vec![
            "trivium-safe",
            "trivium-suspicious",
            "trivium-critical",
            "trivium-memo",
            "trivium-tags",
            "trivium-bookmarked",
            "name",
            "name"
        ]
    );
    assert_eq!(records.len(), 4, "header plus three rows");
    assert_eq!(&records[1][6], "x");
    assert_eq!(&records[1][7], "alpha");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn legacy_project_loads_with_synthesized_column_metadata() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = Uuid::new_v4();
    let project_dir = store.project_dir(&id);
    fs::create_dir_all(&project_dir).expect("failed to create project dir");

    // Version 0 stores the original column names as frame columns and has no
    // column metadata. New imports never write this shape any more.
    let mut df = DataFrame::new(vec![
        Series::new("name", &["alpha", "beta"]),
        Series::new("count", &[1_i64, 2]),
        Series::new("__rowid", &[0_i64, 1]),
    ])
    .expect("failed to build legacy frame");
    write_project_dataframe(&project_dir.join("data.parquet"), &mut df)
        .expect("failed to write legacy parquet");
    store
        .insert(ProjectMeta {
            id,
            name: "legacy".to_string(),
            description: None,
            created_at: Utc::now(),
            total_records: 2,
            flagged_records: 0,
            ioc_applied_records: 0,
            source_sha256: None,
            source_file_name: None,
            source_format: None,
            source_size: None,
            source_path: None,
            hidden_columns: Vec::new(),
            storage_version: 0,
            columns: Vec::new(),
        })
        .expect("failed to insert legacy metadata");

    let loaded = load_project(&store, ProjectRequest { project_id: id }).expect("load failed");
    assert_eq!(loaded.columns, vec!["name", "count"]);
    assert_eq!(
        loaded.columns_meta,
        vec![
            column("name", "name", "name", 0),
            column("count", "count", "count", 1)
        ]
    );

    // Legacy conditions keep referring to the original column name.
    let rows = query(
        &store,
        id,
        QuerySpec {
            condition: Some(condition("name", "alpha")),
            ..Default::default()
        },
    );
    assert_eq!(rows.total_filtered_rows, 1);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn source_record_names_are_internal_only_for_the_new_storage_format() {
    // The transitional T12 name is internal for version 2 projects but stays a
    // data column in an old project that genuinely used that header.
    assert!(!is_internal_column_for_version("__source_record", 0));
    assert!(is_internal_column_for_version("__source_record", 2));
    assert!(is_internal_column_for_version(
        "__trivium_source_record",
        2
    ));
    // The original internal names are reserved in every version.
    assert!(is_internal_column_for_version("__rowid", 0));
    assert!(is_internal_column_for_version("__original", 0));
}

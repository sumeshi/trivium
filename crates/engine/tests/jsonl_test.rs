//! JSONL import: JSON Pointer flattening, column union, typed columns, and
//! create_project dispatch.

use std::fs;
use std::path::{Path, PathBuf};

use polars::prelude::{AnyValue, DataType};
use uuid::Uuid;

use trivium_engine::import::jsonl;
use trivium_engine::ops;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-jsonl-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn write_jsonl(root: &Path, name: &str, lines: &[String]) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, lines.join("\n")).expect("failed to write fixture");
    path
}

fn value_at<'a>(
    df: &'a polars::prelude::DataFrame,
    column: &str,
    row: usize,
) -> AnyValue<'a> {
    df.column(column)
        .unwrap_or_else(|_| panic!("missing column {}", column))
        .get(row)
        .expect("failed to read row value")
}

#[test]
fn nested_objects_flatten_to_pointer_columns() {
    let root = temp_root();
    let path = write_jsonl(
        &root,
        "nested.jsonl",
        &[
            r#"{"ts": "2024-01-01T00:00:01", "host": {"name": "WS01", "ip": "10.0.0.1"}, "user": "alice"}"#.to_string(),
            r#"{"ts": "2024-01-01T00:00:02", "host": {"name": "WS02", "ip": "10.0.0.2"}, "user": "bob"}"#.to_string(),
        ],
    );

    let imported = jsonl::load(&path).expect("jsonl load failed");
    assert!(imported.flags.is_empty(), "jsonl import carries no flag entries");
    assert_eq!(
        imported
            .columns
            .iter()
            .map(|column| column.source.as_str())
            .collect::<Vec<_>>(),
        vec!["/ts", "/host/name", "/host/ip", "/user"],
        "columns must be the first-appearance union with JSON Pointer paths"
    );
    assert_eq!(
        imported
            .columns
            .iter()
            .map(|column| column.label.as_str())
            .collect::<Vec<_>>(),
        vec!["ts", "host/name", "host/ip", "user"]
    );
    let df = imported.frame;
    assert_eq!(df.height(), 2);
    let name_id = imported.columns[1].id.clone();
    let ip_id = imported.columns[2].id.clone();
    assert_eq!(
        value_at(&df, &name_id, 0).get_str(),
        Some("WS01"),
        "one-level nested keys must flatten to pointer columns"
    );
    assert_eq!(value_at(&df, &ip_id, 1).get_str(), Some("10.0.0.2"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn missing_keys_become_null_across_column_union() {
    let root = temp_root();
    let path = write_jsonl(
        &root,
        "union.jsonl",
        &[
            r#"{"a": 1}"#.to_string(),
            r#"{"b": "x"}"#.to_string(),
        ],
    );

    let imported = jsonl::load(&path).expect("jsonl load failed");
    assert_eq!(
        imported
            .columns
            .iter()
            .map(|column| column.source.as_str())
            .collect::<Vec<_>>(),
        vec!["/a", "/b"]
    );
    let df = imported.frame;
    assert_eq!(df.height(), 2);
    let a_id = imported.columns[0].id.clone();
    let b_id = imported.columns[1].id.clone();
    assert!(value_at(&df, &b_id, 0).is_null(), "absent key row 0 must be null");
    assert!(value_at(&df, &a_id, 1).is_null(), "absent key row 1 must be null");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn numeric_and_boolean_values_are_stored_as_exact_text() {
    let root = temp_root();
    let path = write_jsonl(
        &root,
        "typed.jsonl",
        &[
            r#"{"eventid": 4624, "score": 1.5, "ok": true}"#.to_string(),
            r#"{"eventid": 4688, "score": 2.5, "ok": false}"#.to_string(),
        ],
    );

    let imported = jsonl::load(&path).expect("jsonl load failed");
    let df = imported.frame;
    let event_id = imported.columns[0].id.clone();
    let score_id = imported.columns[1].id.clone();
    let ok_id = imported.columns[2].id.clone();
    // T09: values are stored exactly as text; kinds are metadata, not storage.
    for id in [&event_id, &score_id, &ok_id] {
        assert_eq!(
            df.column(id).expect("column").dtype(),
            &DataType::String,
            "json scalars are kept as exact text"
        );
    }
    assert_eq!(value_at(&df, &event_id, 0).get_str(), Some("4624"));
    assert_eq!(value_at(&df, &score_id, 0).get_str(), Some("1.5"));
    assert_eq!(value_at(&df, &ok_id, 1).get_str(), Some("false"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn ops_create_project_accepts_jsonl_extension() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_jsonl(
        &root,
        "events.jsonl",
        &[
            r#"{"ts": "2024-01-01T00:00:01", "proc": {"name": "explorer.exe", "pid": 100}}"#.to_string(),
            r#"{"ts": "2024-01-01T00:00:02", "proc": {"name": "cmd.exe", "pid": 200}}"#.to_string(),
        ],
    );

    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project from jsonl fixture");
    assert_eq!(summary.meta.total_records, 2);
    assert_eq!(summary.meta.storage_version, 2);
    assert_eq!(summary.meta.columns.len(), 3);

    let loaded = ops::projects::load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .expect("failed to load created project");
    assert_eq!(loaded.columns.len(), 3);
    assert!(
        loaded
            .columns_meta
            .iter()
            .any(|column| column.source == "/proc/pid"),
        "flattened /proc/pid column must be present"
    );

    let _ = fs::remove_dir_all(root);
}

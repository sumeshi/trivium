//! JSONL import: nested key flattening, column union, typed columns, and create_project dispatch.

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
fn nested_objects_flatten_to_dotted_columns() {
    let root = temp_root();
    let path = write_jsonl(
        &root,
        "nested.jsonl",
        &[
            r#"{"ts": "2024-01-01T00:00:01", "host": {"name": "WS01", "ip": "10.0.0.1"}, "user": "alice"}"#.to_string(),
            r#"{"ts": "2024-01-01T00:00:02", "host": {"name": "WS02", "ip": "10.0.0.2"}, "user": "bob"}"#.to_string(),
        ],
    );

    let (df, flags) = jsonl::load(&path).expect("jsonl load failed");
    assert!(flags.is_empty(), "jsonl import carries no flag entries");
    assert_eq!(
        df.get_column_names(),
        vec!["ts", "host.name", "host.ip", "user", "__rowid"],
        "columns must be the first-appearance union with dotted nested keys"
    );
    assert_eq!(df.height(), 2);
    assert_eq!(
        value_at(&df, "host.name", 0).get_str(),
        Some("WS01"),
        "one-level nested keys must flatten to a.b columns"
    );
    assert_eq!(value_at(&df, "host.ip", 1).get_str(), Some("10.0.0.2"));

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

    let (df, _) = jsonl::load(&path).expect("jsonl load failed");
    assert_eq!(df.get_column_names(), vec!["a", "b", "__rowid"]);
    assert_eq!(df.height(), 2);
    assert!(value_at(&df, "b", 0).is_null(), "absent key row 0 must be null");
    assert!(value_at(&df, "a", 1).is_null(), "absent key row 1 must be null");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn numeric_and_boolean_columns_keep_their_types() {
    let root = temp_root();
    let path = write_jsonl(
        &root,
        "typed.jsonl",
        &[
            r#"{"eventid": 4624, "score": 1.5, "ok": true}"#.to_string(),
            r#"{"eventid": 4688, "score": 2.5, "ok": false}"#.to_string(),
        ],
    );

    let (df, _) = jsonl::load(&path).expect("jsonl load failed");
    assert_eq!(
        df.column("eventid").expect("eventid").dtype(),
        &DataType::Int64,
        "all-integer columns must stay Int64"
    );
    assert_eq!(
        df.column("score").expect("score").dtype(),
        &DataType::Float64,
        "float columns must stay Float64"
    );
    assert_eq!(
        df.column("ok").expect("ok").dtype(),
        &DataType::Boolean,
        "all-boolean columns must stay Boolean"
    );

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
        },
    )
    .expect("failed to create project from jsonl fixture");
    assert_eq!(summary.meta.total_records, 2);

    let loaded = ops::projects::load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .expect("failed to load created project");
    assert_eq!(loaded.columns.len(), 3);
    assert!(
        loaded.columns.contains(&"proc.pid".to_string()),
        "flattened proc.pid column must be present"
    );

    let _ = fs::remove_dir_all(root);
}

//! T11: JSON arrays and JSONL are opened from the GUI.
//!
//! A JSON file holds one object or an array of objects; JSONL/NDJSON holds one
//! object per physical line. Both use the same record-to-column conversion as
//! the existing JSONL import (JSON Pointer columns, column union, `__original`).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::import::{json, jsonl};
use trivium_engine::models::{ProjectRow, ProjectSummary};
use trivium_engine::ops;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-json-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn write_file(root: &Path, name: &str, body: &str) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn create(store: &ProjectsStore, path: &Path) -> ProjectSummary {
    ops::projects::create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
}

fn load_rows(store: &ProjectsStore, summary: &ProjectSummary) -> Vec<ProjectRow> {
    ops::rows::query_project_rows(
        store,
        ops::rows::QueryRowsPayload {
            project_id: summary.meta.id,
            spec: ops::rows::QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .expect("query failed")
    .rows
}

fn source_ids(summary: &ProjectSummary) -> HashMap<String, String> {
    summary
        .meta
        .columns
        .iter()
        .map(|column| (column.source.clone(), column.id.clone()))
        .collect()
}

fn label_ids(summary: &ProjectSummary) -> HashMap<String, String> {
    summary
        .meta
        .columns
        .iter()
        .map(|column| (column.label.clone(), column.id.clone()))
        .collect()
}

fn cell_text<'a>(row: &'a ProjectRow, id: &str) -> Option<&'a str> {
    row.cells
        .get(id)
        .expect("missing cell")
        .text
        .as_deref()
}

#[test]
fn json_array_becomes_one_row_per_element_with_later_keys() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(
        &root,
        "events.json",
        r#"[
  {"ts": "t1", "user": "alice"},
  {"ts": "t2", "user": "bob"},
  {"ts": "t3", "user": "carol", "only_later": "late"}
]"#,
    );

    let imported = json::load(&path).expect("json load failed");
    assert_eq!(
        imported
            .columns
            .iter()
            .map(|column| column.source.as_str())
            .collect::<Vec<_>>(),
        vec!["/ts", "/user", "/only_later"],
        "array elements share one column union"
    );
    assert_eq!(imported.frame.height(), 3, "the array itself is not one cell");

    let summary = create(&store, &path);
    assert_eq!(summary.meta.total_records, 3);
    let ids = source_ids(&summary);
    let rows = load_rows(&store, &summary);
    assert_eq!(rows.len(), 3);
    assert_eq!(cell_text(&rows[0], &ids["/user"]), Some("alice"));
    assert_eq!(cell_text(&rows[2], &ids["/user"]), Some("carol"));
    assert_eq!(
        cell_text(&rows[2], &ids["/only_later"]),
        Some("late"),
        "a key that first appears in the last record stays a column"
    );
    assert!(
        cell_text(&rows[0], &ids["/only_later"]).is_none(),
        "an absent key stays null, not an empty string"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn json_single_object_becomes_one_row() {
    let root = temp_root();
    let path = write_file(
        &root,
        "single.json",
        r#"{"ts": "t1", "host": {"name": "WS01", "ip": "10.0.0.1"}}"#,
    );

    let imported = json::load(&path).expect("json load failed");
    assert_eq!(
        imported
            .columns
            .iter()
            .map(|column| column.source.as_str())
            .collect::<Vec<_>>(),
        vec!["/ts", "/host/name", "/host/ip"],
        "nested objects use the shared JSON Pointer conversion"
    );
    assert_eq!(imported.frame.height(), 1);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn non_object_array_element_reports_its_position() {
    let root = temp_root();
    let path = write_file(&root, "bad_element.json", r#"[{"a": 1}, 2]"#);

    let err = json::load(&path)
        .err()
        .expect("a numeric array element must fail");
    assert!(
        err.to_string()
            .contains("Record 2 in the JSON array is not an object."),
        "unexpected error: {}",
        err
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn broken_json_fails_instead_of_falling_back_to_csv() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(&root, "broken.json", "{\"a\": 1,");

    let err = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect_err("broken JSON must not be imported as CSV");
    assert!(
        err.to_string().starts_with("Failed to parse JSON:"),
        "unexpected error: {}",
        err
    );
    assert!(
        store.all().is_empty(),
        "a failed import must not register a project"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn uppercase_json_and_ndjson_extensions_are_detected() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");

    let json_path = write_file(
        &root,
        "EVENTS.JSON",
        r#"[{"ts": "t1"}, {"ts": "t2"}, {"ts": "t3"}]"#,
    );
    let json_summary = create(&store, &json_path);
    assert_eq!(json_summary.meta.total_records, 3);

    let ndjson_path = write_file(
        &root,
        "EVENTS.NDJSON",
        "{\"ts\": \"t1\"}\n\n{\"ts\": \"t2\"}\n{\"ts\": \"t3\"}\n",
    );
    let ndjson_summary = create(&store, &ndjson_path);
    assert_eq!(ndjson_summary.meta.total_records, 3);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn csv_json_and_jsonl_import_the_same_three_records() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");

    let csv_path = write_file(
        &root,
        "same.csv",
        "ts,user,count\n2024-01-01T00:00:01,alice,1\n2024-01-01T00:00:02,bob,2\n2024-01-01T00:00:03,carol,3\n",
    );
    let json_path = write_file(
        &root,
        "same.json",
        r#"[
  {"ts": "2024-01-01T00:00:01", "user": "alice", "count": 1},
  {"ts": "2024-01-01T00:00:02", "user": "bob", "count": 2},
  {"ts": "2024-01-01T00:00:03", "user": "carol", "count": 3}
]"#,
    );
    let jsonl_path = write_file(
        &root,
        "same.jsonl",
        "{\"ts\": \"2024-01-01T00:00:01\", \"user\": \"alice\", \"count\": 1}\n\
         {\"ts\": \"2024-01-01T00:00:02\", \"user\": \"bob\", \"count\": 2}\n\
         {\"ts\": \"2024-01-01T00:00:03\", \"user\": \"carol\", \"count\": 3}\n",
    );

    let mut snapshots = Vec::new();
    for path in [&csv_path, &json_path, &jsonl_path] {
        let summary = create(&store, path);
        assert_eq!(summary.meta.total_records, 3, "{}", path.display());
        let ids = label_ids(&summary);
        let rows = load_rows(&store, &summary);
        assert_eq!(rows.len(), 3, "{}", path.display());
        let values: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                ["ts", "user", "count"]
                    .iter()
                    .map(|label| {
                        cell_text(row, &ids[*label])
                            .unwrap_or_else(|| panic!("missing {}", label))
                            .to_string()
                    })
                    .collect()
            })
            .collect();
        snapshots.push(values);
    }
    assert_eq!(
        snapshots[0], snapshots[1],
        "CSV and JSON must produce the same rows"
    );
    assert_eq!(
        snapshots[0], snapshots[2],
        "CSV and JSONL must produce the same rows"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn jsonl_errors_keep_the_physical_line_number_across_blank_lines() {
    let root = temp_root();
    let path = write_file(
        &root,
        "broken.jsonl",
        "{\"a\": 1}\n\n{\"a\": 2,\n{\"a\": 3}\n",
    );

    let err = jsonl::load(&path)
        .err()
        .expect("broken JSONL must fail");
    assert!(
        err.to_string().contains("line 3"),
        "a blank line is skipped but the error must name line 3: {}",
        err
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn load_project_survives_json_array_import() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(
        &root,
        "reopen.json",
        r#"[{"a": 1}, {"a": 2}, {"b": 3}]"#,
    );
    let summary = create(&store, &path);

    let loaded = ops::projects::load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .expect("failed to load created project");
    assert_eq!(loaded.columns.len(), 2, "union of /a and /b");
    assert_eq!(loaded.initial_rows.len(), 3);

    let _ = fs::remove_dir_all(root);
}

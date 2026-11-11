//! T20c: list pages carry cell previews, and `get_cell` fetches the full value.
//!
//! The read counters are process-global, so every test in this binary takes the
//! same mutex before touching a project.

use std::collections::HashMap;
use std::fs;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use trivium_engine::ops;
use trivium_engine::ops::cell::{get_cell, GetCellPayload};
use trivium_engine::ops::flags::{set_hidden_columns, HiddenColumnsPayload};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::project_io::{
    last_page_projection, project_full_reads, project_page_reads, reset_project_read_counters,
};
use trivium_engine::store::ProjectsStore;

mod common;

const LONG_CHARS: usize = 300_000;
const PREVIEW_CHARS: usize = 256;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn test_guard() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn temp_root(label: &str) -> std::path::PathBuf {
    let root =
        std::env::temp_dir().join(format!("trivium-cell-preview-{label}-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn create(store: &ProjectsStore, path: &std::path::Path) -> Uuid {
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
    .meta
    .id
}

fn page_rows(store: &ProjectsStore, id: Uuid) -> Vec<trivium_engine::models::ProjectRow> {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .expect("query failed")
    .rows
}

fn cell_payload(id: Uuid, row_index: usize, column_id: &str) -> GetCellPayload {
    GetCellPayload {
        project_id: id,
        row_index,
        column_id: column_id.to_string(),
    }
}

#[test]
fn long_cell_page_is_a_preview_and_get_cell_returns_the_full_value() {
    let _guard = test_guard();
    let root = temp_root("long");
    let long_text = "a".repeat(LONG_CHARS);
    let csv = root.join("long.csv");
    fs::write(&csv, format!("id,payload\nshort,small\nlong,{long_text}\n"))
        .expect("write csv fixture");
    let store = ProjectsStore::new(root.clone()).expect("store");
    let id = create(&store, &csv);
    let ids: HashMap<String, String> = common::column_ids(&store, id);
    let payload_id = common::id(&ids, "payload");
    let id_id = common::id(&ids, "id");

    let rows = page_rows(&store, id);
    assert_eq!(rows.len(), 2);
    let long_row = rows
        .iter()
        .find(|row| row.row_index == 1)
        .expect("long row");
    let preview = long_row.cells.get(&payload_id).expect("payload cell");
    assert_eq!(
        preview.text.as_deref().map(|text| text.chars().count()),
        Some(PREVIEW_CHARS)
    );
    assert!(preview.truncated, "the list cell must be marked truncated");
    assert_eq!(preview.length, Some(LONG_CHARS));
    assert_eq!(preview.kind, "string");

    // The compatibility `data` field is not truncated; only `cells` is a
    // preview. The frontend drops `data` from its page cache.
    let data_text = long_row
        .data
        .get(&payload_id)
        .and_then(|value| value.as_str())
        .expect("data payload");
    assert_eq!(data_text.chars().count(), LONG_CHARS);
    assert_eq!(data_text, long_text);

    // Hidden columns are presentation-only: get_cell still serves them.
    set_hidden_columns(
        &store,
        HiddenColumnsPayload {
            project_id: id,
            hidden_columns: vec![payload_id.clone()],
        },
    )
    .expect("hide payload");

    reset_project_read_counters();
    let full = get_cell(&store, cell_payload(id, 1, &payload_id)).expect("get cell");
    assert_eq!(full.text.as_deref(), Some(long_text.as_str()));
    assert!(!full.truncated);
    assert_eq!(full.length, Some(LONG_CHARS));
    assert_eq!(full.kind, "string");

    assert_eq!(
        project_full_reads(),
        0,
        "get_cell must not read the whole frame"
    );
    assert!(
        project_page_reads() >= 1,
        "get_cell must use the row-page reader"
    );
    let projection = last_page_projection();
    assert!(projection.contains(&payload_id));
    assert!(projection.contains(&"__original".to_string()));
    assert!(
        !projection.contains(&id_id),
        "get_cell must project only the requested column"
    );
    assert!(
        !projection.contains(&"__rowid".to_string()),
        "get_cell does not need the row-id anchor"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn get_cell_keeps_null_missing_and_empty_kinds() {
    let _guard = test_guard();
    let root = temp_root("kinds");
    let jsonl = root.join("kinds.jsonl");
    fs::write(
        &jsonl,
        concat!(
            "{\"nullv\": null, \"empty\": \"\", \"other\": 1}\n",
            "{\"nullv\": null, \"empty\": \"\", \"missing\": \"present\"}\n",
        ),
    )
    .expect("write jsonl fixture");
    let store = ProjectsStore::new(root.clone()).expect("store");
    let id = create(&store, &jsonl);
    let ids: HashMap<String, String> = common::column_ids(&store, id);
    let rows = page_rows(&store, id);
    let row = &rows[0];

    for source in ["/nullv", "/empty", "/missing"] {
        let column_id = common::id(&ids, source);
        let page_cell = row.cells.get(&column_id).expect("page cell");
        let full = get_cell(&store, cell_payload(id, row.row_index, &column_id)).expect("get cell");
        assert_eq!(full.text, page_cell.text, "{source} text differs");
        assert_eq!(full.kind, page_cell.kind, "{source} kind differs");
        assert_eq!(full.length, page_cell.length, "{source} length differs");
        assert!(!full.truncated);
    }

    assert_eq!(
        row.cells.get(&common::id(&ids, "/nullv")).unwrap().kind,
        "null"
    );
    assert_eq!(
        row.cells.get(&common::id(&ids, "/empty")).unwrap().kind,
        "string"
    );
    assert_eq!(
        row.cells.get(&common::id(&ids, "/missing")).unwrap().kind,
        "missing"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn get_cell_rejects_internal_unknown_and_out_of_range_rows() {
    let _guard = test_guard();
    let root = temp_root("errors");
    let csv = root.join("errors.csv");
    fs::write(&csv, "id,payload\n0,hello\n1,world\n").expect("write csv fixture");
    let store = ProjectsStore::new(root.clone()).expect("store");
    let id = create(&store, &csv);
    let ids: HashMap<String, String> = common::column_ids(&store, id);
    let payload_id = common::id(&ids, "payload");

    for internal in ["__rowid", "__original"] {
        assert!(
            get_cell(&store, cell_payload(id, 0, internal)).is_err(),
            "{internal} must be rejected"
        );
    }
    assert!(
        get_cell(&store, cell_payload(id, 0, "c999999")).is_err(),
        "unknown column must be rejected"
    );

    let total_rows = store.find(&id).expect("project meta").total_records;
    assert!(
        get_cell(&store, cell_payload(id, total_rows, &payload_id)).is_err(),
        "out-of-range row must be rejected"
    );
    assert!(get_cell(&store, cell_payload(id, 0, &payload_id)).is_ok());

    let _ = fs::remove_dir_all(root);
}

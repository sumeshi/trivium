//! T20a: the query page path reads only the requested page and columns.
//!
//! The counters are process-global, so this binary serializes its tests on one
//! mutex and resets the counters immediately before the measured query.

use std::fs;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use trivium_engine::ops;
use trivium_engine::ops::flags::HiddenColumnsPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{
    query_project_rows, QueryRowsPayload, QueryRowsResponse, QuerySpec,
};
use trivium_engine::project_io::{
    last_column_read_projection, last_page_projection, project_full_reads, project_page_reads,
    reset_project_read_counters,
};
use trivium_engine::store::ProjectsStore;

mod common;

const ROWS: usize = 100;
const PAGE: usize = 10;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn page_test_guard() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn setup_project() -> (
    std::path::PathBuf,
    ProjectsStore,
    Uuid,
    std::collections::HashMap<String, String>,
) {
    let root = std::env::temp_dir().join(format!("trivium-page-read-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let csv = root.join("page.csv");
    let mut body = String::from("id,group,note\n");
    for index in 0..ROWS {
        let note = match index {
            7 | 42 | 77 => "needle".to_string(),
            _ => format!("note{index:03}"),
        };
        body.push_str(&format!("row{index:03},g{},{}\n", index % 3, note));
    }
    fs::write(&csv, body).expect("failed to write fixture");

    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    (root, store, id, ids)
}

fn query(
    store: &ProjectsStore,
    id: Uuid,
    spec: QuerySpec,
    offset: usize,
    limit: usize,
) -> QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec,
            offset: Some(offset),
            limit: Some(limit),
        },
    )
    .expect("query failed")
}

fn row_ids(response: &QueryRowsResponse) -> Vec<usize> {
    response.rows.iter().map(|row| row.row_index).collect()
}

#[test]
fn unconditional_page_moves_read_pages_not_the_whole_frame() {
    let _guard = page_test_guard();
    let (root, store, id, ids) = setup_project();
    let id_column = common::id(&ids, "id");

    reset_project_read_counters();
    let mut seen = Vec::new();
    for offset in [0, PAGE, PAGE * 2] {
        let response = query(&store, id, QuerySpec::default(), offset, PAGE);
        assert_eq!(response.total_rows, ROWS);
        assert_eq!(response.total_filtered_rows, ROWS);
        assert_eq!(response.rows.len(), PAGE);
        assert_eq!(row_ids(&response)[0], offset);
        let expected = format!("row{offset:03}");
        assert_eq!(
            common::cell_text(&response.rows[0], &id_column),
            Some(expected.as_str())
        );
        assert_eq!(
            response.rows[0]
                .cells
                .get(&id_column)
                .and_then(|cell| cell.text.as_deref()),
            Some(format!("row{offset:03}").as_str())
        );
        seen.extend(row_ids(&response));
    }

    assert_eq!(
        project_full_reads(),
        0,
        "a normal page must not read all rows"
    );
    assert!(
        project_page_reads() >= 3,
        "each page move must use a page read"
    );
    assert_eq!(seen, (0..PAGE * 3).collect::<Vec<_>>());

    let projection = last_page_projection();
    assert!(projection.iter().any(|column| column == &id_column));
    // CSV cells are source strings and have no null/missing distinction, so
    // the page does not read the full `__original` copy.
    assert!(
        !projection.iter().any(|column| column == "__original"),
        "a CSV page must not carry the full original record: {projection:?}"
    );
    assert!(projection.iter().any(|column| column == "__rowid"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn json_page_reads_the_original_record_for_cell_kinds() {
    let _guard = page_test_guard();
    let root = std::env::temp_dir().join(format!("trivium-page-read-json-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let jsonl = root.join("page.jsonl");
    fs::write(
        &jsonl,
        "{\"id\":1,\"nullv\":null}\n{\"id\":2,\"later\":\"present\"}\n",
    )
    .expect("failed to write fixture");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: jsonl.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id;
    let ids = common::column_ids(&store, id);

    reset_project_read_counters();
    let response = query(&store, id, QuerySpec::default(), 0, PAGE);
    assert_eq!(response.rows.len(), 2);
    let projection = last_page_projection();
    assert!(
        projection.iter().any(|column| column == "__original"),
        "a JSON page needs the original record to classify null and missing: {projection:?}"
    );
    assert_eq!(
        response.rows[0]
            .cells
            .get(&common::id(&ids, "/nullv"))
            .map(|cell| cell.kind.as_str()),
        Some("null")
    );
    assert_eq!(
        response.rows[0]
            .cells
            .get(&common::id(&ids, "/later"))
            .map(|cell| cell.kind.as_str()),
        Some("missing")
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn hidden_columns_are_not_in_the_page_projection_or_rows() {
    let _guard = page_test_guard();
    let (root, store, id, ids) = setup_project();
    let id_column = common::id(&ids, "id");
    let hidden_column = common::id(&ids, "note");

    ops::flags::set_hidden_columns(
        &store,
        HiddenColumnsPayload {
            project_id: id,
            hidden_columns: vec![hidden_column.clone()],
        },
    )
    .expect("failed to hide column");

    reset_project_read_counters();
    let response = query(&store, id, QuerySpec::default(), 0, PAGE);
    assert_eq!(response.rows.len(), PAGE);
    assert_eq!(project_full_reads(), 0);
    assert!(project_page_reads() >= 1);

    let projection = last_page_projection();
    assert!(
        !projection.iter().any(|column| column == &hidden_column),
        "hidden column must not be read for a page: {projection:?}"
    );
    assert!(projection.iter().any(|column| column == &id_column));
    for row in &response.rows {
        assert!(!row.cells.contains_key(&hidden_column));
        assert!(!row.cells.contains_key(&hidden_column));
        assert!(row.cells.contains_key(&id_column));
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn scoped_search_resolves_with_only_the_search_columns() {
    let _guard = page_test_guard();
    let (root, store, id, ids) = setup_project();
    let id_column = common::id(&ids, "id");
    let note_column = common::id(&ids, "note");

    reset_project_read_counters();
    let response = query(
        &store,
        id,
        QuerySpec {
            search: Some("needle".to_string()),
            search_columns: Some(vec![note_column.clone()]),
            ..Default::default()
        },
        0,
        PAGE,
    );

    assert_eq!(response.total_filtered_rows, 3);
    assert_eq!(row_ids(&response), vec![7, 42, 77]);
    assert_eq!(
        common::cell_text(&response.rows[0], &id_column),
        Some("row007")
    );
    assert_eq!(
        project_full_reads(),
        0,
        "search must not read the full frame"
    );
    assert!(project_page_reads() >= 1);

    let resolve_projection = last_column_read_projection();
    assert!(
        resolve_projection
            .iter()
            .any(|column| column == &note_column),
        "search scope must be projected: {resolve_projection:?}"
    );
    assert!(
        !resolve_projection.iter().any(|column| column == &id_column),
        "a display-only column must not be read for resolution: {resolve_projection:?}"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn sorted_pages_keep_the_requested_order_without_a_full_frame() {
    let _guard = page_test_guard();
    let (root, store, id, ids) = setup_project();
    let id_column = common::id(&ids, "id");
    let spec = QuerySpec {
        sort_key: Some(id_column.clone()),
        sort_direction: Some("desc".to_string()),
        ..Default::default()
    };

    reset_project_read_counters();
    let first = query(&store, id, spec.clone(), 0, PAGE);
    let second = query(&store, id, spec, 30, PAGE);

    assert_eq!(
        row_ids(&first),
        (90..100).rev().collect::<Vec<_>>(),
        "descending id order"
    );
    assert_eq!(
        row_ids(&second),
        (60..70).rev().collect::<Vec<_>>(),
        "the later page keeps the same order"
    );
    assert_eq!(
        common::cell_text(&first.rows[0], &id_column),
        Some("row099")
    );
    assert_eq!(project_full_reads(), 0);
    assert!(project_page_reads() >= 2);

    let _ = fs::remove_dir_all(root);
}

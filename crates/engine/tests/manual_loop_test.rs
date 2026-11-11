//! T26: one cross-layer manual review loop on the T00 fixtures.
//!
//! The native Tauri app cannot be built in this environment, so this suite
//! pins the engine/IPC contract the real-app checklist depends on: exact
//! display text, hidden-column conditions, sorting and paging, selection bulk
//! annotation, selected export order, restart persistence, the empty
//! bookmark-only result, and the long-integer IPC shape.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::annotation::AnnotationPatch;
use trivium_engine::condition::{Condition, Node, Op};
use trivium_engine::ops;
use trivium_engine::ops::bulk::{bulk_preview, bulk_update, BulkAnnotatePayload};
use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::flags::{
    set_hidden_columns, update_flag, HiddenColumnsPayload, UpdateFlagPayload,
};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{
    query_project_rows, QueryRowsPayload, QueryRowsResponse, QuerySpec,
};
use trivium_engine::storage::close_flags_db;
use trivium_engine::store::ProjectsStore;

mod common;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/manual");

fn fixture(name: &str) -> PathBuf {
    Path::new(FIXTURES).join(name)
}

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-manual-loop-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn import(
    store: &ProjectsStore,
    name: &str,
) -> (Uuid, HashMap<String, String>) {
    let summary = ops::projects::create_project(
        store,
        CreateProjectPayload {
            path: fixture(name).to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to import fixture");
    let id = summary.meta.id;
    (id, common::column_ids(store, id))
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

fn query_all(store: &ProjectsStore, id: Uuid, spec: QuerySpec) -> QueryRowsResponse {
    query(store, id, spec, 0, 100)
}

fn row_ids(response: &QueryRowsResponse) -> Vec<usize> {
    response.rows.iter().map(|row| row.row_index).collect()
}

fn cell_text<'a>(row: &'a trivium_engine::models::ProjectRow, id: &str) -> Option<&'a str> {
    row.cells
        .get(id)
        .unwrap_or_else(|| panic!("missing cell for column {id}"))
        .text
        .as_deref()
}

fn cell_kind<'a>(row: &'a trivium_engine::models::ProjectRow, id: &str) -> &'a str {
    row.cells
        .get(id)
        .unwrap_or_else(|| panic!("missing cell for column {id}"))
        .kind
        .as_str()
}

fn read_csv(path: &Path) -> (Vec<String>, Vec<Vec<String>>) {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .expect("failed to open exported CSV");
    let headers = reader
        .headers()
        .expect("failed to read header")
        .iter()
        .map(str::to_string)
        .collect();
    let rows = reader
        .records()
        .map(|record| {
            record
                .expect("failed to read record")
                .iter()
                .map(str::to_string)
                .collect()
        })
        .collect();
    (headers, rows)
}

fn column_index(headers: &[String], name: &str) -> usize {
    headers
        .iter()
        .position(|header| header == name)
        .unwrap_or_else(|| panic!("column not found: {name}; headers={headers:?}"))
}

#[test]
fn csv_display_and_export_keep_the_t00_source_strings() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let (id, ids) = import(&store, "t00_source.csv");

    let response = query_all(&store, id, QuerySpec::default());
    assert_eq!(response.total_rows, 3);
    assert_eq!(row_ids(&response), vec![0, 1, 2]);

    let expected = [
        (0usize, "flag", Some("old-user-note")),
        (0, "memo", Some("original memo")),
        (0, "__rowid", Some("7")),
        (0, "trivium-memo", Some("old trivium memo")),
        (0, "zip", Some("00123")),
        (0, "comma", Some("a,b")),
        (0, "quote", Some("say \"hi\"")),
        (0, "multiline", Some("line1\nline2")),
        (0, "bigint", Some("9007199254740993")),
        (0, "spaced", Some("  padded  ")),
        (1, "zip", Some("00000")),
        (1, "bigint", Some("9223372036854775807")),
        (2, "zip", Some("00001")),
        (2, "bigint", Some("18446744073709551615")),
        (2, "spaced", Some("")),
    ];
    for (row_index, source, text) in expected {
        let row = &response.rows[row_index];
        assert_eq!(
            cell_text(row, &common::id(&ids, source)),
            text,
            "display text changed for row {row_index} column {source}"
        );
    }

    // The display value and the exported value must be the same source text.
    let destination = root.join("t00-all.csv");
    let result = export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: Some(false),
        },
    )
    .expect("export failed");
    assert_eq!(result.rows, 3);
    let (headers, exported) = read_csv(&destination);
    for (position, row) in response.rows.iter().enumerate() {
        for source in [
            "flag",
            "memo",
            "__rowid",
            "trivium-memo",
            "zip",
            "comma",
            "quote",
            "multiline",
            "bigint",
            "spaced",
        ] {
            let column = column_index(&headers, source);
            let displayed = cell_text(row, &common::id(&ids, source)).unwrap_or("");
            assert_eq!(
                exported[position][column], displayed,
                "export differs from display for row {position} column {source}"
            );
        }
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn json_and_jsonl_display_the_same_exact_values() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");

    let (jsonl_id, jsonl_ids) = import(&store, "t00_source.jsonl");
    let jsonl = query_all(&store, jsonl_id, QuerySpec::default());
    assert_eq!(jsonl.total_rows, 3);
    assert_eq!(
        cell_text(&jsonl.rows[0], &common::id(&jsonl_ids, "/big")),
        Some("9223372036854775807")
    );
    assert_eq!(
        cell_text(&jsonl.rows[0], &common::id(&jsonl_ids, "/u64")),
        Some("18446744073709551615")
    );
    assert_eq!(
        cell_text(&jsonl.rows[1], &common::id(&jsonl_ids, "/big")),
        Some("9007199254740993")
    );
    assert_eq!(
        cell_text(&jsonl.rows[2], &common::id(&jsonl_ids, "/big")),
        Some("-9223372036854775808")
    );
    assert_eq!(
        cell_text(&jsonl.rows[0], &common::id(&jsonl_ids, "/arr")),
        Some("[1,2,3]")
    );
    assert_eq!(
        cell_text(&jsonl.rows[0], &common::id(&jsonl_ids, "/a.b")),
        Some("literal-dot")
    );
    assert_eq!(
        cell_text(&jsonl.rows[0], &common::id(&jsonl_ids, "/a/b")),
        Some("nested-b")
    );
    // Null, an absent key, and an empty string stay three distinct values.
    assert_eq!(
        cell_text(&jsonl.rows[0], &common::id(&jsonl_ids, "/nullv")),
        None
    );
    assert_eq!(
        cell_kind(&jsonl.rows[0], &common::id(&jsonl_ids, "/nullv")),
        "null"
    );
    assert_eq!(
        cell_kind(&jsonl.rows[0], &common::id(&jsonl_ids, "/only_later")),
        "missing"
    );
    assert_eq!(
        cell_text(&jsonl.rows[0], &common::id(&jsonl_ids, "/empty")),
        Some("")
    );

    let (json_id, json_ids) = import(&store, "t00_source.json");
    let json = query_all(&store, json_id, QuerySpec::default());
    assert_eq!(json.total_rows, 3);
    assert_eq!(
        cell_text(&json.rows[1], &common::id(&json_ids, "/big")),
        Some("9007199254740993")
    );
    assert_eq!(
        cell_text(&json.rows[0], &common::id(&json_ids, "/nullv")),
        None
    );
    assert_eq!(
        cell_text(&json.rows[1], &common::id(&json_ids, "/only_later")),
        Some("second-only")
    );
    assert_eq!(
        cell_kind(&json.rows[2], &common::id(&json_ids, "/only_later")),
        "missing"
    );
    assert_eq!(
        cell_text(&json.rows[2], &common::id(&json_ids, "/big")),
        Some("-9223372036854775808")
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn hidden_column_condition_and_search_match_without_returning_the_column() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let (id, ids) = import(&store, "t00_source.csv");
    let comma = common::id(&ids, "comma");

    set_hidden_columns(
        &store,
        HiddenColumnsPayload {
            project_id: id,
            hidden_columns: vec![comma.clone()],
        },
    )
    .expect("failed to hide comma column");

    let by_condition = query_all(
        &store,
        id,
        QuerySpec {
            condition: Some(Node::Condition(Condition {
                column: comma.clone(),
                op: Op::Eq,
                values: vec!["a,b".to_string()],
                negated: false,
            })),
            ..Default::default()
        },
    );
    assert_eq!(by_condition.total_filtered_rows, 1);
    assert_eq!(row_ids(&by_condition), vec![0]);

    let by_search = query_all(
        &store,
        id,
        QuerySpec {
            search: Some("p,q".to_string()),
            search_columns: Some(vec![comma.clone()]),
            ..Default::default()
        },
    );
    assert_eq!(by_search.total_filtered_rows, 1);
    assert_eq!(row_ids(&by_search), vec![2]);

    let all_rows = query_all(&store, id, QuerySpec::default());
    assert_eq!(all_rows.total_rows, 3);
    for response in [&by_condition, &by_search, &all_rows] {
        for row in &response.rows {
            assert!(
                !row.data.contains_key(&comma),
                "hidden column leaked into row data"
            );
            assert!(
                !row.cells.contains_key(&comma),
                "hidden column leaked into row cells"
            );
        }
    }
    // Inspect the actual page JSON: `data` is no longer serialized, and the
    // hidden column must be absent from the cells map too.
    let ipc = serde_json::to_value(&all_rows).expect("page response must serialize");
    assert!(ipc["rows"][0].get("data").is_none());
    assert!(ipc["rows"][0]["cells"].get(&comma).is_none());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn sorted_pages_selection_bulk_and_selected_export_agree() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let (id, ids) = import(&store, "t00_source.csv");
    let zip = common::id(&ids, "zip");
    let sorted = QuerySpec {
        sort_key: Some(zip.clone()),
        sort_direction: Some("asc".to_string()),
        ..Default::default()
    };

    let first_page = query(&store, id, sorted.clone(), 0, 2);
    let second_page = query(&store, id, sorted.clone(), 2, 2);
    assert_eq!(row_ids(&first_page), vec![1, 2]);
    assert_eq!(row_ids(&second_page), vec![0]);
    assert_eq!(first_page.total_filtered_rows, 3);
    assert_eq!(second_page.total_filtered_rows, 3);

    let selection = vec![0usize, 2];
    let payload = BulkAnnotatePayload {
        project_id: id,
        target: "selection".to_string(),
        row_indices: Some(selection.clone()),
        spec: QuerySpec::default(),
        patch: AnnotationPatch {
            flag: Some("critical".to_string()),
            add_tags: vec!["reviewed".to_string()],
            ..AnnotationPatch::default()
        },
        preview_token: None,
    };
    let preview = bulk_preview(&store, payload.clone()).expect("bulk preview failed");
    assert_eq!(preview.count, 2);
    let applied = bulk_update(
        &store,
        BulkAnnotatePayload {
            preview_token: Some(preview.preview_token),
            ..payload
        },
    )
    .expect("bulk update failed");
    assert_eq!(applied.updated, 2);

    let after = query_all(&store, id, sorted.clone());
    assert_eq!(row_ids(&after), vec![1, 2, 0]);
    let by_id: HashMap<usize, &trivium_engine::models::ProjectRow> =
        after.rows.iter().map(|row| (row.row_index, row)).collect();
    assert_eq!(by_id[&0].flag, "critical");
    assert_eq!(by_id[&2].flag, "critical");
    assert_eq!(by_id[&1].flag, "");
    assert_eq!(by_id[&0].tags, vec!["reviewed".to_string()]);
    assert_eq!(by_id[&2].tags, vec!["reviewed".to_string()]);
    assert!(by_id[&1].tags.is_empty());

    let destination = root.join("selected.csv");
    let result = export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("selected".to_string()),
            spec: sorted,
            columns: Some(vec![zip]),
            row_indices: Some(selection),
            include_annotations: Some(true),
        },
    )
    .expect("selected export failed");
    assert_eq!(result.rows, 2);

    let (headers, exported) = read_csv(&destination);
    let zip_column = column_index(&headers, "zip");
    let critical_column = column_index(&headers, "trivium-critical");
    let exported_zips: Vec<&str> = exported.iter().map(|row| row[zip_column].as_str()).collect();
    assert_eq!(
        exported_zips,
        vec!["00001", "00123"],
        "selected export must follow the query order"
    );
    assert!(exported.iter().all(|row| row[critical_column] == "1"));
    let tags_column = column_index(&headers, "trivium-tags");
    assert!(exported.iter().all(|row| row[tags_column] == "reviewed"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn annotations_survive_reopen_and_bookmark_only_is_empty_at_zero() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let (id, _ids) = import(&store, "t00_source.jsonl");

    let empty = query_all(
        &store,
        id,
        QuerySpec {
            bookmarked_only: Some(true),
            ..Default::default()
        },
    );
    assert_eq!(empty.total_filtered_rows, 0);
    assert!(empty.rows.is_empty());

    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 2,
            flag: Some("suspicious".to_string()),
            memo: Some(Some("line1\nline2".to_string())),
            tags: Some(vec!["pc".to_string(), "triage".to_string()]),
            bookmarked: Some(true),
        },
    )
    .expect("annotation failed");

    let project_dir = store.project_dir(&id);
    close_flags_db(&project_dir);
    drop(store);

    let reopened = ProjectsStore::new(root.clone()).expect("failed to reopen store");
    let restored = query_all(&reopened, id, QuerySpec::default());
    let row = restored
        .rows
        .iter()
        .find(|row| row.row_index == 2)
        .expect("row 2 missing after reopen");
    assert_eq!(row.flag, "suspicious");
    assert_eq!(row.memo.as_deref(), Some("line1\nline2"));
    assert_eq!(row.tags, vec!["pc".to_string(), "triage".to_string()]);
    assert!(row.bookmarked);
    assert_eq!(restored.annotated_total, 1);

    let bookmarked = query_all(
        &reopened,
        id,
        QuerySpec {
            bookmarked_only: Some(true),
            ..Default::default()
        },
    );
    assert_eq!(bookmarked.total_filtered_rows, 1);
    assert_eq!(row_ids(&bookmarked), vec![2]);

    // Clearing only the bookmark must keep the other annotation fields.
    update_flag(
        &reopened,
        UpdateFlagPayload {
            project_id: id,
            row_index: 2,
            flag: None,
            memo: None,
            tags: None,
            bookmarked: Some(false),
        },
    )
    .expect("bookmark clear failed");
    let cleared = query_all(
        &reopened,
        id,
        QuerySpec {
            bookmarked_only: Some(true),
            ..Default::default()
        },
    );
    assert_eq!(cleared.total_filtered_rows, 0);
    assert!(cleared.rows.is_empty());
    let kept = query_all(&reopened, id, QuerySpec::default());
    let row = kept
        .rows
        .iter()
        .find(|row| row.row_index == 2)
        .expect("row 2 missing");
    assert_eq!(row.flag, "suspicious");
    assert_eq!(row.tags, vec!["pc".to_string(), "triage".to_string()]);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn ipc_json_sends_long_integers_as_plain_decimal_strings() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let (csv_id, csv_ids) = import(&store, "t00_source.csv");
    let response = query_all(&store, csv_id, QuerySpec::default());
    let value = serde_json::to_value(&response).expect("response must serialize");
    let rows = value["rows"].as_array().expect("rows array");

    for (row_index, source, expected) in [
        (0usize, "bigint", "9007199254740993"),
        (1, "bigint", "9223372036854775807"),
        (2, "bigint", "18446744073709551615"),
    ] {
        let text = &rows[row_index]["cells"][common::id(&csv_ids, source)]["text"];
        assert!(
            text.is_string(),
            "IPC cell text must be a string, got {text}"
        );
        assert_eq!(text.as_str(), Some(expected));
    }

    let (jsonl_id, jsonl_ids) = import(&store, "t00_source.jsonl");
    let jsonl = query_all(&store, jsonl_id, QuerySpec::default());
    let value = serde_json::to_value(&jsonl).expect("response must serialize");
    let rows = value["rows"].as_array().expect("rows array");
    assert_eq!(
        rows[0]["cells"][common::id(&jsonl_ids, "/u64")]["text"].as_str(),
        Some("18446744073709551615")
    );
    assert_eq!(
        rows[0]["cells"][common::id(&jsonl_ids, "/big")]["text"].as_str(),
        Some("9223372036854775807")
    );

    let _ = fs::remove_dir_all(root);
}

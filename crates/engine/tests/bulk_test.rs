//! Bulk annotation over selected, filtered, and all rows, with one-level undo.

use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::bulk::{
    bulk_preview, bulk_undo, bulk_update, BulkAnnotatePayload, BulkUndoPayload,
};
use trivium_engine::ops::iocs::SaveIocsPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QueryRowsResponse};
use trivium_engine::store::ProjectsStore;

/// The undo layer is process-global, so tests sharing this binary must not
/// interleave their bulk_update/bulk_undo pairs.
static UNDO_LOCK: Mutex<()> = Mutex::new(());

fn undo_guard() -> MutexGuard<'static, ()> {
    UNDO_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// 12 rows: the first 6 are alice, the last 6 are bob.
// EventID is spread evenly: 4624 / 4625 / 4688 four rows each.
fn write_fixture(root: &std::path::Path) -> PathBuf {
    let path = root.join("bulk.csv");
    let body = "ts,EventID,host,user,proc\n\
2024-01-01T00:00:01,4688,WS01,alice,powershell\n\
2024-01-01T00:00:02,4624,WS02,alice,explorer\n\
2024-01-01T00:00:03,4625,WS03,alice,cmd\n\
2024-01-01T00:00:04,4624,WS04,alice,explorer\n\
2024-01-01T00:00:05,4625,WS05,alice,cmd\n\
2024-01-01T00:00:06,4688,WS06,alice,powershell\n\
2024-01-01T00:00:07,4624,WS07,bob,explorer\n\
2024-01-01T00:00:08,4625,WS08,bob,cmd\n\
2024-01-01T00:00:09,4688,WS09,bob,powershell\n\
2024-01-01T00:00:10,4624,WS10,bob,explorer\n\
2024-01-01T00:00:11,4625,WS11,bob,cmd\n\
2024-01-01T00:00:12,4688,WS12,bob,powershell\n";
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn setup(with_ioc: bool) -> (PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-bulk-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_fixture(&root);
    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
        },
    )
    .expect("failed to create project");
    let id = summary.meta.id;

    if with_ioc {
        ops::iocs::save_iocs(
            &store,
            SaveIocsPayload {
                project_id: id,
                entries: vec![IocEntry {
                    flag: "critical".to_string(),
                    tag: "logon-fail".to_string(),
                    query: "eventid:4625".to_string(),
                }],
            },
        )
        .expect("failed to save iocs");
    }

    (root, store, id)
}

fn payload(
    id: Uuid,
    target: &str,
    search: Option<&str>,
    row_indices: Option<Vec<usize>>,
    flag: &str,
    memo: Option<&str>,
) -> BulkAnnotatePayload {
    BulkAnnotatePayload {
        project_id: id,
        target: target.to_string(),
        row_indices,
        search: search.map(|value| value.to_string()),
        columns: None,
        flag_filter: None,
        condition: None,
        flag: Some(flag.to_string()),
        memo: memo.map(|value| Some(value.to_string())),
        tags: None,
        bookmarked: None,
    }
}

fn query(store: &ProjectsStore, id: Uuid, search: Option<&str>) -> QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            search: search.map(|value| value.to_string()),
            search_columns: None,
            flag_filter: None,
            offset: None,
            limit: None,
            sort_key: None,
            sort_direction: None,
            condition: None,
            bookmarked_only: None,
        },
    )
    .expect("query failed")
}

/// Per-row (flag, memo), indexed by row_index.
fn query_flags(store: &ProjectsStore, id: Uuid) -> Vec<(String, Option<String>)> {
    let response = query(store, id, None);
    let mut by_index: Vec<(String, Option<String>)> =
        vec![(String::new(), None); response.total_rows];
    for row in &response.rows {
        by_index[row.row_index] = (row.flag.clone(), row.memo.clone());
    }
    by_index
}

fn flagged(store: &ProjectsStore, id: Uuid) -> usize {
    store.find(&id).expect("meta missing").flagged_records
}

fn undo(store: &ProjectsStore, id: Uuid) -> usize {
    bulk_undo(store, BulkUndoPayload { project_id: id })
        .expect("undo failed")
        .updated
}

#[test]
fn bulk_all_applies_and_undo_restores() {
    let _guard = undo_guard();
    let (root, store, id) = setup(false);

    let preview = bulk_preview(
        &store,
        payload(id, "all", None, None, "suspicious", Some("bulk")),
    )
    .expect("preview failed");
    assert_eq!(preview.count, 12);

    let applied = bulk_update(
        &store,
        payload(id, "all", None, None, "suspicious", Some("bulk")),
    )
    .expect("bulk update failed");
    assert_eq!(applied.updated, 12);

    let rows = query_flags(&store, id);
    assert_eq!(rows.len(), 12);
    for (index, (flag, memo)) in rows.iter().enumerate() {
        assert_eq!(flag, "suspicious", "row {index}");
        assert_eq!(memo.as_deref(), Some("bulk"), "row {index}");
    }
    assert_eq!(flagged(&store, id), 12);

    assert_eq!(undo(&store, id), 12);
    for (index, (flag, memo)) in query_flags(&store, id).iter().enumerate() {
        assert_eq!(flag, "", "row {index}");
        assert_eq!(memo, &None, "row {index}");
    }
    assert_eq!(flagged(&store, id), 0);

    let second =
        bulk_undo(&store, BulkUndoPayload { project_id: id }).expect_err("second undo must fail");
    assert!(
        second.to_string().contains("No bulk operation to undo."),
        "unexpected error: {second}"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_filter_matches_the_query_path() {
    let _guard = undo_guard();
    let (root, store, id) = setup(false);

    let preview = bulk_preview(
        &store,
        payload(id, "filter", Some("user:alice"), None, "suspicious", None),
    )
    .expect("preview failed");
    assert_eq!(preview.count, 6);
    assert_eq!(
        preview.count,
        query(&store, id, Some("user:alice")).total_filtered_rows,
        "preview must agree with the query path"
    );

    let applied = bulk_update(
        &store,
        payload(id, "filter", Some("user:alice"), None, "suspicious", None),
    )
    .expect("bulk update failed");
    assert_eq!(applied.updated, 6);

    let rows = query_flags(&store, id);
    for (index, (flag, _)) in rows.iter().enumerate() {
        let expected = if index < 6 { "suspicious" } else { "" };
        assert_eq!(flag, expected, "row {index}");
    }
    assert_eq!(flagged(&store, id), 6);

    assert_eq!(undo(&store, id), 6);
    for (index, (flag, _)) in query_flags(&store, id).iter().enumerate() {
        assert_eq!(flag, "", "row {index}");
    }
    assert_eq!(flagged(&store, id), 0);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_selection_dedupes_and_rejects_out_of_range() {
    let _guard = undo_guard();
    let (root, store, id) = setup(false);

    let preview = bulk_preview(
        &store,
        payload(id, "selection", None, Some(vec![1, 1, 4]), "safe", None),
    )
    .expect("preview failed");
    assert_eq!(preview.count, 2);

    let applied = bulk_update(
        &store,
        payload(id, "selection", None, Some(vec![1, 1, 4]), "safe", None),
    )
    .expect("bulk update failed");
    assert_eq!(applied.updated, 2);

    let rows = query_flags(&store, id);
    assert_eq!(rows[1].0, "safe");
    assert_eq!(rows[4].0, "safe");
    for index in [0usize, 2, 3, 5] {
        assert_eq!(rows[index].0, "", "row {index}");
    }

    assert_eq!(undo(&store, id), 2);

    let error = bulk_update(
        &store,
        payload(id, "selection", None, Some(vec![1, 99]), "safe", None),
    )
    .expect_err("out-of-range selection must fail");
    assert!(
        error.to_string().contains("Row index out of range: 99."),
        "unexpected error: {error}"
    );

    let before = flagged(&store, id);
    let preview_all =
        bulk_preview(&store, payload(id, "all", None, None, "safe", None)).expect("preview failed");
    assert_eq!(preview_all.count, 12);
    assert_eq!(flagged(&store, id), before, "preview must not change state");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_update_preserves_ioc_outcomes_and_undo_restores_them() {
    let _guard = undo_guard();
    let (root, store, id) = setup(true);

    let initial = query_flags(&store, id);
    for index in [2usize, 4, 7, 10] {
        assert_eq!(initial[index].0, "critical", "row {index}");
        assert!(
            initial[index]
                .1
                .as_deref()
                .unwrap_or("")
                .contains("[logon-fail]"),
            "row {index}"
        );
    }

    let applied = bulk_update(&store, payload(id, "all", None, None, "safe", None))
        .expect("bulk update failed");
    assert_eq!(applied.updated, 12);

    let rows = query_flags(&store, id);
    for (index, (flag, memo)) in rows.iter().enumerate() {
        assert_eq!(flag, "safe", "user flag must win on row {index}");
        if [2usize, 4, 7, 10].contains(&index) {
            assert!(
                memo.as_deref().unwrap_or("").contains("[logon-fail]"),
                "row {index} keeps the IOC tag"
            );
        }
    }
    // The bulk path does not recalculate IOC counters.
    assert_eq!(
        store.find(&id).expect("meta missing").ioc_applied_records,
        4,
        "bulk_update must not recalculate ioc_applied_records"
    );

    assert_eq!(undo(&store, id), 12);
    let restored = query_flags(&store, id);
    for (index, (flag, _)) in restored.iter().enumerate() {
        let expected = if [2usize, 4, 7, 10].contains(&index) {
            "critical"
        } else {
            ""
        };
        assert_eq!(flag, expected, "row {index}");
    }
    assert_eq!(flagged(&store, id), 0);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_clear_removes_entries_and_reveals_ioc_flags() {
    let _guard = undo_guard();
    let (root, store, id) = setup(true);

    bulk_update(&store, payload(id, "all", None, None, "suspicious", None))
        .expect("bulk update failed");
    assert_eq!(flagged(&store, id), 12);

    let cleared =
        bulk_update(&store, payload(id, "all", None, None, "", None)).expect("bulk clear failed");
    assert_eq!(cleared.updated, 12);
    assert_eq!(flagged(&store, id), 0);

    let rows = query_flags(&store, id);
    for (index, (flag, _)) in rows.iter().enumerate() {
        let expected = if [2usize, 4, 7, 10].contains(&index) {
            "critical"
        } else {
            ""
        };
        assert_eq!(flag, expected, "row {index}");
    }

    let _ = fs::remove_dir_all(root);
}

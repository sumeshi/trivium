//! Bulk annotation over selected, filtered, and all rows, with one-level undo.

use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use trivium_engine::annotation::AnnotationPatch;
use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::bulk::{
    bulk_preview, bulk_undo, bulk_update, BulkAnnotatePayload, BulkUndoPayload,
};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::iocs::SaveIocsPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QueryRowsResponse, QuerySpec};
use trivium_engine::storage::load_flags;
use trivium_engine::store::ProjectsStore;

mod common;

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

fn setup(with_ioc: bool) -> (PathBuf, ProjectsStore, Uuid, std::collections::HashMap<String, String>) {
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
            restore_annotations: None,
        },
    )
    .expect("failed to create project");
    let id = summary.meta.id;
    let ids = common::column_ids(&store, id);

    if with_ioc {
        ops::iocs::save_iocs(
            &store,
            SaveIocsPayload {
                project_id: id,
                entries: vec![IocEntry {
                    flag: "critical".to_string(),
                    tag: "logon-fail".to_string(),
                    query: common::search(&ids, "EventID", "4625"),
                }],
            },
        )
        .expect("failed to save iocs");
    }

    (root, store, id, ids)
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
        row_indices: row_indices,
        preview_token: None,
        spec: QuerySpec {
            search: search.map(|value| value.to_string()),
            search_columns: None,
            flag_filter: None,
            condition: None,
            ..Default::default()
        },
        patch: AnnotationPatch {
            flag: Some(flag.to_string()),
            memo: memo.map(|value| Some(value.to_string())),
            tags: None,
            bookmarked: None,
            ..Default::default()
        },
    }
}

fn previewed(store: &ProjectsStore, payload: BulkAnnotatePayload) -> BulkAnnotatePayload {
    let preview = bulk_preview(store, payload.clone()).expect("preview failed");
    let mut payload = payload;
    payload.preview_token = Some(preview.preview_token);
    payload
}

fn query(store: &ProjectsStore, id: Uuid, search: Option<&str>) -> QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: search.map(|value| value.to_string()),
                search_columns: None,
                flag_filter: None,
                sort_key: None,
                sort_direction: None,
                condition: None,
                bookmarked_only: None,
                ..Default::default()
            },
            offset: None,
            limit: None,
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

fn update_single(
    store: &ProjectsStore,
    id: Uuid,
    row_index: usize,
    flag: Option<&str>,
    memo: Option<Option<&str>>,
    tags: Option<Vec<&str>>,
    bookmarked: Option<bool>,
) {
    update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index,
            flag: flag.map(str::to_string),
            memo: memo.map(|value| value.map(str::to_string)),
            tags: tags.map(|values| values.into_iter().map(str::to_string).collect()),
            bookmarked,
        },
    )
    .expect("single update failed");
}

type Annotations = (String, Option<String>, Vec<String>, bool);

/// Per-row (flag, memo, tags, bookmark), indexed by row_index.
fn annotations(store: &ProjectsStore, id: Uuid) -> Vec<Annotations> {
    let response = query(store, id, None);
    let mut by_index: Vec<Annotations> =
        vec![(String::new(), None, Vec::new(), false); response.total_rows];
    for row in &response.rows {
        by_index[row.row_index] = (
            row.flag.clone(),
            row.memo.clone(),
            row.tags.clone(),
            row.bookmarked,
        );
    }
    by_index
}

/// Counts flagged rows directly from the annotation database.
fn flagged_from_db(store: &ProjectsStore, id: Uuid) -> usize {
    load_flags(&store.project_dir(&id).join("flags.json"))
        .expect("failed to load flags")
        .values()
        .filter(|entry| !entry.flag.trim().is_empty())
        .count()
}

#[test]
fn bulk_all_applies_and_undo_restores() {
    let _guard = undo_guard();
    let (root, store, id, _ids) = setup(false);

    let preview = bulk_preview(
        &store,
        payload(id, "all", None, None, "suspicious", Some("bulk")),
    )
    .expect("preview failed");
    assert_eq!(preview.count, 12);

    let applied = bulk_update(
        &store,
        previewed(&store, payload(id, "all", None, None, "suspicious", Some("bulk"))),
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
    let (root, store, id, ids) = setup(false);

    let preview = bulk_preview(
        &store,
        payload(id, "filtered", Some(&common::search(&ids, "user", "alice")), None, "suspicious", None),
    )
    .expect("preview failed");
    assert_eq!(preview.count, 6);
    assert_eq!(
        preview.count,
        query(&store, id, Some(&common::search(&ids, "user", "alice"))).total_filtered_rows,
        "preview must agree with the query path"
    );

    let applied = bulk_update(
        &store,
        previewed(&store, payload(id, "filtered", Some(&common::search(&ids, "user", "alice")), None, "suspicious", None)),
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
    let (root, store, id, _ids) = setup(false);

    let preview = bulk_preview(
        &store,
        payload(id, "selection", None, Some(vec![1, 1, 4]), "safe", None),
    )
    .expect("preview failed");
    assert_eq!(preview.count, 2);

    let applied = bulk_update(
        &store,
        previewed(&store, payload(id, "selection", None, Some(vec![1, 1, 4]), "safe", None)),
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
fn bulk_update_ignores_legacy_iocs_and_undo_restores_user_flags() {
    let _guard = undo_guard();
    let (root, store, id, _ids) = setup(true);

    // A saved IOC definition does not flag or tag any row by itself.
    let initial = query_flags(&store, id);
    for (index, (flag, memo)) in initial.iter().enumerate() {
        assert_eq!(flag, "", "row {index}");
        assert_eq!(memo, &None, "row {index}");
    }

    let applied = bulk_update(
        &store,
        previewed(&store, payload(id, "all", None, None, "safe", None)),
    )
    .expect("bulk update failed");
    assert_eq!(applied.updated, 12);

    let rows = query_flags(&store, id);
    for (index, (flag, memo)) in rows.iter().enumerate() {
        assert_eq!(flag, "safe", "user flag must apply on row {index}");
        assert_eq!(memo, &None, "row {index} must not gain an IOC tag");
    }
    assert_eq!(
        store.find(&id).expect("meta missing").ioc_applied_records,
        0,
        "bulk_update must not recalculate ioc_applied_records"
    );

    assert_eq!(undo(&store, id), 12);
    let restored = query_flags(&store, id);
    for (index, (flag, _)) in restored.iter().enumerate() {
        assert_eq!(flag, "", "row {index}");
    }
    assert_eq!(flagged(&store, id), 0);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_clear_does_not_reveal_legacy_ioc_flags() {
    let _guard = undo_guard();
    let (root, store, id, _ids) = setup(true);

    bulk_update(
        &store,
        previewed(&store, payload(id, "all", None, None, "suspicious", None)),
    )
    .expect("bulk update failed");
    assert_eq!(flagged(&store, id), 12);

    let cleared = bulk_update(
        &store,
        previewed(&store, payload(id, "all", None, None, "", None)),
    )
    .expect("bulk clear failed");
    assert_eq!(cleared.updated, 12);
    assert_eq!(flagged(&store, id), 0);

    let rows = query_flags(&store, id);
    for (index, (flag, _)) in rows.iter().enumerate() {
        assert_eq!(flag, "", "row {index}");
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_clear_undo_restores_memo_tags_and_bookmark() {
    let _guard = undo_guard();
    let (root, store, id, _ids) = setup(false);

    update_single(
        &store,
        id,
        0,
        Some("suspicious"),
        Some(Some("note")),
        Some(vec!["pc", "gpu"]),
        Some(true),
    );
    update_single(
        &store,
        id,
        1,
        Some("critical"),
        Some(Some("second")),
        Some(vec!["server"]),
        Some(false),
    );

    bulk_update(
        &store,
        previewed(&store, payload(id, "all", None, None, "", None)),
    )
    .expect("bulk clear failed");
    // Clearing the flag keeps the other fields of each entry.
    let cleared = annotations(&store, id);
    assert_eq!(cleared[0].0, "");
    assert_eq!(cleared[0].1.as_deref(), Some("note"));
    assert_eq!(cleared[0].2, vec!["pc".to_string(), "gpu".to_string()]);
    assert!(cleared[0].3);
    assert_eq!(flagged(&store, id), 0);
    assert_eq!(flagged_from_db(&store, id), 0);

    assert_eq!(undo(&store, id), 12);

    let restored = annotations(&store, id);
    assert_eq!(restored[0].0, "suspicious");
    assert_eq!(restored[0].1.as_deref(), Some("note"));
    assert_eq!(restored[0].2, vec!["pc".to_string(), "gpu".to_string()]);
    assert!(restored[0].3);
    assert_eq!(restored[1].0, "critical");
    assert_eq!(restored[1].1.as_deref(), Some("second"));
    assert_eq!(restored[1].2, vec!["server".to_string()]);
    assert!(!restored[1].3);
    for row in &restored[2..] {
        assert_eq!(row.0, "");
        assert_eq!(row.1, None);
        assert!(row.2.is_empty());
        assert!(!row.3);
    }
    assert_eq!(flagged(&store, id), 2);
    assert_eq!(flagged_from_db(&store, id), 2);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn undo_is_independent_per_project() {
    let _guard = undo_guard();
    let (root_a, store_a, id_a, _ids_a) = setup(false);
    let (root_b, store_b, id_b, _ids_b) = setup(false);

    bulk_update(
        &store_a,
        previewed(&store_a, payload(id_a, "all", None, None, "safe", None)),
    )
    .expect("bulk update A failed");
    bulk_update(
        &store_b,
        previewed(&store_b, payload(id_b, "all", None, None, "suspicious", None)),
    )
    .expect("bulk update B failed");

    // Undoing A must not remove or consume B's undo entry.
    assert_eq!(undo(&store_a, id_a), 12);
    for (index, (flag, _)) in query_flags(&store_a, id_a).iter().enumerate() {
        assert_eq!(flag, "", "A row {index}");
    }
    for (index, (flag, _)) in query_flags(&store_b, id_b).iter().enumerate() {
        assert_eq!(flag, "suspicious", "B row {index}");
    }

    assert_eq!(undo(&store_b, id_b), 12);
    for (index, (flag, _)) in query_flags(&store_b, id_b).iter().enumerate() {
        assert_eq!(flag, "", "B row {index}");
    }

    let _ = fs::remove_dir_all(root_a);
    let _ = fs::remove_dir_all(root_b);
}

#[test]
fn undo_refuses_to_overwrite_a_later_update() {
    let _guard = undo_guard();
    let (root, store, id, _ids) = setup(false);

    bulk_update(
        &store,
        previewed(&store, payload(id, "all", None, None, "safe", None)),
    )
    .expect("bulk update failed");

    update_single(
        &store,
        id,
        3,
        Some("critical"),
        Some(Some("after bulk")),
        Some(vec!["manual"]),
        Some(true),
    );
    let before = annotations(&store, id);

    let error = bulk_undo(&store, BulkUndoPayload { project_id: id })
        .expect_err("undo must not overwrite a later update");
    assert!(
        error
            .to_string()
            .contains("Annotation changed since the bulk operation; undo would overwrite it."),
        "unexpected error: {error}"
    );
    assert_eq!(annotations(&store, id), before, "failed undo must not write");
    assert_eq!(flagged_from_db(&store, id), 12);

    // The conflict leaves the undo entry in place, so a retry reports the same
    // conflict instead of silently applying the old snapshot.
    let retry = bulk_undo(&store, BulkUndoPayload { project_id: id })
        .expect_err("undo entry must remain after a conflict");
    assert!(
        retry
            .to_string()
            .contains("Annotation changed since the bulk operation; undo would overwrite it."),
        "unexpected error: {retry}"
    );
    assert_eq!(annotations(&store, id), before, "retry must not write");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_and_undo_update_flagged_counts_by_difference() {
    let _guard = undo_guard();
    let (root, store, id, ids) = setup(false);

    update_single(&store, id, 0, Some("safe"), None, None, None);
    update_single(&store, id, 1, None, None, None, Some(true));
    update_single(&store, id, 2, Some("suspicious"), None, None, None);
    assert_eq!(flagged(&store, id), 2);
    assert_eq!(flagged_from_db(&store, id), 2);

    let applied = bulk_update(
        &store,
        previewed(
            &store,
            payload(
                id,
                "filtered",
                Some(&common::search(&ids, "user", "alice")),
                None,
                "safe",
                None,
            ),
        ),
    )
    .expect("bulk update failed");
    assert_eq!(applied.updated, 6);

    // Rows 0-5 are now flagged; the bookmark-only row 1 gained a flag, and
    // rows 6-11 were outside the target.
    assert_eq!(flagged(&store, id), 6);
    assert_eq!(flagged(&store, id), flagged_from_db(&store, id));

    assert_eq!(undo(&store, id), 6);
    assert_eq!(flagged(&store, id), 2);
    assert_eq!(flagged(&store, id), flagged_from_db(&store, id));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn failed_preview_token_leaves_annotations_unchanged() {
    let _guard = undo_guard();
    let (root, store, id, _ids) = setup(false);

    let base = payload(id, "selection", None, Some(vec![0, 1]), "safe", None);
    let preview = bulk_preview(&store, base.clone()).expect("preview failed");
    let mut stale = base;
    stale.preview_token = Some(preview.preview_token);

    // The preview counted zero annotated rows; flagging row 1 changes the
    // annotated count and therefore invalidates the token.
    update_single(&store, id, 1, Some("safe"), None, None, None);
    let before = annotations(&store, id);

    let error = bulk_update(&store, stale).expect_err("stale token must fail");
    assert!(
        error.to_string().contains("Target changed; preview again."),
        "unexpected error: {error}"
    );
    assert_eq!(annotations(&store, id), before, "failed batch must not write");

    let _ = fs::remove_dir_all(root);
}

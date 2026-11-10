//! F19: user tags and bookmarks across storage, update_flag, bulk, query,
//! export, and the legacy flags.json migration path.

use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use trivium_engine::models::FlagEntry;
use trivium_engine::ops;
use trivium_engine::ops::bulk::{bulk_undo, bulk_update, BulkAnnotatePayload, BulkUndoPayload};
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::group::GroupTreePayload;
use trivium_engine::ops::histogram::HistogramPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::report::{build_report, BuildReportPayload};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QueryRowsResponse};
use trivium_engine::ops::time::TimeAnalysisPayload;
use trivium_engine::storage::{load_flags, upsert_flag};
use trivium_engine::store::ProjectsStore;

// 12 rows; timestamps are one second apart so time analysis sees one session.
fn write_fixture(root: &std::path::Path) -> PathBuf {
    let path = root.join("tags.csv");
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

fn setup() -> (PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-tags-{}", Uuid::new_v4()));
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
    (root, store, summary.meta.id)
}

fn query(
    store: &ProjectsStore,
    id: Uuid,
    bookmarked_only: Option<bool>,
) -> QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            search: None,
            search_columns: None,
            flag_filter: None,
            bookmarked_only,
            offset: None,
            limit: None,
            sort_key: None,
            sort_direction: None,
            condition: None,
        },
    )
    .expect("query failed")
}

fn update(
    store: &ProjectsStore,
    id: Uuid,
    row_index: usize,
    flag: &str,
    memo: Option<&str>,
    tags: Option<Vec<&str>>,
    bookmarked: Option<bool>,
) -> trivium_engine::models::ProjectRow {
    ops::flags::update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index,
            flag: Some(flag.to_string()),
            memo: memo.map(|value| Some(value.to_string())),
            tags: tags.map(|values| values.into_iter().map(str::to_string).collect()),
            bookmarked,
        },
    )
    .expect("update_flag failed")
}

#[test]
fn legacy_flags_json_loads_and_keeps_tags() {
    let root = std::env::temp_dir().join(format!("trivium-tags-legacy-{}", Uuid::new_v4()));
    let project_dir = root.join("project");
    fs::create_dir_all(&project_dir).expect("failed to create project dir");
    let flags_path = project_dir.join("flags.json");
    // Old on-disk shape: no tags/bookmarked keys at all.
    fs::write(&flags_path, r#"{"0":{"flag":"safe","memo":"hello"}}"#)
        .expect("failed to write legacy flags");

    let flags = load_flags(&flags_path).expect("legacy load failed");
    let entry = flags.get(&0).expect("legacy entry missing");
    assert_eq!(entry.flag, "safe");
    assert_eq!(entry.memo.as_deref(), Some("hello"));
    assert!(entry.tags.is_empty(), "tags must default to empty");
    assert!(!entry.bookmarked, "bookmarked must default to false");

    upsert_flag(
        &flags_path,
        0,
        &FlagEntry {
            flag: "safe".to_string(),
            memo: Some("hello".to_string()),
            tags: vec!["pc".to_string(), "gpu".to_string()],
            bookmarked: true,
        },
    )
    .expect("upsert failed");

    let reloaded = load_flags(&flags_path).expect("reload failed");
    let entry = reloaded.get(&0).expect("reloaded entry missing");
    assert_eq!(entry.tags, vec!["pc".to_string(), "gpu".to_string()]);
    assert!(entry.bookmarked);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn update_flag_preserves_tags_and_bookmark_when_omitted() {
    let (root, store, id) = setup();

    let created = update(
        &store,
        id,
        0,
        "suspicious",
        Some("note"),
        Some(vec!["pc", "gpu"]),
        Some(true),
    );
    assert_eq!(created.tags, vec!["pc".to_string(), "gpu".to_string()]);
    assert!(created.bookmarked);

    let changed = update(&store, id, 0, "suspicious", Some("updated"), None, None);
    assert_eq!(changed.memo.as_deref(), Some("updated"));
    assert_eq!(changed.tags, vec!["pc".to_string(), "gpu".to_string()]);
    assert!(changed.bookmarked);

    let row = query(&store, id, None)
        .rows
        .into_iter()
        .find(|row| row.row_index == 0)
        .expect("row missing");
    assert_eq!(row.memo.as_deref(), Some("updated"));
    assert_eq!(row.tags, vec!["pc".to_string(), "gpu".to_string()]);
    assert!(row.bookmarked);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_update_keeps_tags_and_bookmarks_and_undo_restores_them() {
    let (root, store, id) = setup();

    update(
        &store,
        id,
        0,
        "suspicious",
        Some("first"),
        Some(vec!["pc"]),
        Some(true),
    );

    bulk_update(
        &store,
        BulkAnnotatePayload {
            project_id: id,
            target: "all".to_string(),
            row_indices: None,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            flag: Some("safe".to_string()),
            memo: None,
            tags: None,
            bookmarked: None,
        },
    )
    .expect("bulk update failed");

    let rows = query(&store, id, None).rows;
    let row0 = rows.iter().find(|row| row.row_index == 0).expect("row0");
    assert_eq!(row0.flag, "safe");
    assert_eq!(row0.tags, vec!["pc".to_string()]);
    assert!(row0.bookmarked);
    assert!(rows.iter().all(|row| row.flag == "safe"));

    bulk_undo(&store, BulkUndoPayload { project_id: id }).expect("undo failed");

    let restored = query(&store, id, None).rows;
    let row0 = restored
        .iter()
        .find(|row| row.row_index == 0)
        .expect("row0");
    assert_eq!(row0.flag, "suspicious");
    assert_eq!(row0.memo.as_deref(), Some("first"));
    assert_eq!(row0.tags, vec!["pc".to_string()]);
    assert!(row0.bookmarked);
    for row in restored.iter().filter(|row| row.row_index != 0) {
        assert_eq!(row.flag, "", "row {}", row.row_index);
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bookmarked_only_filters_query_but_not_other_analyses() {
    let (root, store, id) = setup();

    for row_index in [0usize, 4, 9] {
        update(&store, id, row_index, "", None, None, Some(true));
    }

    assert_eq!(query(&store, id, Some(true)).total_filtered_rows, 3);
    assert_eq!(query(&store, id, None).total_filtered_rows, 12);
    assert_eq!(query(&store, id, Some(false)).total_filtered_rows, 12);

    let grouped = ops::group::group_tree(
        &store,
        GroupTreePayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            group_columns: vec!["EventID".to_string()],
        },
    )
    .expect("group failed");
    assert_eq!(grouped.total, 12);

    let histogram = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: "EventID".to_string(),
            buckets: 4,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            range: None,
        },
    )
    .expect("histogram failed");
    assert_eq!(
        histogram.buckets.iter().map(|bucket| bucket.count).sum::<u64>(),
        12
    );

    let time = ops::time::time_analysis(
        &store,
        TimeAnalysisPayload {
            project_id: id,
            column: "ts".to_string(),
            gap_seconds: 60,
            session_minutes: 30,
            burst_window_seconds: 60,
            burst_min: 10,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
        },
    )
    .expect("time analysis failed");
    assert_eq!(time.sessions.iter().map(|span| span.count).sum::<u64>(), 12);

    let report = build_report(
        &store,
        BuildReportPayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: None,
            sort_key: None,
            sort_direction: None,
            condition: None,
            destination: root.join("report.html").to_string_lossy().to_string(),
        },
    )
    .expect("report failed");
    assert_eq!(report.hit_rows, 12);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn export_writes_user_tags_and_reimport_keeps_them_as_memo_text() {
    let (root, store, id) = setup();

    update(&store, id, 0, "safe", Some("manual note"), Some(vec!["pc"]), None);

    let destination = root.join("exported.csv");
    ops::export::export_project(
        &store,
        ops::export::ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: None,
            search: None,
            condition: None,
            columns: None,
            sort_key: None,
            sort_direction: None,
        },
    )
    .expect("export failed");

    let exported = fs::read_to_string(&destination).expect("failed to read export");
    assert!(exported.contains("[pc]"), "export must contain the user tag");
    assert!(exported.contains("manual note"));

    // Known F19 limit: import keeps the whole memo text and does not rebuild
    // tags. The tag therefore survives as part of the memo.
    let reimported = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: destination.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
        },
    )
    .expect("reimport failed");
    let row = query(&store, reimported.meta.id, None)
        .rows
        .into_iter()
        .find(|row| row.row_index == 0)
        .expect("reimported row missing");
    assert!(row
        .memo
        .as_deref()
        .unwrap_or_default()
        .contains("[pc]"));
    assert!(row.tags.is_empty(), "import must not rebuild tags");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn query_feedback_includes_tags_and_bookmark() {
    let (root, store, id) = setup();

    update(
        &store,
        id,
        3,
        "critical",
        None,
        Some(vec!["host", "triage"]),
        Some(true),
    );

    let row = query(&store, id, None)
        .rows
        .into_iter()
        .find(|row| row.row_index == 3)
        .expect("row missing");
    assert_eq!(row.flag, "critical");
    assert_eq!(row.tags, vec!["host".to_string(), "triage".to_string()]);
    assert!(row.bookmarked);

    let _ = fs::remove_dir_all(root);
}

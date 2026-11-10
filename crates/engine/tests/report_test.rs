//! Static HTML report output: query parity, escaping, and column stats.

use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::iocs::SaveIocsPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::report::{build_report, BuildReportPayload};
use trivium_engine::ops::rows::{QueryRowsPayload, QuerySpec};
use trivium_engine::store::ProjectsStore;

// 12 rows. Rows 0..4 are alice; rows 4..12 use bob, carol, dave, erin.
fn write_fixture(root: &Path) -> PathBuf {
    let path = root.join("report.csv");
    let body = "ts,EventID,host,user,proc\n\
2024-01-01T00:00:01,4688,WS01,alice,powershell\n\
2024-01-01T00:00:02,4624,WS02,alice,explorer\n\
2024-01-01T00:00:03,4625,WS03,alice,cmd\n\
2024-01-01T00:00:04,4624,WS04,alice,explorer\n\
2024-01-01T00:00:05,4625,WS05,bob,cmd\n\
2024-01-01T00:00:06,4688,WS06,bob,powershell\n\
2024-01-01T00:00:07,4624,WS07,carol,explorer\n\
2024-01-01T00:00:08,4625,WS08,carol,cmd\n\
2024-01-01T00:00:09,4688,WS09,dave,powershell\n\
2024-01-01T00:00:10,4624,WS10,dave,explorer\n\
2024-01-01T00:00:11,4625,WS11,erin,cmd\n\
2024-01-01T00:00:12,4688,WS12,erin,powershell\n";
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn setup() -> (PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-report-{}", Uuid::new_v4()));
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

    for (row_index, flag, memo) in [
        (0usize, "suspicious", Some("manual note")),
        // Intentionally hostile memo used to verify HTML escaping.
        (4usize, "safe", Some("<script>alert(1)</script>")),
    ] {
        ops::flags::update_flag(
            &store,
            UpdateFlagPayload {
                project_id: summary.meta.id,
                row_index,
                flag: Some(flag.to_string()),
                memo: memo.map(|m| Some(m.to_string())),
                tags: None,
                bookmarked: None,
            },
        )
        .expect("failed to set user flag");
    }

    ops::iocs::save_iocs(
        &store,
        SaveIocsPayload {
            project_id: summary.meta.id,
            entries: vec![IocEntry {
                flag: "critical".to_string(),
                tag: "logon-fail".to_string(),
                query: "eventid:4625".to_string(),
            }],
        },
    )
    .expect("failed to save iocs");

    (root, store, summary.meta.id)
}

fn report_payload(id: Uuid, destination: &Path) -> BuildReportPayload {
    BuildReportPayload {
        project_id: id,
        search: Some("alice".to_string()),
        columns: None,
        flag_filter: None,
        sort_key: Some("EventID".to_string()),
        sort_direction: Some("desc".to_string()),
        condition: None,
        destination: destination.to_string_lossy().to_string(),
    }
}

#[test]
fn report_hit_rows_match_query() {
    let (root, store, id) = setup();
    let destination = root.join("reports").join("filtered.html");
    let meta = build_report(&store, report_payload(id, &destination)).expect("report failed");

    let query = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: Some("alice".to_string()),
                search_columns: None,
                flag_filter: None,
                sort_key: Some("EventID".to_string()),
                sort_direction: Some("desc".to_string()),
                condition: None,
                bookmarked_only: None,
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .expect("query failed");

    assert_eq!(meta.total_rows, 12);
    assert_eq!(meta.hit_rows, 4, "alice rows in the fixture");
    assert_eq!(
        meta.hit_rows, query.total_filtered_rows,
        "report hit count must match the query path"
    );
    assert_eq!(meta.flagged_in_matches, query.annotated_filtered);
    assert_eq!(meta.flagged_in_matches, 2);

    let html = fs::read_to_string(&destination).expect("report was not written");
    let hit_meta = format!(
        "<meta name=\"trivium-hit-rows\" content=\"{}\">",
        query.total_filtered_rows
    );
    assert!(html.contains(&hit_meta), "missing hit meta: {hit_meta}");
    assert!(html.contains(&format!(
        "<meta name=\"trivium-total-rows\" content=\"{}\">",
        meta.total_rows
    )));
    assert!(html.contains(&format!(
        "<meta name=\"trivium-flagged-in-matches\" content=\"{}\">",
        meta.flagged_in_matches
    )));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn report_escapes_memos_and_omits_script_tags() {
    let (root, store, id) = setup();
    let destination = root.join("report.html");
    let mut payload = report_payload(id, &destination);
    payload.search = None;
    payload.sort_key = None;
    payload.sort_direction = None;
    let meta = build_report(&store, payload).expect("report failed");
    assert_eq!(meta.total_rows, 12);
    assert_eq!(meta.hit_rows, 12);

    let html = fs::read_to_string(&destination).expect("report was not written");
    assert!(html.contains("manual note"), "manual memo is missing");
    assert!(html.contains("alert(1)"), "hostile memo text is missing");
    assert!(
        html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"),
        "hostile memo must be escaped"
    );
    assert!(
        !html.contains("<script"),
        "raw <script must never appear in the report"
    );
    assert!(html.contains("IOC applied rows: 3"), "project meta is missing");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn report_resolves_display_columns_case_insensitively() {
    let (root, store, id) = setup();
    let destination = root.join("columns.html");
    let mut payload = report_payload(id, &destination);
    payload.search = None;
    payload.sort_key = None;
    payload.sort_direction = None;
    payload.columns = Some(vec!["USER".to_string(), "eventid".to_string()]);
    build_report(&store, payload).expect("report failed");

    let html = fs::read_to_string(&destination).expect("report was not written");
    assert!(html.contains("data-column=\"user\""));
    assert!(html.contains("data-column=\"EventID\""));
    assert!(!html.contains("data-column=\"USER\""));

    let _ = fs::remove_dir_all(root);
}

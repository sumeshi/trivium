//! Parity test for the unified IOC evaluation (R5 + B2).
//!
//! The query path (`ops::rows`), the export path (`ops::export`), and the
//! applied-record count (`ioc::calculate_ioc_applied_records`) must produce
//! the same flags and the same memo tags for the same data, IOC set, and
//! user flags.
//!
//! Unified rules (B2):
//! - a non-empty user flag wins the flag column; a memo-only entry (empty
//!   flag) does not block the IOC flag
//! - matched IOC tags are appended to the memo on every matching row,
//!   whether or not the user set a flag

use std::fs;
use std::path::PathBuf;

use csv::ReaderBuilder;
use uuid::Uuid;

use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::export::ExportProjectPayload;
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::iocs::SaveIocsPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::QueryRowsPayload;
use trivium_engine::store::ProjectsStore;

// 10 rows: rows 0..4 are EventID 4624 (alice), rows 5..9 are 4625 (bob).
// The IOC below matches every 4625 row.
fn write_fixture(root: &std::path::Path) -> PathBuf {
    let path = root.join("parity.csv");
    let mut body = String::from("ts,EventID,host,user,proc\n");
    for i in 0..10 {
        let (event_id, user) = if i < 5 {
            ("4624", "alice")
        } else {
            ("4625", "bob")
        };
        body.push_str(&format!(
            "2024-01-01T00:00:{:02},{},{},WS{},cmd.exe\n",
            i + 1,
            event_id,
            user,
            i
        ));
    }
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn setup() -> (PathBuf, ProjectsStore, uuid::Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-parity-{}", Uuid::new_v4()));
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

    // User flags: row 0 (no IOC match), row 5 "safe" (IOC match; user wins),
    // and row 6 memo-only (empty flag; must NOT block the IOC flag).
    for (row_index, flag, memo) in [
        (0usize, "suspicious", None),
        (5usize, "safe", None),
        (6usize, "", Some("memo only")),
    ] {
        ops::flags::update_flag(
            &store,
            UpdateFlagPayload {
                project_id: summary.meta.id,
                row_index,
                flag: flag.to_string(),
                memo: memo.map(|m| m.to_string()),
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

fn query_flags(store: &ProjectsStore, id: uuid::Uuid) -> Vec<(String, Option<String>)> {
    let response = ops::rows::query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: None,
            offset: None,
            limit: None,
            sort_key: None,
            sort_direction: None,
            condition: None,
            bookmarked_only: None,
        },
    )
    .expect("query failed");
    let mut by_index: Vec<(String, Option<String>)> =
        vec![(Default::default(), None); response.rows.len()];
    for row in &response.rows {
        by_index[row.row_index] = (row.flag.clone(), row.memo.clone());
    }
    by_index
}

/// Parses the exported CSV and returns per-row (flag, memo) in row order,
/// derived from the trivium-* columns.
fn export_flags(destination: &std::path::Path) -> Vec<(String, String)> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(destination)
        .expect("failed to open export");
    let headers = reader.headers().expect("failed to read headers").clone();
    let i_safe = headers.iter().position(|h| h == "trivium-safe").unwrap();
    let i_susp = headers
        .iter()
        .position(|h| h == "trivium-suspicious")
        .unwrap();
    let i_crit = headers
        .iter()
        .position(|h| h == "trivium-critical")
        .unwrap();
    let i_memo = headers.iter().position(|h| h == "trivium-memo").unwrap();

    let mut out = Vec::new();
    for record in reader.records() {
        let record = record.expect("failed to read record");
        let flag = if record.get(i_crit).unwrap() == "1" {
            "critical"
        } else if record.get(i_susp).unwrap() == "1" {
            "suspicious"
        } else if record.get(i_safe).unwrap() == "1" {
            "safe"
        } else {
            ""
        };
        out.push((flag.to_string(), record.get(i_memo).unwrap().to_string()));
    }
    out
}

#[test]
fn query_export_and_count_agree_on_flags_and_tags() {
    let (root, store, id) = setup();

    // Count path: IOC applies only where no user flag exists. Row 6 has a
    // memo-only entry (empty flag), so it still counts. -> rows 6..9.
    let meta = store.find(&id).expect("meta missing");
    assert_eq!(meta.ioc_applied_records, 4);

    // Query path.
    let query = query_flags(&store, id);

    // Export path.
    let destination = root.join("exported.csv");
    ops::export::export_project(
        &store,
        ExportProjectPayload {
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
    let exported = export_flags(&destination);

    assert_eq!(query.len(), 10);
    assert_eq!(exported.len(), 10);

    let expected = [
        "suspicious", // row 0: user flag, no IOC match
        "", "", "", "", // rows 1..4: nothing
        "safe",     // row 5: user flag wins over IOC
        "critical", // row 6: memo-only entry does not block the IOC flag
        "critical", "critical", "critical", // rows 7..9: IOC
    ];
    for i in 0..10 {
        assert_eq!(query[i].0, expected[i], "query flag for row {i}");
        assert_eq!(exported[i].0, expected[i], "export flag for row {i}");
    }

    // Count path agrees with the flag sources: exactly the four IOC rows.
    let ioc_rows = (0..10).filter(|&i| query[i].0 == "critical").count();
    assert_eq!(ioc_rows, meta.ioc_applied_records as usize);

    // B2 tag parity: every matching row (5..9) shows [logon-fail] in both
    // paths, including the user-flagged row 5 and the memo-only row 6.
    for i in [5usize, 6, 7, 8, 9] {
        assert!(
            query[i].1.as_deref().unwrap_or("").contains("[logon-fail]"),
            "query memo for row {i}"
        );
        assert!(exported[i].1.contains("[logon-fail]"), "export memo for row {i}");
    }
    // The memo-only note survives in both paths alongside the tag.
    assert!(query[6].1.as_deref().unwrap_or("").contains("memo only"));
    assert!(exported[6].1.contains("memo only"));
    // Non-matching rows carry no tag.
    for i in [0usize, 1, 2, 3, 4] {
        assert!(!query[i].1.as_deref().unwrap_or("").contains("[logon-fail]"));
        assert!(!exported[i].1.contains("[logon-fail]"));
    }

    let _ = fs::remove_dir_all(root);
}

//! Query/export parity for user annotations after automatic IOC removal (T25).
//!
//! Saving IOC definitions must not change row flags, memos, or the stored
//! `ioc_applied_records` count. The query path, the export path, and the
//! project load path all show user annotations only. The IOC evaluation module
//! remains for an explicit future operation; it is not called here.

use std::fs;
use std::path::PathBuf;

use csv::ReaderBuilder;
use uuid::Uuid;

use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::export::ExportProjectPayload;
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::iocs::SaveIocsPayload;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::rows::{QueryRowsPayload, QuerySpec};
use trivium_engine::store::ProjectsStore;

mod common;

// 10 rows: rows 0..4 are EventID 4624 (alice), rows 5..9 are 4625 (bob).
// The legacy IOC below matches every 4625 row.
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
            restore_annotations: None,
        },
    )
    .expect("failed to create project");
    let ids = common::column_ids(&store, summary.meta.id);

    // User flags: row 0 (no IOC match), row 5 "safe" (IOC match), and row 6
    // memo-only. The legacy IOC matches every 4625 row (5..9).
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
                query: common::search(&ids, "EventID", "4625"),
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
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: None,
                sort_key: None,
                sort_direction: None,
                condition: None,
                bookmarked_only: None,
            },
            offset: None,
            limit: None,
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
fn query_export_and_load_show_user_annotations_only() {
    let (root, store, id) = setup();

    // The stored legacy count is not recalculated when IOC definitions change.
    let meta = store.find(&id).expect("meta missing");
    assert_eq!(meta.ioc_applied_records, 0);

    // Project load returns the definitions, but opening the project leaves
    // every row without an automatic flag or tag.
    let loaded = ops::projects::load_project(&store, ProjectRequest { project_id: id })
        .expect("load failed");
    assert_eq!(loaded.iocs.len(), 1, "legacy IOC definitions are kept");
    assert_eq!(loaded.project.meta.ioc_applied_records, 0);
    for row in &loaded.initial_rows {
        let expected = match row.row_index {
            0 => "suspicious",
            5 => "safe",
            _ => "",
        };
        assert_eq!(row.flag, expected, "loaded flag for row {}", row.row_index);
        assert!(
            !row.memo.as_deref().unwrap_or("").contains("[logon-fail]"),
            "loaded memo for row {} must not gain the IOC tag",
            row.row_index
        );
    }

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
            spec: QuerySpec {
                search: None,
                condition: None,
                sort_key: None,
                sort_direction: None,
                ..Default::default()
            },
            columns: None,
            row_indices: None,
            include_annotations: None,
        },
    )
    .expect("export failed");
    let exported = export_flags(&destination);

    assert_eq!(query.len(), 10);
    assert_eq!(exported.len(), 10);

    let expected = [
        "suspicious", // row 0: user flag
        "",
        "",
        "",
        "",     // rows 1..4: nothing
        "safe", // row 5: user flag
        "",     // row 6: memo-only entry, no automatic flag
        "",
        "",
        "", // rows 7..9: no automatic IOC flag
    ];
    for i in 0..10 {
        assert_eq!(query[i].0, expected[i], "query flag for row {i}");
        assert_eq!(exported[i].0, expected[i], "export flag for row {i}");
    }

    // The memo-only note survives in both paths and no IOC tag is appended.
    assert_eq!(query[6].1.as_deref(), Some("memo only"));
    assert_eq!(exported[6].1, "memo only");
    for i in [0usize, 5, 6, 7, 8, 9] {
        assert!(
            !query[i].1.as_deref().unwrap_or("").contains("[logon-fail]"),
            "query memo for row {i}"
        );
        assert!(
            !exported[i].1.contains("[logon-fail]"),
            "export memo for row {i}"
        );
    }

    let _ = fs::remove_dir_all(root);
}

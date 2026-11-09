//! End-to-end smoke test over the engine ops, without any GUI:
//! create -> query -> annotate -> IOC -> export -> re-import -> delete.
//!
//! The fixture CSV is generated inline (deterministic) instead of depending on
//! scripts/gen_fixture.py, so the test runs on any fresh checkout.

use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::export::ExportProjectPayload;
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::iocs::SaveIocsPayload;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::rows::QueryRowsPayload;
use trivium_engine::store::ProjectsStore;

/// Column layout: ts,EventID,host,user,proc,path,msg
/// 20 rows total:
/// - rows 0..9  : user=alice, EventID 4624 (rows 0..6) / 4625 (rows 6..10)
/// - rows 10..19: user=bob,   EventID 4624 (rows 10..15) / 4688 (rows 15..20)
const ROWS: [(&str, &str, &str, &str); 20] = [
    ("2024-01-01T00:00:01", "4624", "alice", "WS01"),
    ("2024-01-01T00:00:02", "4624", "alice", "WS02"),
    ("2024-01-01T00:00:03", "4624", "alice", "WS03"),
    ("2024-01-01T00:00:04", "4624", "alice", "WS04"),
    ("2024-01-01T00:00:05", "4624", "alice", "WS05"),
    ("2024-01-01T00:00:06", "4624", "alice", "WS06"),
    ("2024-01-01T00:00:07", "4625", "alice", "WS07"),
    ("2024-01-01T00:00:08", "4625", "alice", "WS08"),
    ("2024-01-01T00:00:09", "4625", "alice", "WS09"),
    ("2024-01-01T00:00:10", "4625", "alice", "WS10"),
    ("2024-01-01T00:00:11", "4624", "bob", "WS11"),
    ("2024-01-01T00:00:12", "4624", "bob", "WS12"),
    ("2024-01-01T00:00:13", "4624", "bob", "WS13"),
    ("2024-01-01T00:00:14", "4624", "bob", "WS14"),
    ("2024-01-01T00:00:15", "4624", "bob", "WS15"),
    ("2024-01-01T00:00:16", "4688", "bob", "WS16"),
    ("2024-01-01T00:00:17", "4688", "bob", "WS17"),
    ("2024-01-01T00:00:18", "4688", "bob", "WS18"),
    ("2024-01-01T00:00:19", "4688", "bob", "WS19"),
    ("2024-01-01T00:00:20", "4688", "bob", "WS20"),
];

fn temp_root() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trivium-smoke-{}", Uuid::new_v4()));
    fs::create_dir_all(&dir).expect("failed to create smoke temp dir");
    dir
}

fn write_fixture_csv(root: &std::path::Path) -> PathBuf {
    let path = root.join("smoke.csv");
    let mut body = String::from("ts,EventID,host,user,proc,path,msg\n");
    for (index, (ts, event_id, user, host)) in ROWS.iter().enumerate() {
        // Row 2 carries a quoted message with a comma to exercise CSV escaping.
        let msg = if index == 2 {
            "\"note, with comma\"".to_string()
        } else {
            format!("event={} on {}", event_id, host)
        };
        body.push_str(&format!(
            "{},{},{},{},cmd.exe,C:\\Temp\\,{}\n",
            ts, event_id, host, user, msg
        ));
    }
    fs::write(&path, body).expect("failed to write fixture csv");
    path
}

fn setup_project() -> (PathBuf, ProjectsStore, uuid::Uuid) {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_fixture_csv(&root);
    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: Some("smoke".to_string()),
        },
    )
    .expect("failed to create project");
    (root, store, summary.meta.id)
}

fn query(store: &ProjectsStore, id: uuid::Uuid) -> ops::rows::QueryRowsResponse {
    ops::rows::query_project_rows(
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
        },
    )
    .expect("query failed")
}

#[test]
fn create_and_load_project() {
    let (root, store, id) = setup_project();
    let response = ops::projects::load_project(
        &store,
        ProjectRequest {
            project_id: id.clone(),
        },
    )
    .expect("load failed");

    assert_eq!(response.project.meta.total_records, 20);
    assert_eq!(response.project.meta.name, "smoke");
    assert_eq!(
        response.columns,
        vec![
            "ts".to_string(),
            "EventID".to_string(),
            "host".to_string(),
            "user".to_string(),
            "proc".to_string(),
            "path".to_string(),
            "msg".to_string()
        ]
    );
    assert!(response.hidden_columns.is_empty());
    assert!(response.iocs.is_empty());
    assert_eq!(response.initial_rows.len(), 20);

    let listed = ops::projects::list_projects(&store).expect("list failed");
    assert!(listed.iter().any(|s| s.meta.id == id));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn search_scoping_and_operators() {
    let (root, store, id) = setup_project();

    let base = |search: Option<String>, columns: Option<Vec<String>>| {
        ops::rows::query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: id,
                search,
                columns,
                flag_filter: None,
                offset: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
            condition: None,
            },
        )
        .expect("query failed")
    };

    // Plain term across all columns.
    let r = base(Some("alice".to_string()), None);
    assert_eq!(r.total_filtered_rows, 10);
    assert_eq!(r.rows.len(), 10);

    // Column-scoped term.
    let r = base(Some("user:alice".to_string()), None);
    assert_eq!(r.total_filtered_rows, 10);

    // Column list restricts the searchable text (B1 behaviour): searching
    // "alice" while only EventID is searchable finds nothing.
    let r = base(
        Some("alice".to_string()),
        Some(vec!["EventID".to_string()]),
    );
    assert_eq!(r.total_filtered_rows, 0);

    // AND of two scoped terms.
    let r = base(Some("user:alice eventid:4625".to_string()), None);
    assert_eq!(r.total_filtered_rows, 4);

    // NOT: alice rows without 4625.
    let r = base(Some("user:alice -eventid:4625".to_string()), None);
    assert_eq!(r.total_filtered_rows, 6);

    // OR with column carry-over: user:alice|bob covers all rows.
    let r = base(Some("user:alice|bob".to_string()), None);
    assert_eq!(r.total_filtered_rows, 20);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn sort_and_pagination() {
    let (root, store, id) = setup_project();
    let r = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: None,
            offset: None,
            limit: Some(5),
            sort_key: Some("EventID".to_string()),
            sort_direction: Some("desc".to_string()),
            condition: None,
        },
    )
    .expect("query failed");

    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.total_filtered_rows, 20);
    // 4688 is the largest EventID; descending sort puts those first.
    for row in &r.rows {
        assert_eq!(row.data["EventID"], serde_json::json!(4688));
    }

    let r2 = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: None,
            offset: Some(5),
            limit: Some(5),
            sort_key: Some("EventID".to_string()),
            sort_direction: Some("desc".to_string()),
            condition: None,
        },
    )
    .expect("query failed");
    assert_eq!(r2.offset, 5);
    assert_ne!(r2.rows[0].row_index, r.rows[0].row_index);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn flag_update_and_flag_filter() {
    let (root, store, id) = setup_project();

    let updated = ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: "suspicious".to_string(),
            memo: Some("manual note".to_string()),
        },
    )
    .expect("update failed");
    assert_eq!(updated.flag, "suspicious");

    let r = query(&store, id);
    assert_eq!(r.total_flagged, 1);
    assert_eq!(r.total_rows, 20);

    let r = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: Some("suspicious".to_string()),
            offset: None,
            limit: None,
            sort_key: None,
            sort_direction: None,
            condition: None,
        },
    )
    .expect("query failed");
    assert_eq!(r.total_filtered_rows, 1);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].row_index, 0);
    assert_eq!(r.rows[0].memo.as_deref(), Some("manual note"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn ioc_apply_and_query() {
    let (root, store, id) = setup_project();

    // User flag on an alice/4624 row (no IOC match)...
    ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: "safe".to_string(),
            memo: None,
        },
    )
    .expect("update failed");
    // ...and on an alice/4625 row (IOC match): the user flag wins the flag
    // column, but the matched tag must still surface (B2).
    ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 7,
            flag: "safe".to_string(),
            memo: None,
        },
    )
    .expect("update failed");

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
    .expect("save iocs failed");

    // ioc_applied_records counts rows where the IOC flag actually applies
    // (no user flag): the three remaining 4625 rows (6, 8, 9).
    let meta = store.find(&id).expect("meta missing");
    assert_eq!(meta.ioc_applied_records, 3);

    let r = query(&store, id);
    assert_eq!(r.total_flagged, 5); // 2 user safe + 3 IOC critical

    let flagged: Vec<&trivium_engine::models::ProjectRow> = r
        .rows
        .iter()
        .filter(|row| row.flag == "critical")
        .collect();
    assert_eq!(flagged.len(), 3);
    for row in &flagged {
        let memo = row.memo.as_deref().unwrap_or_default();
        assert!(memo.contains("[logon-fail]"), "memo was: {memo:?}");
    }

    // User flag wins over IOC on row 0 (no match, no tag)...
    let row0 = r.rows.iter().find(|row| row.row_index == 0).unwrap();
    assert_eq!(row0.flag, "safe");
    assert!(row0.memo.is_none());
    // ...and on row 7 (match: user flag, but the tag is appended).
    let row7 = r.rows.iter().find(|row| row.row_index == 7).unwrap();
    assert_eq!(row7.flag, "safe");
    assert!(row7
        .memo
        .as_deref()
        .unwrap_or_default()
        .contains("[logon-fail]"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn export_and_reimport_roundtrip() {
    let (root, store, id) = setup_project();

    ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: "suspicious".to_string(),
            memo: Some("manual note".to_string()),
        },
    )
    .expect("update failed");
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
    .expect("save iocs failed");

    let destination = root.join("exported.csv");
    ops::export::export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
        },
    )
    .expect("export failed");

    let exported = fs::read_to_string(&destination).expect("failed to read export");
    let lines: Vec<&str> = exported.lines().collect();
    assert_eq!(lines.len(), 21); // header + 20 rows
    assert!(lines[0].starts_with("trivium-safe,trivium-suspicious,trivium-critical,trivium-memo"));
    assert!(exported.contains("manual note"));
    assert!(exported.contains("[logon-fail]"));
    // The quoted message with a comma survives the round trip.
    assert!(exported.contains("note, with comma"));

    // Re-import the export: trivium-* columns become flags and memos again.
    let reimported = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: destination.to_string_lossy().to_string(),
            description: None,
        },
    )
    .expect("reimport failed");
    assert_eq!(reimported.meta.total_records, 20);
    assert_eq!(reimported.meta.flagged_records, 5); // 1 suspicious + 4 critical

    let r = query(&store, reimported.meta.id);
    let suspicious: Vec<&trivium_engine::models::ProjectRow> = r
        .rows
        .iter()
        .filter(|row| row.flag == "suspicious")
        .collect();
    assert_eq!(suspicious.len(), 1);
    assert_eq!(suspicious[0].row_index, 0);
    assert!(suspicious[0]
        .memo
        .as_deref()
        .unwrap_or_default()
        .contains("manual note"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn delete_project_removes_files_and_meta() {
    let (root, store, id) = setup_project();
    let project_dir = store.project_dir(&id);
    assert!(project_dir.exists());

    ops::projects::delete_project(
        &store,
        ProjectRequest {
            project_id: id.clone(),
        },
    )
    .expect("delete failed");

    assert!(!project_dir.exists());
    let listed = ops::projects::list_projects(&store).expect("list failed");
    assert!(listed.iter().all(|s| s.meta.id != id));
    let _ = fs::remove_dir_all(root);
}

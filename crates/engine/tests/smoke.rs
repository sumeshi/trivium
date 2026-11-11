//! End-to-end smoke test over the engine ops, without any GUI:
//! create -> query -> annotate -> IOC -> export -> re-import -> delete.
//!
//! Most fixtures are inline. Gap and duplicate checks also call python3.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use uuid::Uuid;

use trivium_engine::condition::{Condition, Group, GroupKind, Node, Op};
use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::export::ExportProjectPayload;
use trivium_engine::ops::flags::{HiddenColumnsPayload, UpdateFlagPayload};
use trivium_engine::ops::surround::{SurroundMode, SurroundingRowsPayload};
use trivium_engine::ops::iocs::SaveIocsPayload;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::rows::{QueryRowsPayload, QuerySpec};
use trivium_engine::ops::cross::CrossSearchPayload;
use trivium_engine::ops::dupes::{DupeMode, DupesPayload};
use trivium_engine::ops::group::GroupTreePayload;
use trivium_engine::ops::histogram::{HistogramPayload, HistogramRange};
use trivium_engine::ops::stats::ColumnStatsPayload;
use trivium_engine::ops::time::TimeAnalysisPayload;
use trivium_engine::store::ProjectsStore;

mod common;

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

fn setup_project() -> (PathBuf, ProjectsStore, uuid::Uuid, std::collections::HashMap<String, String>) {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_fixture_csv(&root);
    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: Some("smoke".to_string()),
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project");
    let ids = common::column_ids(&store, summary.meta.id);
    (root, store, summary.meta.id, ids)
}

fn query(store: &ProjectsStore, id: uuid::Uuid) -> ops::rows::QueryRowsResponse {
    ops::rows::query_project_rows(
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
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .expect("query failed")
}

#[test]
fn create_and_load_project() {
    let (root, store, id, ids) = setup_project();
    let response = ops::projects::load_project(
        &store,
        ProjectRequest {
            project_id: id.clone(),
        },
    )
    .expect("load failed");

    assert_eq!(response.project.meta.total_records, 20);
    assert_eq!(response.project.meta.name, "smoke");
    let source_names = ["ts", "EventID", "host", "user", "proc", "path", "msg"];
    let expected_ids: Vec<String> = source_names.iter().map(|name| common::id(&ids, name)).collect();
    assert_eq!(response.columns, expected_ids);
    assert_eq!(
        response
            .columns_meta
            .iter()
            .map(|column| column.source.as_str())
            .collect::<Vec<_>>(),
        source_names
    );
    assert!(response.hidden_columns.is_empty());
    assert!(response.iocs.is_empty());
    assert_eq!(response.initial_rows.len(), 20);
    assert_eq!(
        response.column_kinds.get(&common::id(&ids, "ts")).map(String::as_str),
        Some("datetime")
    );
    assert_eq!(
        response.column_kinds.get(&common::id(&ids, "EventID")).map(String::as_str),
        Some("int")
    );
    assert_eq!(
        response.column_kinds.get(&common::id(&ids, "user")).map(String::as_str),
        Some("string")
    );

    let listed = ops::projects::list_projects(&store).expect("list failed");
    assert!(listed.iter().any(|s| s.meta.id == id));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn search_scoping_and_operators() {
    let (root, store, id, ids) = setup_project();

    let base = |search: Option<String>, search_columns: Option<Vec<String>>| {
        ops::rows::query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: id,
                spec: QuerySpec {
                    search: search,
                    search_columns: search_columns,
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
    };

    // Plain term across all columns.
    let r = base(Some("alice".to_string()), None);
    assert_eq!(r.total_filtered_rows, 10);
    assert_eq!(r.rows.len(), 10);

    // Column-scoped term.
    let r = base(Some(common::search(&ids, "user", "alice")), None);
    assert_eq!(r.total_filtered_rows, 10);

    // An explicit search scope restricts the searchable text: searching
    // "alice" while only EventID is searchable finds nothing.
    let r = base(
        Some("alice".to_string()),
        Some(vec![common::id(&ids, "EventID")]),
    );
    assert_eq!(r.total_filtered_rows, 0);

    // AND of two scoped terms.
    let r = base(
        Some(format!(
            "{} {}",
            common::search(&ids, "user", "alice"),
            common::search(&ids, "EventID", "4625")
        )),
        None,
    );
    assert_eq!(r.total_filtered_rows, 4);

    // NOT: alice rows without 4625.
    let r = base(
        Some(format!(
            "{} -{}",
            common::search(&ids, "user", "alice"),
            common::search(&ids, "EventID", "4625")
        )),
        None,
    );
    assert_eq!(r.total_filtered_rows, 6);

    // OR with column carry-over: user:alice|bob covers all rows.
    let r = base(Some(common::search(&ids, "user", "alice|bob")), None);
    assert_eq!(r.total_filtered_rows, 20);

    let _ = fs::remove_dir_all(root);
}

fn eq_condition(column: &str, value: &str) -> Node {
    Node::Condition(Condition {
        column: column.to_string(),
        op: Op::Eq,
        values: vec![value.to_string()],
        negated: false,
    })
}

fn and_group(children: Vec<Node>) -> Node {
    Node::Group(Group {
        kind: GroupKind::And,
        children,
    })
}

#[test]
fn nested_condition_matches_either_group() {
    let (root, store, id, ids) = setup_project();
    let user_id = common::id(&ids, "user");
    let event_id = common::id(&ids, "EventID");
    // (user = alice AND EventID = 4624) OR (user = bob AND EventID = 4688)
    // Fixture: 6 alice/4624 rows and 5 bob/4688 rows.
    let condition = Node::Group(Group {
        kind: GroupKind::Or,
        children: vec![
            and_group(vec![
                eq_condition(&user_id, "alice"),
                eq_condition(&event_id, "4624"),
            ]),
            and_group(vec![
                eq_condition(&user_id, "bob"),
                eq_condition(&event_id, "4688"),
            ]),
        ],
    });
    let response = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: None,
                sort_key: None,
                sort_direction: None,
                condition: Some(condition),
                bookmarked_only: None,
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .expect("query failed");
    assert_eq!(response.total_filtered_rows, 11);
    assert!(response.rows.iter().all(|row| {
        let user = row.data.get(&user_id).and_then(|value| value.as_str());
        let event = row.data.get(&event_id).and_then(|value| value.as_str());
        matches!(
            (user, event),
            (Some("alice"), Some("4624")) | (Some("bob"), Some("4688"))
        )
    }));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn nanosecond_integer_is_not_rounded() {
    let root = temp_root();
    let csv = root.join("ns.csv");
    fs::write(
        &csv,
        "name,n,ts\na,1700000000123456789,2024-01-01T00:00:01\nb,2,2024-01-01T00:00:02\n",
    )
    .expect("failed to write ns csv");
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
    let n_id = common::id(&ids, "n");
    let ts_id = common::id(&ids, "ts");
    let loaded = ops::projects::load_project(
        &store,
        ProjectRequest { project_id: id },
    )
    .expect("load failed");
    assert_eq!(loaded.column_kinds.get(&n_id).map(String::as_str), Some("int"));
    assert_eq!(loaded.column_kinds.get(&ts_id).map(String::as_str), Some("datetime"));

    let exact = "1700000000123456789";
    let response = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: None,
                sort_key: None,
                sort_direction: None,
                condition: Some(eq_condition(&n_id, exact)),
                bookmarked_only: None,
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .expect("query failed");
    assert_eq!(response.total_filtered_rows, 1);
    let cell = response.rows[0].data.get(&n_id).expect("n");
    let preserved = cell.as_i64() == Some(1_700_000_000_123_456_789)
        || cell.as_str() == Some(exact);
    assert!(preserved, "value was rounded: {cell}");

    let _ = fs::remove_dir_all(root);
}

fn python_missing_unique(csv: &Path) -> serde_json::Value {
    let script = r#"
import csv, json, sys
with open(sys.argv[1], newline="") as handle:
    rows = list(csv.DictReader(handle))
out = {}
for column in rows[0].keys():
    values = [row[column] for row in rows]
    present = [value for value in values if value != ""]
    out[column] = {"missing": sum(value == "" for value in values), "unique": len(set(present))}
print(json.dumps(out))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(csv)
        .output()
        .expect("python3");
    assert!(
        output.status.success(),
        "python3 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("python json")
}

#[test]
fn column_stats_match_python_counts() {
    let root = temp_root();
    let csv = root.join("stats.csv");
    let mut body = String::from("user,event,note,token\n");
    for index in 0..30 {
        let user = match index % 5 {
            0 => "",
            1 | 2 => "alice",
            _ => "bob",
        };
        let event = if index == 7 {
            ""
        } else if index % 2 == 0 {
            "4624"
        } else {
            "4625"
        };
        let note = if index % 6 == 0 { "" } else { "alpha" };
        let token = if index < 2 {
            "dup".to_string()
        } else {
            format!("t{index:02}")
        };
        body.push_str(&format!("{user},{event},{note},{token}\n"));
    }
    fs::write(&csv, body).expect("failed to write stats csv");
    let expected = python_missing_unique(&csv);

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
    let user_id = common::id(&ids, "user");
    let event_id = common::id(&ids, "event");
    let note_id = common::id(&ids, "note");
    let token_id = common::id(&ids, "token");

    let omitted = format!(r#"{{"projectId":"{id}","column":"{user_id}"}}"#);
    let parsed: ColumnStatsPayload = serde_json::from_str(&omitted).expect("payload");
    assert_eq!(parsed.rare_max, 1);
    assert_eq!(parsed.column, user_id);

    let named_columns = [
        ("user", user_id.clone()),
        ("event", event_id.clone()),
        ("note", note_id.clone()),
        ("token", token_id.clone()),
    ];
    for (name, column) in &named_columns {
        let stats = ops::stats::column_stats(
            &store,
            ColumnStatsPayload {
                project_id: id,
                column: column.clone(),
                rare_max: 1,
            },
        )
        .expect("stats");
        let want = &expected[*name];
        assert_eq!(
            stats.missing,
            want["missing"].as_u64().expect("missing"),
            "{name} missing"
        );
        assert_eq!(
            stats.unique,
            want["unique"].as_u64().expect("unique"),
            "{name} unique"
        );
    }

    let user = ops::stats::column_stats(
        &store,
        ColumnStatsPayload {
            project_id: id,
            column: user_id,
            rare_max: 1,
        },
    )
    .expect("user stats");
    assert_eq!(user.kind, "string");
    assert_eq!(
        user.top.iter().map(|item| item.count).sum::<u64>() + user.missing,
        30
    );

    let event = ops::stats::column_stats(
        &store,
        ColumnStatsPayload {
            project_id: id,
            column: event_id,
            rare_max: 1,
        },
    )
    .expect("event stats");
    assert_eq!(event.kind, "int");

    let token = ops::stats::column_stats(
        &store,
        ColumnStatsPayload {
            project_id: id,
            column: token_id,
            rare_max: 1,
        },
    )
    .expect("token stats");
    assert_eq!(token.top.len(), 20);
    assert_eq!(token.top[0].value, "dup");
    assert_eq!(token.top[0].count, 2);
    assert!(token.top.windows(2).all(|pair| pair[0].count >= pair[1].count));
    assert!(token.rare.iter().all(|item| item.count == 1));
    assert_eq!(token.rare.len() as u64, token.unique - 1);
    assert!(!token.rare.iter().any(|item| item.value == "dup"));

    assert!(ops::stats::column_stats(
        &store,
        ColumnStatsPayload {
            project_id: id,
            column: "nope".to_string(),
            rare_max: 1,
        },
    )
    .is_err());
    assert!(ops::stats::column_stats(
        &store,
        ColumnStatsPayload {
            project_id: id,
            column: "__rowid".to_string(),
            rare_max: 1,
        },
    )
    .is_err());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn duplicate_rows_keep_every_occurrence() {
    let root = temp_root();
    let csv = root.join("dupes.csv");
    fs::write(&csv, "user,proc\nalice,cmd\nalice,cmd\nalice,cmd\nbob,cmd\n").expect("write");
    let store = ProjectsStore::new(root.clone()).expect("store");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("create")
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    let user_id = common::id(&ids, "user");
    let proc_id = common::id(&ids, "proc");

    let duplicates = ops::dupes::duplicate_rows(
        &store,
        DupesPayload {
            project_id: id,
            columns: vec![user_id.clone(), proc_id.clone()],
            mode: DupeMode::Duplicates,
        },
    )
    .expect("duplicates");
    assert_eq!(
        duplicates.rows.iter().map(|row| row.row_index).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let unique = ops::dupes::duplicate_rows(
        &store,
        DupesPayload {
            project_id: id,
            columns: vec![user_id, proc_id],
            mode: DupeMode::Unique,
        },
    )
    .expect("unique");
    assert_eq!(
        unique.rows.iter().map(|row| row.row_index).collect::<Vec<_>>(),
        vec![3]
    );
    assert!(ops::dupes::duplicate_rows(
        &store,
        DupesPayload {
            project_id: id,
            columns: Vec::new(),
            mode: DupeMode::Duplicates,
        },
    )
    .is_err());

    let _ = fs::remove_dir_all(root);
}

fn python_duplicate_counts(csv: &Path) -> serde_json::Value {
    let script = r#"
import csv, json, sys
from collections import Counter
with open(sys.argv[1], newline="") as handle:
    rows = list(csv.DictReader(handle))
keys = [(row["user"], row["proc"]) for row in rows]
counts = Counter(keys)
print(json.dumps({
    "rows": len(rows),
    "duplicates": sum(1 for key in keys if counts[key] > 1),
    "unique": sum(1 for key in keys if counts[key] == 1),
}))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(csv)
        .output()
        .expect("python3");
    assert!(
        output.status.success(),
        "python3 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("python json")
}

#[test]
fn duplicate_rows_match_fixture_duplicates() {
    let root = temp_root();
    let csv = root.join("fixture.csv");
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/gen_fixture.py");
    let status = Command::new("python3")
        .arg(&script)
        .args(["--rows", "1000", "--seed", "42", "--out"])
        .arg(&csv)
        .status()
        .expect("python3");
    assert!(status.success(), "gen_fixture failed");
    let expected = python_duplicate_counts(&csv);
    let expected_rows = expected["rows"].as_u64().expect("rows");
    let expected_dupes = expected["duplicates"].as_u64().expect("duplicates");
    let expected_unique = expected["unique"].as_u64().expect("unique");
    assert_eq!(expected_rows, 1000);
    assert!(expected_dupes > 0, "fixture produced no duplicate user/proc keys");
    assert_eq!(expected_dupes + expected_unique, expected_rows);

    let store = ProjectsStore::new(root.clone()).expect("store");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("create")
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    let user_id = common::id(&ids, "user");
    let proc_id = common::id(&ids, "proc");
    let duplicates = ops::dupes::duplicate_rows(
        &store,
        DupesPayload {
            project_id: id,
            columns: vec![user_id.clone(), proc_id.clone()],
            mode: DupeMode::Duplicates,
        },
    )
    .expect("duplicates");
    let unique = ops::dupes::duplicate_rows(
        &store,
        DupesPayload {
            project_id: id,
            columns: vec![user_id, proc_id],
            mode: DupeMode::Unique,
        },
    )
    .expect("unique");
    assert_eq!(duplicates.rows.len() as u64, expected_dupes);
    assert_eq!(unique.rows.len() as u64, expected_unique);

    let _ = fs::remove_dir_all(root);
}

fn bucket_sum(buckets: &[trivium_engine::analysis::HistogramBucket]) -> u64 {
    buckets.iter().map(|bucket| bucket.count).sum()
}

#[test]
fn histogram_bucket_sum_matches_filtered_rows() {
    let (root, store, id, ids) = setup_project();
    let user_id = common::id(&ids, "user");
    let event_id = common::id(&ids, "EventID");
    let ts_id = common::id(&ids, "ts");
    let condition = Some(eq_condition(&user_id, "alice"));
    let queried = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: None,
                sort_key: None,
                sort_direction: None,
                condition: condition.clone(),
                bookmarked_only: None,
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .expect("query");
    let hist = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: event_id.clone(),
            buckets: 4,
            search: None,
            columns: None,
            flag_filter: None,
            condition,
            range: None,
        },
    )
    .expect("histogram");
    assert_eq!(hist.missing, 0);
    assert_eq!(hist.buckets.len(), 4);
    assert_eq!(bucket_sum(&hist.buckets), queried.total_filtered_rows as u64);

    let searched = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: Some("WS01".to_string()),
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
    .expect("search query");
    let searched_hist = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: event_id.clone(),
            buckets: 4,
            search: Some("WS01".to_string()),
            columns: None,
            flag_filter: None,
            condition: None,
            range: None,
        },
    )
    .expect("search histogram");
    assert_eq!(
        bucket_sum(&searched_hist.buckets) + searched_hist.missing,
        searched.total_filtered_rows as u64
    );

    let omitted = format!(r#"{{"projectId":"{id}","column":"{ts_id}"}}"#);
    let parsed: HistogramPayload = serde_json::from_str(&omitted).expect("payload");
    assert_eq!(parsed.buckets, 50);
    let timestamps = ops::histogram::histogram(&store, parsed).expect("timestamps");
    assert_eq!(timestamps.kind, "datetime");
    assert_eq!(timestamps.missing, 0);
    assert_eq!(timestamps.buckets.len(), 50);
    assert_eq!(bucket_sum(&timestamps.buckets), 20);

    assert!(ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: user_id.clone(),
            buckets: 4,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            range: None,
        },
    )
    .is_err());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn histogram_places_values_in_equal_width_buckets() {
    let root = temp_root();
    let csv = root.join("hist.csv");
    fs::write(&csv, "n,x\n0,0.0\n10,0.5\n20,1.0\n").expect("write");
    let store = ProjectsStore::new(root.clone()).expect("store");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("create")
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    let n_id = common::id(&ids, "n");
    let x_id = common::id(&ids, "x");
    let ints = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: n_id,
            buckets: 2,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            range: None,
        },
    )
    .expect("ints");
    assert_eq!(ints.kind, "int");
    assert_eq!(bucket_sum(&ints.buckets), 3);
    assert_eq!(ints.buckets[0].count, 1);
    assert_eq!(ints.buckets[1].count, 2);
    assert_eq!(ints.buckets[0].start, "0");
    assert_eq!(ints.buckets[0].end, "10");
    assert_eq!(ints.buckets[1].start, "10");
    assert_eq!(ints.buckets[1].end, "20");

    let floats = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: x_id,
            buckets: 2,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            range: None,
        },
    )
    .expect("floats");
    assert_eq!(floats.kind, "float");
    assert_eq!(floats.buckets[0].count, 1);
    assert_eq!(floats.buckets[1].count, 2);
    assert_eq!(bucket_sum(&floats.buckets), 3);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn histogram_range_counts_outside_values() {
    let (root, store, id, ids) = setup_project();
    let event_id = common::id(&ids, "EventID");
    let ts_id = common::id(&ids, "ts");
    let ints = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: event_id.clone(),
            buckets: 2,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            range: Some(HistogramRange {
                min: "4624".to_string(),
                max: "4625".to_string(),
            }),
        },
    )
    .expect("int range");
    assert_eq!(ints.kind, "int");
    assert_eq!(ints.missing, 0);
    assert_eq!(ints.outside, 5);
    assert_eq!(bucket_sum(&ints.buckets), 15);
    assert_eq!(bucket_sum(&ints.buckets) + ints.outside + ints.missing, 20);

    let times = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: ts_id.clone(),
            buckets: 2,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            range: Some(HistogramRange {
                min: "2024-01-01T00:00:01".to_string(),
                max: "2024-01-01T00:00:11".to_string(),
            }),
        },
    )
    .expect("datetime range");
    assert_eq!(times.kind, "datetime");
    assert_eq!(times.missing, 0);
    assert_eq!(times.outside, 9);
    assert_eq!(times.buckets.len(), 2);
    assert_eq!(bucket_sum(&times.buckets), 11);
    assert_eq!(bucket_sum(&times.buckets) + times.outside + times.missing, 20);
    assert!(times.buckets[0].start.starts_with("2024-01-01T00:00:01"));
    assert!(times.buckets[1].end.starts_with("2024-01-01T00:00:11"));

    let json = format!(
        r#"{{"projectId":"{id}","column":"{event_id}","buckets":2,"range":{{"min":"4624","max":"4625"}}}}"#
    );
    let parsed: HistogramPayload = serde_json::from_str(&json).expect("range payload");
    assert_eq!(parsed.range.expect("range").min, "4624");

    let invalid = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: ts_id.clone(),
            buckets: 2,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            range: Some(HistogramRange {
                min: "not-a-date".to_string(),
                max: "2024-01-01T00:00:11".to_string(),
            }),
        },
    );
    assert!(invalid.is_err());

    let reversed = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: event_id.clone(),
            buckets: 2,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            range: Some(HistogramRange {
                min: "4625".to_string(),
                max: "4624".to_string(),
            }),
        },
    );
    assert!(reversed.is_err());

    let _ = fs::remove_dir_all(root);
}

fn python_fixture_gaps(csv: &Path) -> serde_json::Value {
    let script = r#"
import csv, json, sys
from datetime import datetime

def parse(value):
    # Python < 3.11 rejects fractional seconds longer than 6 digits, and the
    # fixture alternates 3-digit (ms) and 9-digit (ns) fractions.
    if "." in value:
        head, fraction = value.split(".", 1)
        value = head + "." + fraction[:6]
    return datetime.fromisoformat(value)

with open(sys.argv[1], newline="") as handle:
    times = [parse(row["ts"]) for row in csv.DictReader(handle)]
gaps = []
sessions = 1 if times else 0
for left, right in zip(times, times[1:]):
    if (right - left).total_seconds() >= 60:
        gaps.append((right - left).total_seconds())
        sessions += 1
print(json.dumps({
    "rows": len(times),
    "gaps": len(gaps),
    "min_gap": min(gaps) if gaps else 0,
    "sessions": sessions,
}))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(csv)
        .output()
        .expect("python3");
    assert!(
        output.status.success(),
        "python3 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("python json")
}

#[test]
fn time_analysis_finds_fixture_gaps() {
    let root = temp_root();
    let csv = root.join("fixture.csv");
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/gen_fixture.py");
    let status = Command::new("python3")
        .arg(&script)
        .args(["--rows", "300", "--seed", "42", "--out"])
        .arg(&csv)
        .status()
        .expect("python3");
    assert!(status.success(), "gen_fixture failed");
    let expected = python_fixture_gaps(&csv);
    let store = ProjectsStore::new(root.clone()).expect("store");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("create")
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    let ts_id = common::id(&ids, "ts");
    let analysis = ops::time::time_analysis(
        &store,
        TimeAnalysisPayload {
            project_id: id,
            column: ts_id,
            gap_seconds: 60,
            session_minutes: 1,
            burst_window_seconds: 60,
            burst_min: 10,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
        },
    )
    .expect("time analysis");
    assert_eq!(analysis.gaps.len() as u64, expected["gaps"].as_u64().expect("gaps"));
    assert!(analysis.gaps.len() >= 2);
    assert!(analysis.gaps.iter().all(|gap| gap.count >= 37 * 60));
    assert_eq!(
        analysis.sessions.len() as u64,
        expected["sessions"].as_u64().expect("sessions")
    );
    assert_eq!(
        analysis.sessions.iter().map(|span| span.count).sum::<u64>(),
        expected["rows"].as_u64().expect("rows")
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn time_analysis_gap_range_matches_bounds() {
    let root = temp_root();
    let csv = root.join("gaps.csv");
    fs::write(
        &csv,
        "ts\n2024-01-01T00:00:00\n2024-01-01T00:00:01\n2024-01-01T00:00:02\n2024-01-01T00:02:00\n",
    )
    .expect("write");
    let store = ProjectsStore::new(root.clone()).expect("store");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("create")
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    let ts_id = common::id(&ids, "ts");
    let analysis = ops::time::time_analysis(
        &store,
        TimeAnalysisPayload {
            project_id: id,
            column: ts_id.clone(),
            gap_seconds: 30,
            session_minutes: 1,
            burst_window_seconds: 10,
            burst_min: 2,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
        },
    )
    .expect("time analysis");
    assert_eq!(analysis.gaps.len(), 1);
    assert_eq!(analysis.gaps[0].start, "2024-01-01T00:00:02");
    assert_eq!(analysis.gaps[0].end, "2024-01-01T00:02:00");
    assert_eq!(analysis.gaps[0].count, 118);
    assert_eq!(analysis.sessions.len(), 2);
    assert_eq!(analysis.sessions[0].count, 3);
    assert_eq!(analysis.sessions[1].count, 1);
    assert_eq!(analysis.bursts.len(), 1);
    assert_eq!(analysis.bursts[0].count, 3);

    let condition = Node::Group(Group {
        kind: GroupKind::And,
        children: vec![
            Node::Condition(Condition {
                column: ts_id.clone(),
                op: Op::Ge,
                values: vec![analysis.gaps[0].start.clone()],
                negated: false,
            }),
            Node::Condition(Condition {
                column: ts_id.clone(),
                op: Op::Le,
                values: vec![analysis.gaps[0].end.clone()],
                negated: false,
            }),
        ],
    });
    let queried = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: None,
                sort_key: None,
                sort_direction: None,
                condition: Some(condition),
                bookmarked_only: None,
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .expect("query");
    assert_eq!(queried.total_filtered_rows, 2);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn sort_and_pagination() {
    let (root, store, id, ids) = setup_project();
    let event_id = common::id(&ids, "EventID");
    let r = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: None,
                sort_key: Some(event_id.clone()),
                sort_direction: Some("desc".to_string()),
                condition: None,
                bookmarked_only: None,
                ..Default::default()
            },
            offset: None,
            limit: Some(5),
        },
    )
    .expect("query failed");

    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.total_filtered_rows, 20);
    // 4688 is the largest EventID; descending sort puts those first.
    for row in &r.rows {
        assert_eq!(row.data[&event_id], serde_json::json!("4688"));
    }

    let r2 = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: None,
                sort_key: Some(event_id.clone()),
                sort_direction: Some("desc".to_string()),
                condition: None,
                bookmarked_only: None,
                ..Default::default()
            },
            offset: Some(5),
            limit: Some(5),
        },
    )
    .expect("query failed");
    assert_eq!(r2.offset, 5);
    assert_ne!(r2.rows[0].row_index, r.rows[0].row_index);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn flag_update_and_flag_filter() {
    let (root, store, id, _ids) = setup_project();

    let updated = ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: Some("suspicious".to_string()),
            memo: Some(Some("manual note".to_string())),
            tags: None,
            bookmarked: None,
        },
    )
    .expect("update failed");
    assert_eq!(updated.entry.expect("entry missing").flag, "suspicious");

    let r = query(&store, id);
    assert_eq!(r.annotated_total, 1);
    assert_eq!(r.total_rows, 20);

    let r = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: Some("suspicious".to_string()),
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
    .expect("query failed");
    assert_eq!(r.total_filtered_rows, 1);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].row_index, 0);
    assert_eq!(r.rows[0].memo.as_deref(), Some("manual note"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn ioc_apply_and_query() {
    let (root, store, id, ids) = setup_project();

    // User flag on an alice/4624 row (no IOC match)...
    ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: Some("safe".to_string()),
            memo: None,
            tags: None,
            bookmarked: None,
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
            flag: Some("safe".to_string()),
            memo: None,
            tags: None,
            bookmarked: None,
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
                query: common::search(&ids, "EventID", "4625"),
            }],
        },
    )
    .expect("save iocs failed");

    // ioc_applied_records counts rows where the IOC flag actually applies
    // (no user flag): the three remaining 4625 rows (6, 8, 9).
    let meta = store.find(&id).expect("meta missing");
    assert_eq!(meta.ioc_applied_records, 3);

    let r = query(&store, id);
    assert_eq!(r.annotated_total, 5); // 2 user safe + 3 IOC critical

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
    let (root, store, id, ids) = setup_project();

    ops::flags::update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: Some("suspicious".to_string()),
            memo: Some(Some("manual note".to_string())),
            tags: None,
            bookmarked: None,
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
                query: common::search(&ids, "EventID", "4625"),
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
            delimiter: None,
            restore_annotations: Some(true),
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
    let (root, store, id, _ids) = setup_project();
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

#[test]
fn surrounding_rows_keep_hidden_values_and_time_order() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = root.join("around.csv");
    // File order is not time order. Row 0 is the middle timestamp.
    fs::write(
        &csv,
        "\
ts,note,secret
2024-01-01T00:00:05,c,hidden-c
2024-01-01T00:00:01,a,hidden-a
2024-01-01T00:00:03,b,hidden-b
2024-01-01T00:00:09,e,hidden-e
2024-01-01T00:00:07,d,hidden-d
",
    )
    .expect("failed to write fixture");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: Some("around".to_string()),
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    let ts_id = common::id(&ids, "ts");
    let note_id = common::id(&ids, "note");
    let secret_id = common::id(&ids, "secret");
    ops::flags::set_hidden_columns(
        &store,
        HiddenColumnsPayload {
            project_id: id,
            hidden_columns: vec![secret_id.clone()],
        },
    )
    .expect("failed to hide secret");

    let around = |mode: SurroundMode, count: u64| {
        ops::surround::surrounding_rows(
            &store,
            SurroundingRowsPayload {
                project_id: id,
                row_index: 0,
                column: ts_id.clone(),
                mode,
                count,
            },
        )
        .expect("surrounding rows failed")
    };
    let secrets = |rows: &[trivium_engine::models::ProjectRow]| -> Vec<String> {
        rows.iter()
            .map(|row| {
                row.data
                    .get(&secret_id)
                    .and_then(|value| value.as_str())
                    .unwrap()
                    .to_string()
            })
            .collect()
    };

    let neighbors = around(SurroundMode::Neighbors, 1);
    assert_eq!(
        neighbors
            .rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        vec![2, 0, 4]
    );
    assert_eq!(
        secrets(&neighbors.rows),
        vec!["hidden-b", "hidden-c", "hidden-d"]
    );

    let window = around(SurroundMode::Window, 2);
    assert_eq!(
        secrets(&window.rows),
        vec!["hidden-b", "hidden-c", "hidden-d"]
    );

    let wide = around(SurroundMode::Neighbors, 4);
    assert_eq!(
        secrets(&wide.rows),
        vec!["hidden-a", "hidden-b", "hidden-c", "hidden-d", "hidden-e"]
    );

    let not_time = ops::surround::surrounding_rows(
        &store,
        SurroundingRowsPayload {
            project_id: id,
            row_index: 0,
            column: note_id,
            mode: SurroundMode::Neighbors,
            count: 1,
        },
    );
    assert!(not_time.unwrap_err().to_string().contains("not datetime"));
    let missing = ops::surround::surrounding_rows(
        &store,
        SurroundingRowsPayload {
            project_id: id,
            row_index: 99,
            column: ts_id,
            mode: SurroundMode::Window,
            count: 1,
        },
    );
    assert!(missing.unwrap_err().to_string().contains("Row not found"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn cross_project_search_matches_each_query() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let first_csv = write_fixture_csv(&root);
    let second_csv = root.join("other.csv");
    fs::write(
        &second_csv,
        "\
ts,EventID,host,user,proc,path,msg
2024-01-01T00:00:01,4624,H1,alice,cmd.exe,C:\\Temp\\,a
2024-01-01T00:00:02,4624,H1,bob,cmd.exe,C:\\Temp\\,b
2024-01-01T00:00:03,4624,H1,alice,cmd.exe,C:\\Temp\\,c
",
    )
    .expect("failed to write second fixture");
    let create = |path: &std::path::Path| {
        ops::projects::create_project(
            &store,
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
    };
    let first = create(&first_csv);
    let second = create(&second_csv);
    let ids = common::column_ids(&store, first);
    let condition = eq_condition(&common::id(&ids, "user"), "alice");
    let hits = ops::cross::search_projects(
        &store,
        CrossSearchPayload {
            project_ids: vec![first, second],
            search: None,
            columns: None,
            flag_filter: None,
            offset: None,
            limit: Some(1),
            sort_key: None,
            sort_direction: None,
            condition: Some(condition.clone()),
        },
    )
    .expect("cross search failed");
    assert_eq!(hits.len(), 2);
    for hit in &hits {
        let alone = ops::rows::query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: hit.project_id,
                spec: QuerySpec {
                    search: None,
                    search_columns: None,
                    flag_filter: None,
                    sort_key: None,
                    sort_direction: None,
                    condition: Some(condition.clone()),
                    bookmarked_only: None,
                    ..Default::default()
                },
                offset: None,
                limit: Some(1),
            },
        )
        .expect("query failed");
        assert_eq!(hit.total_filtered_rows, alone.total_filtered_rows);
        assert_eq!(hit.rows.len(), alone.rows.len());
        assert_eq!(hit.rows.len(), 1);
    }
    assert_eq!(hits[0].total_filtered_rows, 10);
    assert_eq!(hits[1].total_filtered_rows, 2);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn legacy_flags_json_defaults_tags_and_bookmark() {
    let root = temp_root();
    let project_dir = root.join("legacy");
    fs::create_dir_all(&project_dir).expect("failed to create legacy dir");
    let flags_path = project_dir.join("flags.json");
    fs::write(&flags_path, r#"{"0":{"flag":"safe","memo":"hello"}}"#)
        .expect("failed to write legacy flags");

    let flags = trivium_engine::storage::load_flags(&flags_path).expect("legacy load failed");
    let entry = flags.get(&0).expect("legacy entry missing");
    assert_eq!(entry.flag, "safe");
    assert_eq!(entry.memo.as_deref(), Some("hello"));
    assert!(entry.tags.is_empty());
    assert!(!entry.bookmarked);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn grouping_counts_match_the_query_path() {
    let (root, store, id, ids) = setup_project();
    let grouped = ops::group::group_tree(
        &store,
        GroupTreePayload {
            project_id: id,
            search: None,
            columns: None,
            flag_filter: None,
            condition: None,
            group_columns: vec![common::id(&ids, "EventID")],
        },
    )
    .expect("grouping failed");
    let queried = query(&store, id);

    assert_eq!(grouped.total, queried.total_filtered_rows);
    assert_eq!(
        grouped.nodes.iter().map(|node| node.count).sum::<usize>(),
        grouped.total
    );
    assert_eq!(grouped.nodes.len(), 3);
    let _ = fs::remove_dir_all(root);
}

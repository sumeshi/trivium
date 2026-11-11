//! Append-only IOC cache increments (P3).
//!
//! Covers the incremental path in `cached_ioc_outcomes`: when the IOC set is
//! the cached set plus entries appended at the end, the merged result must be
//! identical to a full `evaluate_iocs` over the whole set (flags and tag
//! order included). Non-append changes must fall back to a full evaluation
//! and still agree with `evaluate_iocs`.

use std::fs;
use std::path::{Path, PathBuf};

use polars::prelude::{DataFrame, NamedFrom, Series};
use uuid::Uuid;

use trivium_engine::flags::normalize_flag_value;
use trivium_engine::ioc::{cached_ioc_outcomes, evaluate_iocs};
use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::storage::load_ioc_flag_cache;
use trivium_engine::store::ProjectsStore;

const ROWS: usize = 30;

fn entry(flag: &str, tag: &str, query: &str) -> IocEntry {
    IocEntry {
        flag: flag.to_string(),
        tag: tag.to_string(),
        query: query.to_string(),
    }
}

/// 30 rows: host cycles WS00..WS04, user is alice for rows 0..14 and bob for
/// rows 15..29.
fn frame(rows: usize) -> DataFrame {
    let hosts: Vec<String> = (0..rows).map(|i| format!("WS{:02}", i % 5)).collect();
    let users: Vec<String> = (0..rows)
        .map(|i| if i < 15 { "alice" } else { "bob" }.to_string())
        .collect();
    let host_refs: Vec<&str> = hosts.iter().map(|s| s.as_str()).collect();
    let user_refs: Vec<&str> = users.iter().map(|s| s.as_str()).collect();
    DataFrame::new(vec![
        Series::new("host", &host_refs),
        Series::new("user", &user_refs),
    ])
    .expect("failed to build frame")
}

fn normalized(entries: &[IocEntry]) -> Vec<(String, String, String)> {
    entries
        .iter()
        .map(|e| {
            (
                normalize_flag_value(&e.flag),
                e.tag.trim().to_string(),
                e.query.trim().to_string(),
            )
        })
        .collect()
}

fn signature(entries: &[IocEntry]) -> String {
    let mut sorted = normalized(entries);
    sorted.sort();
    serde_json::to_string(&sorted).expect("failed to serialize signature")
}

fn temp_root() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trivium-ioc-inc-{}", Uuid::new_v4()));
    fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn write_csv(root: &Path) -> PathBuf {
    let path = root.join("incremental.csv");
    let mut body = String::from("host,user\n");
    for i in 0..ROWS {
        let user = if i < 15 { "alice" } else { "bob" };
        body.push_str(&format!("WS{:02},{}\n", i % 5, user));
    }
    fs::write(&path, body).expect("failed to write fixture");
    path
}

/// Creates a real project, returns its cache directory.
fn setup_dir() -> (PathBuf, PathBuf) {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_csv(&root);
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
    let project_dir = store.project_dir(&summary.meta.id);
    (root, project_dir)
}

fn assert_matches_full(project_dir: &Path, df: &DataFrame, all: &[IocEntry]) -> Vec<(String, String)> {
    let incremental = cached_ioc_outcomes(project_dir, df, all);
    let full = evaluate_iocs(df, all);
    assert_eq!(incremental, full, "incremental result differs from full");

    let cache = load_ioc_flag_cache(project_dir)
        .expect("failed to load cache")
        .expect("cache missing after evaluation");
    assert_eq!(cache.signature, signature(all), "cache signature mismatch");
    assert_eq!(cache.entries, normalized(all), "cache entries mismatch");

    incremental
        .into_iter()
        .map(|o| (o.flag, o.tags.join(",")))
        .collect()
}

/// Seeds the cache with `old`, then appends `added` and checks the result.
fn run_append_case(old: Vec<IocEntry>, added: Vec<IocEntry>) {
    let (root, project_dir) = setup_dir();
    let df = frame(ROWS);

    let seeded = cached_ioc_outcomes(&project_dir, &df, &old);
    assert_eq!(seeded, evaluate_iocs(&df, &old), "seed evaluation differs");

    let mut all = old;
    all.extend(added);
    let result = assert_matches_full(&project_dir, &df, &all);

    // On rows matched by both sets, the merged tags must contain both in full
    // evaluation order. Rows 1, 6, 11 are alice + WS01.
    for row in [1usize, 6, 11] {
        assert!(
            result[row].1.contains('['),
            "expected merged tags on row {row}: {:?}",
            result[row]
        );
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn append_lower_severity_merges_exactly() {
    run_append_case(
        vec![entry("critical", "crit", "user:alice")],
        vec![entry("suspicious", "susp", "host:WS01")],
    );
}

#[test]
fn append_higher_severity_merges_exactly() {
    run_append_case(
        vec![entry("suspicious", "susp", "user:alice")],
        vec![entry("critical", "crit", "host:WS01")],
    );
}

#[test]
fn append_same_severity_keeps_definition_order() {
    let (root, project_dir) = setup_dir();
    let df = frame(ROWS);

    let old = vec![entry("suspicious", "first", "user:alice")];
    cached_ioc_outcomes(&project_dir, &df, &old);
    let all = vec![
        entry("suspicious", "first", "user:alice"),
        entry("suspicious", "second", "host:WS01"),
    ];
    let result = assert_matches_full(&project_dir, &df, &all);

    // Row 1: old entry comes first in definition order, so [first] then
    // [second]; a full evaluation produces the same sequence.
    assert_eq!(result[1].0, "suspicious");
    assert_eq!(result[1].1, "[first],[second]");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn append_multiple_entries_merge_exactly() {
    run_append_case(
        vec![entry("safe", "safe-a", "user:alice")],
        vec![
            entry("critical", "crit", "host:WS01"),
            entry("suspicious", "susp", "host:WS01"),
            entry("safe", "safe-b", "user:alice"),
        ],
    );
}

#[test]
fn non_append_changes_fall_back_to_full_evaluation() {
    let (root, project_dir) = setup_dir();
    let df = frame(ROWS);

    let a = entry("critical", "crit", "user:alice");
    let b = entry("suspicious", "susp", "host:WS01");
    let c = entry("safe", "safe", "host:WS02");
    cached_ioc_outcomes(&project_dir, &df, &[a.clone(), b.clone()]);

    // Deletion of the first entry is not an append.
    assert_matches_full(&project_dir, &df, std::slice::from_ref(&b));
    // Insertion at the front is not an append either.
    assert_matches_full(&project_dir, &df, &[c.clone(), a.clone(), b.clone()]);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn first_call_without_cache_matches_full_evaluation() {
    let (root, project_dir) = setup_dir();
    let df = frame(ROWS);
    let all = vec![
        entry("critical", "crit", "user:alice"),
        entry("suspicious", "susp", "host:WS01"),
    ];

    assert_matches_full(&project_dir, &df, &all);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn different_row_count_forces_full_evaluation() {
    let (root, project_dir) = setup_dir();
    let df = frame(ROWS);
    let all = vec![
        entry("critical", "crit", "user:alice"),
        entry("suspicious", "susp", "host:WS01"),
    ];
    cached_ioc_outcomes(&project_dir, &df, &all);

    let small = frame(10);
    assert_matches_full(&project_dir, &small, &all);

    let _ = fs::remove_dir_all(root);
}

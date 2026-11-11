//! T19: repeated page requests reuse resolved row ids.
//!
//! The tests serialize on one lock because the result cache is process-local;
//! each test starts from an empty cache and resets the resolution counter.

use std::fs;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use trivium_engine::ops;
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::rows::{
    clear_result_cache, query_project_rows, reset_resolve_count, resolve_count,
    result_cache_stats, QueryRowsPayload, QueryRowsResponse, QuerySpec,
};
use trivium_engine::store::ProjectsStore;

mod common;

const ROWS: usize = 60;
const PAGE: usize = 20;

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Serializes this test binary and starts every test with an empty cache.
fn cache_test_guard() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    clear_result_cache();
    reset_resolve_count();
    guard
}

fn setup_project() -> (std::path::PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-query-cache-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let csv = root.join("cache.csv");
    let mut body = String::from("id,msg\n");
    for index in 0..ROWS {
        let msg = match index {
            3 => "special-a".to_string(),
            50 => "special-b".to_string(),
            _ => format!("value{index:02}"),
        };
        body.push_str(&format!("r{index:02},{msg}\n"));
    }
    fs::write(&csv, body).expect("failed to write fixture");

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

    (root, store, id)
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

fn row_ids(response: &QueryRowsResponse) -> Vec<usize> {
    response.rows.iter().map(|row| row.row_index).collect()
}

fn update(
    store: &ProjectsStore,
    id: Uuid,
    row_index: usize,
    flag: Option<&str>,
    memo: Option<&str>,
    bookmarked: Option<bool>,
) {
    ops::flags::update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index,
            flag: flag.map(str::to_string),
            memo: memo.map(|text| Some(text.to_string())),
            tags: None,
            bookmarked,
        },
    )
    .expect("failed to update annotation");
}

#[test]
fn page_moves_reuse_one_search_and_sort() {
    let _guard = cache_test_guard();
    let (root, store, id) = setup_project();
    let ids = common::column_ids(&store, id);
    let spec = QuerySpec {
        sort_key: Some(common::id(&ids, "id")),
        sort_direction: Some("asc".to_string()),
        ..Default::default()
    };

    let mut seen = Vec::new();
    for offset in [0, PAGE, PAGE * 2] {
        let response = query(&store, id, spec.clone(), offset, PAGE);
        assert_eq!(response.total_filtered_rows, ROWS);
        let page = row_ids(&response);
        assert_eq!(page.len(), PAGE);
        seen.extend(page);
    }

    assert_eq!(resolve_count(), 1, "search/sort must run only once");
    assert_eq!(seen, (0..ROWS).collect::<Vec<_>>());
    let stats = result_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 1);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn changed_condition_returns_new_ids_without_mixing() {
    let _guard = cache_test_guard();
    let (root, store, id) = setup_project();

    let first = query(
        &store,
        id,
        QuerySpec {
            search: Some("special-a".to_string()),
            ..Default::default()
        },
        0,
        PAGE,
    );
    assert_eq!(row_ids(&first), vec![3]);

    let second = query(
        &store,
        id,
        QuerySpec {
            search: Some("special-b".to_string()),
            ..Default::default()
        },
        0,
        PAGE,
    );
    assert_eq!(row_ids(&second), vec![50]);
    assert_eq!(resolve_count(), 2);

    let again = query(
        &store,
        id,
        QuerySpec {
            search: Some("special-a".to_string()),
            ..Default::default()
        },
        0,
        PAGE,
    );
    assert_eq!(row_ids(&again), vec![3]);
    assert_eq!(resolve_count(), 2, "the first result is still cached");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn annotation_filters_miss_after_an_annotation_update() {
    let _guard = cache_test_guard();
    let (root, store, id) = setup_project();

    let priority = QuerySpec {
        flag_filter: Some("priority".to_string()),
        ..Default::default()
    };
    let before = query(&store, id, priority.clone(), 0, PAGE);
    assert_eq!(before.total_filtered_rows, 0);
    assert_eq!(resolve_count(), 1);

    update(&store, id, 1, Some("critical"), None, None);
    let after = query(&store, id, priority, 0, PAGE);
    assert_eq!(row_ids(&after), vec![1]);
    assert_eq!(resolve_count(), 2);

    let bookmarked = QuerySpec {
        bookmarked_only: Some(true),
        ..Default::default()
    };
    let before = query(&store, id, bookmarked.clone(), 0, PAGE);
    assert_eq!(before.total_filtered_rows, 0);
    assert_eq!(resolve_count(), 3);

    update(&store, id, 2, None, None, Some(true));
    let after = query(&store, id, bookmarked, 0, PAGE);
    assert_eq!(row_ids(&after), vec![2]);
    assert_eq!(resolve_count(), 4);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn memo_and_flag_updates_keep_a_data_only_result_cached() {
    let _guard = cache_test_guard();
    let (root, store, id) = setup_project();
    let ids = common::column_ids(&store, id);
    // T20a reads an unconditional page without resolving ids, so this T19
    // regression uses a sorted data-only spec to exercise the cache.
    let spec = QuerySpec {
        sort_key: Some(common::id(&ids, "id")),
        sort_direction: Some("asc".to_string()),
        ..Default::default()
    };

    let first = query(&store, id, spec.clone(), 0, PAGE);
    assert_eq!(first.annotated_total, 0);
    assert_eq!(resolve_count(), 1);

    update(&store, id, 0, None, Some("reviewed"), None);
    let second = query(&store, id, spec.clone(), 0, PAGE);
    assert_eq!(resolve_count(), 1, "a memo update must not re-resolve ids");
    assert_eq!(second.rows[0].memo.as_deref(), Some("reviewed"));

    update(&store, id, 5, Some("suspicious"), None, None);
    let third = query(&store, id, spec, 0, PAGE);
    assert_eq!(
        resolve_count(),
        1,
        "a flag update must not re-resolve a data-only result"
    );
    assert_eq!(third.annotated_total, 1);
    assert_eq!(third.annotated_filtered, 1);
    assert_eq!(third.rows[5].flag, "suspicious");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn cache_keeps_two_results_and_evicts_the_oldest() {
    let _guard = cache_test_guard();
    let (root, store, id) = setup_project();

    for value in ["value00", "value01", "value02"] {
        query(
            &store,
            id,
            QuerySpec {
                search: Some(value.to_string()),
                ..Default::default()
            },
            0,
            PAGE,
        );
    }

    let stats = result_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.max_entries, 2);
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.total_bytes, 2 * (std::mem::size_of::<usize>() + 64));
    assert_eq!(resolve_count(), 3);

    let first_again = query(
        &store,
        id,
        QuerySpec {
            search: Some("value00".to_string()),
            ..Default::default()
        },
        0,
        PAGE,
    );
    assert_eq!(row_ids(&first_again), vec![0]);
    assert_eq!(resolve_count(), 4, "the evicted result is a miss");
    assert_eq!(result_cache_stats().entries, 2);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn deleting_a_project_drops_its_cached_results() {
    let _guard = cache_test_guard();
    let (root, store, id) = setup_project();
    let ids = common::column_ids(&store, id);

    query(
        &store,
        id,
        QuerySpec {
            sort_key: Some(common::id(&ids, "id")),
            sort_direction: Some("asc".to_string()),
            ..Default::default()
        },
        0,
        PAGE,
    );
    assert_eq!(result_cache_stats().entries, 1);

    ops::projects::delete_project(&store, ProjectRequest { project_id: id })
        .expect("failed to delete project");
    assert_eq!(result_cache_stats().entries, 0);
    assert_eq!(result_cache_stats().total_bytes, 0);

    let _ = fs::remove_dir_all(root);
}

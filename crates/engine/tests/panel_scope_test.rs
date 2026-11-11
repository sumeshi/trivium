//! T25: auxiliary analysis uses the applied QuerySpec.
//!
//! Column stats, histograms, and time analysis must not silently ignore the
//! bookmark-only or explicit search-column scope shown by the table.

use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use trivium_engine::ops;
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::histogram::HistogramPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::QuerySpec;
use trivium_engine::ops::stats::ColumnStatsPayload;
use trivium_engine::ops::time::TimeAnalysisPayload;
use trivium_engine::store::ProjectsStore;

mod common;

fn setup() -> (PathBuf, ProjectsStore, Uuid, std::collections::HashMap<String, String>) {
    let root = std::env::temp_dir().join(format!("trivium-panel-scope-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let path = root.join("panels.csv");
    let body = "id,group,ts\n\
                row0,alpha,2024-01-01T00:00:01\n\
                row1,alpha,2024-01-01T00:00:02\n\
                row2,beta,2024-01-01T00:00:03\n\
                row3,beta,2024-01-01T00:00:04\n\
                row4,alpha,2024-01-01T00:00:05\n\
                row5,beta,2024-01-01T00:00:06\n";
    fs::write(&path, body).expect("failed to write fixture");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = ops::projects::create_project(
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
    .id;
    let ids = common::column_ids(&store, id);
    (root, store, id, ids)
}

fn bookmark(store: &ProjectsStore, id: Uuid, row_index: usize) {
    update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index,
            flag: None,
            memo: None,
            tags: None,
            bookmarked: Some(true),
        },
    )
    .expect("bookmark failed");
}

#[test]
fn stats_histogram_and_time_follow_bookmark_and_search_scope() {
    let (root, store, id, ids) = setup();
    let id_column = common::id(&ids, "id");
    let group_column = common::id(&ids, "group");
    let ts_column = common::id(&ids, "ts");

    bookmark(&store, id, 1);
    bookmark(&store, id, 4);

    let bookmarked = QuerySpec {
        bookmarked_only: Some(true),
        ..Default::default()
    };
    let stats = ops::stats::column_stats(
        &store,
        ColumnStatsPayload {
            project_id: id,
            column: id_column.clone(),
            rare_max: 1,
            spec: bookmarked.clone(),
        },
    )
    .expect("stats");
    assert_eq!(stats.unique, 2, "stats must cover only bookmarked rows");

    let histogram = ops::histogram::histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: ts_column.clone(),
            buckets: 6,
            search: None,
            columns: None,
            search_columns: None,
            flag_filter: None,
            bookmarked_only: Some(true),
            condition: None,
            range: None,
        },
    )
    .expect("histogram");
    assert_eq!(
        histogram.buckets.iter().map(|bucket| bucket.count).sum::<u64>(),
        2,
        "histogram must cover only bookmarked rows"
    );

    let time = ops::time::time_analysis(
        &store,
        TimeAnalysisPayload {
            project_id: id,
            column: ts_column,
            gap_seconds: 1,
            session_minutes: 30,
            burst_window_seconds: 60,
            burst_min: 10,
            search: None,
            columns: None,
            search_columns: None,
            flag_filter: None,
            bookmarked_only: Some(true),
            condition: None,
        },
    )
    .expect("time analysis");
    let session_rows: u64 = time.sessions.iter().map(|span| span.count).sum();
    assert_eq!(session_rows, 2, "time analysis must cover only bookmarked rows");

    // A search scoped to a column that does not contain the term yields no
    // rows, rather than falling back to the visible-column list.
    let scoped = QuerySpec {
        search: Some("alpha".to_string()),
        search_columns: Some(vec![id_column]),
        ..Default::default()
    };
    let scoped_stats = ops::stats::column_stats(
        &store,
        ColumnStatsPayload {
            project_id: id,
            column: group_column,
            rare_max: 1,
            spec: scoped,
        },
    )
    .expect("scoped stats");
    assert_eq!(scoped_stats.unique, 0);

    let _ = fs::remove_dir_all(root);
}

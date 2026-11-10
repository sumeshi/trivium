//! F26 numeric and nanosecond precision across sort, search, and export.

use std::fs;
use std::path::{Path, PathBuf};

use polars::prelude::{AnyValue, TimeUnit};
use uuid::Uuid;

use trivium_engine::ops::export::ExportProjectPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{QueryRowsPayload, QuerySpec};
use trivium_engine::ops::{export, projects, rows};
use trivium_engine::store::ProjectsStore;
use trivium_engine::value_utils::anyvalue_to_search_string;

mod common;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-precision-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn write_bigint_fixture(root: &Path) -> PathBuf {
    // Deliberately stored in descending order: an f64 sort ties the two huge
    // values and keeps their original (wrong) order, an i64 sort must fix it.
    let path = root.join("bigint.csv");
    let text = "ts,eventid\n\
                2024-01-01T00:00:01,9223372036854775807\n\
                2024-01-01T00:00:02,9223372036854775806\n\
                2024-01-01T00:00:03,5\n";
    fs::write(&path, text).expect("failed to write fixture");
    path
}

#[test]
fn sort_search_and_export_keep_i64_max_precision() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_bigint_fixture(&root);

    let summary = projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project");
    let id = summary.meta.id;
    let ids = common::column_ids(&store, id);
    let event_id = common::id(&ids, "eventid");

    let response = rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: None,
                search_columns: None,
                flag_filter: None,
                bookmarked_only: None,
                sort_key: Some(event_id.clone()),
                sort_direction: Some("asc".to_string()),
                condition: None,
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .expect("sorted query failed");
    let sorted_values: Vec<String> = response
        .rows
        .iter()
        .map(|row| row.data[&event_id].to_string())
        .collect();
    assert_eq!(
        sorted_values,
        vec!["5", "9223372036854775806", "9223372036854775807"],
        "ascending sort must compare i64 exactly near i64::MAX"
    );

    let hits = rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: Some("9223372036854775807".to_string()),
                search_columns: None,
                flag_filter: None,
                bookmarked_only: None,
                sort_key: None,
                sort_direction: None,
                condition: None,
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .expect("search query failed");
    assert_eq!(
        hits.total_filtered_rows, 1,
        "search must match the exact i64::MAX digits"
    );

    let destination = root.join("export.csv");
    export::export_project(
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
        },
    )
    .expect("export failed");
    let exported = fs::read_to_string(&destination).expect("failed to read export");
    assert!(
        exported.contains("9223372036854775807"),
        "export must keep i64::MAX digits"
    );
    assert!(
        exported.contains("9223372036854775806"),
        "export must keep the neighbor digits"
    );
    assert!(
        !exported.contains("9223372036854776000"),
        "export must not contain the f64-rounded value"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn nanosecond_datetime_search_string_keeps_full_precision() {
    let ns = AnyValue::Datetime(1700000000123456789i64, TimeUnit::Nanoseconds, &None);
    let text = anyvalue_to_search_string(&ns).expect("datetime must stringify");
    assert!(
        text.ends_with(".123456789"),
        "nanosecond fraction must survive stringification, got {}",
        text
    );

    let micros = AnyValue::Datetime(1700000000123456i64, TimeUnit::Microseconds, &None);
    let text = anyvalue_to_search_string(&micros).expect("datetime must stringify");
    assert!(
        text.ends_with(".123456"),
        "microsecond fraction must survive stringification, got {}",
        text
    );
}

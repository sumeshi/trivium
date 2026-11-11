use std::collections::HashMap;
use std::fs;

use polars::prelude::*;
use uuid::Uuid;

use trivium_engine::analysis::histogram::{histogram_series_annotated, Histogram};
use trivium_engine::models::FlagEntry;
use trivium_engine::ops::{
    flags::{update_flag, UpdateFlagPayload},
    histogram::{histogram, HistogramPayload},
    projects::{create_project, CreateProjectPayload},
};
use trivium_engine::project_io::{project_data_reads, reset_project_read_counters};
use trivium_engine::store::ProjectsStore;

mod common;

fn flag(value: &str) -> FlagEntry {
    FlagEntry {
        flag: value.into(),
        memo: None,
        tags: vec![],
        bookmarked: false,
    }
}

fn assert_counts(result: &Histogram, total: u64) {
    for bucket in &result.buckets {
        assert_eq!(
            bucket.flags.safe
                + bucket.flags.suspicious
                + bucket.flags.critical
                + bucket.flags.unflagged,
            bucket.count
        );
    }
    assert_eq!(
        result
            .buckets
            .iter()
            .map(|bucket| bucket.count)
            .sum::<u64>()
            + result.missing
            + result.outside,
        total
    );
}

#[test]
fn legacy_flag_symbols_case_and_unknown_entries_match_query_normalization() {
    let flags = [
        "◯",
        " SAFE ",
        "?",
        "Suspicious",
        "✗",
        "CRITICAL",
        "",
        "unknown",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, value)| (index, flag(value)))
    .collect();
    let series = Series::new("ts", &["2024-01-01T00:00:00.000000001"; 8]);
    let result = histogram_series_annotated(
        "ts",
        &series,
        &(0..8).collect::<Vec<_>>(),
        1,
        None,
        &flags,
        false,
    )
    .unwrap();
    let counts = &result.buckets[0].flags;
    assert_eq!(
        (
            counts.safe,
            counts.suspicious,
            counts.critical,
            counts.unflagged
        ),
        (2, 2, 2, 2)
    );
    assert_counts(&result, 8);
}

#[test]
fn rfc3339_offsets_share_exact_nanosecond_buckets_without_changing_source_text() {
    let text = [
        "2024-01-01T00:00:00.000000001Z",
        "2024-01-01T09:00:00.000000002+09:00",
        "2023-12-31T19:00:00.000000003-05:00",
    ];
    let series = Series::new("ts", &text);
    let result =
        histogram_series_annotated("ts", &series, &[0, 1, 2], 3, None, &HashMap::new(), false)
            .unwrap();
    assert_eq!(result.kind, "datetime");
    assert_eq!(
        result.full_range.as_ref().unwrap().min,
        "2024-01-01T00:00:00.000000001"
    );
    assert_eq!(
        result.full_range.as_ref().unwrap().max,
        "2024-01-01T00:00:00.000000003"
    );
    assert_counts(&result, 3);
    assert_eq!(series.str().unwrap().get(1), Some(text[1]));
}

#[test]
fn nanosecond_buckets_have_exact_nonoverlapping_bounds_and_manual_flag_counts() {
    let series = Series::new(
        "ts",
        &[
            Some("2024-01-01T00:00:00.000000001"),
            Some("2024-01-01T00:00:00.000000002"),
            Some("2024-01-01T00:00:00.000000003"),
            Some("2024-01-01T00:00:00.000000004"),
            None,
        ],
    );
    let flags = [
        (0, flag("safe")),
        (1, flag("suspicious")),
        (2, flag("critical")),
    ]
    .into_iter()
    .collect();
    let result =
        histogram_series_annotated("ts", &series, &[0, 1, 2, 3, 4], 2, None, &flags, true).unwrap();
    assert_counts(&result, 5);
    assert!(!result.auto_focused);
    assert_eq!(result.missing, 1);
    assert_eq!(result.buckets[0].end, "2024-01-01T00:00:00.000000003");
    assert_eq!(result.buckets[1].start, result.buckets[0].end);
    assert!(!result.buckets[0].end_inclusive);
    assert!(result.buckets[1].end_inclusive);
    assert_eq!((result.buckets[0].count, result.buckets[1].count), (2, 2));
    assert_eq!(
        (
            result.buckets[0].flags.safe,
            result.buckets[0].flags.suspicious
        ),
        (1, 1)
    );
    assert_eq!(
        (
            result.buckets[1].flags.critical,
            result.buckets[1].flags.unflagged
        ),
        (1, 1)
    );
}

#[test]
fn dense_initial_period_excludes_ancient_outlier_and_show_all_restores_every_event() {
    let mut values = vec!["2000-01-01T00:00:00.000000001".to_string()];
    values.extend((0..100).map(|index| format!("2024-01-01T00:00:00.{index:09}")));
    let series = Series::new("ts", values);
    let rows = (0..101).collect::<Vec<_>>();
    let flags = HashMap::new();
    let focused = histogram_series_annotated("ts", &series, &rows, 60, None, &flags, true).unwrap();
    assert!(focused.auto_focused);
    assert_eq!(focused.outside, 1);
    assert_counts(&focused, 101);
    let full = focused.full_range.as_ref().unwrap();
    assert_eq!(full.min, "2000-01-01T00:00:00.000000001");
    assert_eq!(full.max, "2024-01-01T00:00:00.000000099");
    let show_all =
        histogram_series_annotated("ts", &series, &rows, 60, None, &flags, false).unwrap();
    assert!(!show_all.auto_focused);
    assert_eq!(show_all.outside, 0);
    assert_counts(&show_all, 101);
    let explicit = histogram_series_annotated(
        "ts",
        &series,
        &rows,
        60,
        Some((&full.min, &full.max)),
        &flags,
        true,
    )
    .unwrap();
    assert!(!explicit.auto_focused);
    assert_eq!(explicit.outside, 0);
    assert_eq!(explicit.buckets, show_all.buckets);
}

#[test]
fn large_sorted_input_uses_repeatable_bounded_sample_without_losing_counts() {
    let values = (0..20_000)
        .map(|index| {
            if index < 200 {
                format!("2000-01-01T00:00:00.{index:09}")
            } else {
                format!("2024-01-01T00:00:00.{index:09}")
            }
        })
        .collect::<Vec<_>>();
    let series = Series::new("ts", values);
    let rows = (0..20_000).collect::<Vec<_>>();
    let flags = HashMap::new();
    let first = histogram_series_annotated("ts", &series, &rows, 60, None, &flags, true).unwrap();
    let second = histogram_series_annotated("ts", &series, &rows, 60, None, &flags, true).unwrap();
    assert_eq!(first, second);
    assert!(first.auto_focused);
    assert!(first.outside >= 200 && first.outside < 1000);
    assert_counts(&first, 20_000);
}

#[test]
fn uniform_period_is_not_arbitrarily_zoomed_and_numeric_flags_are_accurate() {
    let values = (0..100)
        .map(|index| format!("2024-01-01T00:00:{:02}.{:09}", index / 2, index % 2))
        .collect::<Vec<_>>();
    let rows = (0..100).collect::<Vec<_>>();
    let result = histogram_series_annotated(
        "ts",
        &Series::new("ts", values),
        &rows,
        10,
        None,
        &HashMap::new(),
        true,
    )
    .unwrap();
    assert!(!result.auto_focused);
    assert_eq!(result.outside, 0);
    assert_counts(&result, 100);
    let flags = [(1, flag("safe")), (2, flag("critical"))]
        .into_iter()
        .collect();
    for series in [
        Series::new("v", &[0i64, 1, 2, 3]),
        Series::new("v", &[0.1f64, 0.2, 0.3, 0.4]),
    ] {
        let result =
            histogram_series_annotated("v", &series, &[0, 1, 2, 3], 2, None, &flags, false)
                .unwrap();
        assert_counts(&result, 4);
        assert_eq!(
            result
                .buckets
                .iter()
                .map(|bucket| bucket.flags.unflagged)
                .sum::<u64>(),
            2
        );
        assert_eq!(
            result
                .buckets
                .iter()
                .map(|bucket| bucket.flags.safe)
                .sum::<u64>(),
            1
        );
        assert_eq!(
            result
                .buckets
                .iter()
                .map(|bucket| bucket.flags.critical)
                .sum::<u64>(),
            1
        );
    }
}

#[test]
fn operation_respects_search_multiselect_and_bookmark_scope_in_one_source_read() {
    let root = std::env::temp_dir().join(format!("trivium-timeline-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("timeline.csv");
    fs::write(&source, "ts,cohort\n2024-01-01T00:00:00.000000001,keep\n2024-01-01T00:00:00.000000002,keep\n2024-01-01T00:00:00.000000003,keep\n2024-01-01T00:00:00.000000004,drop\n").unwrap();
    let store = ProjectsStore::new(root.clone()).unwrap();
    let id = create_project(
        &store,
        CreateProjectPayload {
            path: source.to_string_lossy().into_owned(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap()
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    for (row_index, value) in [
        (0, "safe"),
        (1, "suspicious"),
        (2, "critical"),
        (3, "critical"),
    ] {
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index,
                flag: Some(value.into()),
                memo: None,
                tags: None,
                bookmarked: Some(row_index != 0),
            },
        )
        .unwrap();
    }
    reset_project_read_counters();
    let result = histogram(
        &store,
        HistogramPayload {
            project_id: id,
            column: common::id(&ids, "ts"),
            buckets: 2,
            search: Some("keep".into()),
            columns: None,
            search_columns: None,
            flag_filter: Some("safe|critical".into()),
            bookmarked_only: Some(true),
            condition: None,
            range: None,
            focus: true,
        },
    )
    .unwrap();
    assert_eq!(
        project_data_reads(),
        1,
        "initial focus must not reread data"
    );
    assert_counts(&result, 1);
    assert_eq!(
        result
            .buckets
            .iter()
            .map(|bucket| bucket.flags.critical)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        result
            .buckets
            .iter()
            .map(|bucket| bucket.flags.safe)
            .sum::<u64>(),
        0
    );
    drop(store);
    let _ = fs::remove_dir_all(root);
}

use std::{collections::HashMap, fs};

use polars::prelude::*;
use serde_json::json;
use trivium_engine::{
    analysis::histogram::{
        histogram_series, histogram_series_annotated, histogram_series_ranged,
        MAX_HISTOGRAM_BUCKETS,
    },
    ops,
    store::ProjectsStore,
};
use uuid::Uuid;

#[test]
fn public_histogram_entries_reject_excess_buckets_for_numeric_and_datetime_columns() {
    let too_many = MAX_HISTOGRAM_BUCKETS + 1;
    for series in [
        Series::new("n".into(), &[1_i64, 2]),
        Series::new("n".into(), &[1.5_f64, 2.5]),
        Series::new(
            "ts".into(),
            &["2026-11-18T00:00:00Z", "2026-11-18T00:00:01Z"],
        ),
    ] {
        for result in [
            histogram_series(series.name(), &series, &[0, 1], too_many),
            histogram_series_ranged(series.name(), &series, &[0, 1], too_many, None),
            histogram_series_annotated(
                series.name(),
                &series,
                &[0, 1],
                too_many,
                None,
                &HashMap::new(),
                false,
            ),
        ] {
            let error = result.expect_err("excess buckets must be rejected, not clamped");
            assert!(
                error.contains(&MAX_HISTOGRAM_BUCKETS.to_string()),
                "{error}"
            );
        }
    }
}

#[test]
fn accepted_library_bucket_counts_keep_counts_and_zero_means_one_bucket() {
    for series in [
        Series::new("n".into(), &[1_i64, 2]),
        Series::new(
            "ts".into(),
            &["2026-11-18T00:00:00Z", "2026-11-18T00:00:01Z"],
        ),
    ] {
        for buckets in [0, 1, 200, MAX_HISTOGRAM_BUCKETS] {
            let histogram = histogram_series_annotated(
                series.name(),
                &series,
                &[0, 1],
                buckets,
                None,
                &HashMap::new(),
                false,
            )
            .unwrap();
            assert_eq!(histogram.buckets.len(), buckets.max(1));
            assert_eq!(
                histogram
                    .buckets
                    .iter()
                    .map(|bucket| bucket.count)
                    .sum::<u64>(),
                2
            );
            assert_eq!(
                histogram
                    .buckets
                    .iter()
                    .map(|bucket| bucket.flags.unflagged)
                    .sum::<u64>(),
                2
            );
        }
    }
}

#[test]
fn operation_rejects_excess_buckets_before_project_or_query_resolution() {
    let root = std::env::temp_dir().join(format!("trivium-histogram-limit-{}", Uuid::new_v4()));
    let store = ProjectsStore::new(root.clone()).unwrap();
    let payload = serde_json::from_value(json!({
        "projectId": Uuid::new_v4(), "column": "missing",
        "search": "|", "buckets": MAX_HISTOGRAM_BUCKETS + 1,
    }))
    .unwrap();
    let error = ops::histogram::histogram(&store, payload)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(&MAX_HISTOGRAM_BUCKETS.to_string()),
        "{error}"
    );
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

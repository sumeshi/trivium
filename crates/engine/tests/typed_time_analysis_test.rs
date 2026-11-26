//! Native temporal columns retain explicit units instead of becoming epoch-like text.
use std::{collections::HashMap, fs};

use chrono::Utc;
use polars::prelude::{DataFrame, DataType, NamedFrom, Series, TimeUnit};
use serde_json::json;
use trivium_engine::{
    analysis::{analyze_series, histogram::histogram_series_annotated},
    models::ProjectMeta,
    ops,
    project_io::{read_project_dataframe, write_project_dataframe},
    store::ProjectsStore,
};
use uuid::Uuid;

fn timestamp_series(unit: TimeUnit) -> Series {
    let base_seconds = 1_794_625_445_i64; // 2026-11-14T03:04:05Z.
    let scale = match unit {
        TimeUnit::Nanoseconds => 1_000_000_000,
        TimeUnit::Microseconds => 1_000_000,
        TimeUnit::Milliseconds => 1_000,
    };
    Series::new(
        "Timestamp",
        [
            Some((base_seconds + 120) * scale),
            None,
            Some(base_seconds * scale),
            Some((base_seconds + 60) * scale),
        ],
    )
    .cast(&DataType::Datetime(unit, None))
    .unwrap()
}

fn date_series() -> Series {
    Series::new(
        "Timestamp",
        [Some(20_773_i32), None, Some(20_771), Some(20_772)],
    )
    .cast(&DataType::Date)
    .unwrap()
}

fn check_counts(series: &Series) {
    let rows = [0, 1, 2, 3];
    let analysis = analyze_series("Timestamp", series, &rows, 1, 1, 120, 2).unwrap();
    let histogram =
        histogram_series_annotated("Timestamp", series, &rows, 3, None, &HashMap::new(), false)
            .unwrap();
    assert_eq!((analysis.missing, analysis.invalid), (1, 0));
    assert_eq!((histogram.missing, histogram.invalid), (1, 0));
    assert_eq!(analysis.sessions.len(), 3);
    assert!(analysis.sessions.iter().all(|span| span.count == 1));
    assert_eq!(analysis.gaps.len(), 2);
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|bucket| bucket.count)
            .sum::<u64>(),
        3
    );
    if !matches!(series.dtype(), DataType::Date) {
        assert_eq!(
            analysis
                .gaps
                .iter()
                .map(|span| span.count)
                .collect::<Vec<_>>(),
            [60, 60]
        );
        assert_eq!(analysis.sessions[0].start, "2026-11-14T03:04:05Z");
        assert_eq!(analysis.sessions[2].start, "2026-11-14T03:06:05Z");
        assert_eq!(analysis.bursts.len(), 1);
        assert_eq!(analysis.bursts[0].count, 3);
    } else {
        assert_eq!(
            analysis
                .gaps
                .iter()
                .map(|span| span.count)
                .collect::<Vec<_>>(),
            [86_400, 86_400]
        );
    }
}

#[test]
fn native_datetime_units_and_date_share_histogram_and_time_analysis_points() {
    for unit in [
        TimeUnit::Nanoseconds,
        TimeUnit::Microseconds,
        TimeUnit::Milliseconds,
    ] {
        check_counts(&timestamp_series(unit));
    }
    check_counts(&date_series());
}

#[test]
fn native_nanosecond_ordering_does_not_round_through_seconds_or_floats() {
    let raw = [Some(1_i64), Some(-1), Some(0), None];
    let series = Series::new("Timestamp", raw)
        .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
        .unwrap();
    let result = analyze_series("Timestamp", &series, &[0, 1, 2, 3], 0, 0, 0, 10).unwrap();
    assert_eq!(
        result
            .sessions
            .iter()
            .map(|span| span.start.as_str())
            .collect::<Vec<_>>(),
        [
            "1969-12-31T23:59:59.999999999Z",
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000000001Z",
        ]
    );
    assert_eq!(
        series
            .cast(&DataType::Int64)
            .unwrap()
            .i64()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        raw
    );
}

#[test]
fn calendarless_time_and_out_of_range_native_values_are_invalid_without_formatting() {
    let time = Series::new("Timestamp", [Some(0_i64), Some(1), None])
        .cast(&DataType::Time)
        .unwrap();
    let result = analyze_series("Timestamp", &time, &[0, 1, 2], 1, 1, 1, 1).unwrap();
    let histogram = histogram_series_annotated(
        "Timestamp",
        &time,
        &[0, 1, 2],
        3,
        None,
        &HashMap::new(),
        false,
    )
    .unwrap();
    assert_eq!((result.missing, result.invalid), (1, 2));
    assert_eq!((histogram.missing, histogram.invalid), (1, 2));
    assert!(result.sessions.is_empty() && result.gaps.is_empty() && result.bursts.is_empty());
    for unit in [TimeUnit::Microseconds, TimeUnit::Milliseconds] {
        let series = Series::new("Timestamp", [Some(i64::MAX), None])
            .cast(&DataType::Datetime(unit, None))
            .unwrap();
        let result = analyze_series("Timestamp", &series, &[0, 1], 1, 1, 1, 1).unwrap();
        assert_eq!((result.missing, result.invalid), (1, 1));
        assert!(result.sessions.is_empty());
    }
    let dates = Series::new("Timestamp", [Some(i32::MAX), None])
        .cast(&DataType::Date)
        .unwrap();
    let result = analyze_series("Timestamp", &dates, &[0, 1], 1, 1, 1, 1).unwrap();
    assert_eq!((result.missing, result.invalid), (1, 1));
}

fn saved_case(store: &ProjectsStore, mut series: Series) -> Uuid {
    series.rename("Timestamp");
    let id = Uuid::new_v4();
    let directory = store.project_dir(&id);
    fs::create_dir_all(&directory).unwrap();
    let mut frame = DataFrame::new(vec![series]).unwrap();
    write_project_dataframe(&directory.join("data.parquet"), &mut frame).unwrap();
    store
        .insert(ProjectMeta {
            id,
            name: "native temporal".into(),
            description: None,
            created_at: Utc::now(),
            total_records: frame.height(),
            flagged_records: 0,
            ioc_applied_records: 0,
            source_sha256: None,
            source_file_name: None,
            source_format: None,
            source_size: None,
            source_path: None,
            import_report: None,
            hidden_columns: vec![],
            storage_version: 0,
            columns: vec![],
        })
        .unwrap();
    id
}

#[test]
fn typed_parquet_analysis_survives_reopen_without_changing_stored_units() {
    let root = tempfile::tempdir().unwrap();
    let cases = root.path().join("cases");
    let store = ProjectsStore::new(cases.clone()).unwrap();
    let inputs = [
        timestamp_series(TimeUnit::Nanoseconds),
        timestamp_series(TimeUnit::Microseconds),
        timestamp_series(TimeUnit::Milliseconds),
        date_series(),
    ];
    let saved = inputs
        .into_iter()
        .map(|series| {
            let dtype = series.dtype().clone();
            let id = saved_case(&store, series);
            let bytes = fs::read(store.project_dir(&id).join("data.parquet")).unwrap();
            (id, dtype, bytes)
        })
        .collect::<Vec<_>>();
    drop(store);
    let store = ProjectsStore::new(cases).unwrap();
    for (id, dtype, before) in saved {
        let params = json!({"projectId":id,"column":"Timestamp","gapSeconds":1,"sessionMinutes":1,"burstWindowSeconds":120,"burstMin":2,"buckets":3});
        let analysis =
            ops::time::time_analysis(&store, serde_json::from_value(params.clone()).unwrap())
                .unwrap();
        let histogram =
            ops::histogram::histogram(&store, serde_json::from_value(params).unwrap()).unwrap();
        assert_eq!((analysis.missing, analysis.invalid), (1, 0));
        assert_eq!((histogram.missing, histogram.invalid), (1, 0));
        assert_eq!(
            analysis.sessions.iter().map(|span| span.count).sum::<u64>(),
            3
        );
        assert_eq!(
            histogram
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            3
        );
        let path = store.project_dir(&id).join("data.parquet");
        assert_eq!(
            read_project_dataframe(&path)
                .unwrap()
                .column("Timestamp")
                .unwrap()
                .dtype(),
            &dtype
        );
        assert_eq!(fs::read(path).unwrap(), before);
    }
}

#[test]
fn modern_source_strings_keep_their_spelling_and_numeric_text_is_not_an_epoch() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let raw = " 2026/11/14 12:4:5,123456789 +0900 ";
    let id = saved_case(
        &store,
        Series::new(
            "Timestamp",
            [Some(raw), Some("1700000000"), Some("not a date"), None],
        ),
    );
    let result = ops::time::time_analysis(
        &store,
        serde_json::from_value(json!({"projectId":id,"column":"Timestamp","asDatetime":true}))
            .unwrap(),
    )
    .unwrap();
    assert_eq!((result.missing, result.invalid), (1, 2));
    assert_eq!(result.sessions.len(), 1);
    assert_eq!(result.sessions[0].start, raw);
    assert_eq!(result.sessions[0].end, raw);
}

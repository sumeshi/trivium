use std::{collections::HashMap, fs};

use polars::prelude::*;
use serde_json::json;
use trivium_engine::{
    analysis::{
        histogram::{histogram_series_annotated, histogram_series_ranged},
        time::analyze_series,
    },
    condition::{Condition, Group, GroupKind, Node, Op},
    jobs::JobContext,
    models::FlagEntry,
    ops::{
        self,
        projects::CreateProjectPayload,
        rows::{resolve_target, QuerySpec},
    },
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

fn bounds(column: &str, start: &str, end: &str, inclusive: bool) -> Node {
    Node::Group(Group {
        kind: GroupKind::And,
        children: [
            (Op::Ge, start),
            (if inclusive { Op::Le } else { Op::Lt }, end),
        ]
        .into_iter()
        .map(|(op, value)| {
            Node::Condition(Condition {
                column: column.into(),
                op,
                values: vec![value.into()],
                negated: false,
            })
        })
        .collect(),
    })
}

/// Compare advertised intervals with the real query evaluator, including bins
/// narrower than one integer/nanosecond and the degenerate single-value case.
fn assert_histogram_intervals(series: Series, buckets: usize, range: Option<(&str, &str)>) {
    let rows: Vec<_> = (0..series.len()).collect();
    let frame = DataFrame::new_infer_height(vec![series.clone().into()]).unwrap();
    let columns = vec![series.name().to_string()];
    let flags: HashMap<_, _> = rows
        .iter()
        .map(|&index| {
            (
                index,
                FlagEntry {
                    flag: ["safe", "suspicious", "critical", ""][index % 4].into(),
                    memo: None,
                    tags: vec![],
                    bookmarked: false,
                },
            )
        })
        .collect();
    for histogram in [
        histogram_series_ranged(series.name(), &series, &rows, buckets, range).unwrap(),
        histogram_series_annotated(series.name(), &series, &rows, buckets, range, &flags, false)
            .unwrap(),
    ] {
        assert_eq!(
            histogram
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>()
                + histogram.missing
                + histogram.outside,
            rows.len() as u64
        );
        for bucket in histogram.buckets {
            let spec = QuerySpec {
                condition: Some(bounds(
                    series.name(),
                    &bucket.start,
                    &bucket.end,
                    bucket.end_inclusive,
                )),
                ..Default::default()
            };
            let target = resolve_target(
                &frame,
                &spec,
                &columns,
                &HashMap::new(),
                &JobContext::none(),
            )
            .unwrap();
            assert_eq!(
                target.filtered.len() as u64,
                bucket.count,
                "bucket: {bucket:?}"
            );
            assert_eq!(
                bucket.flags.safe
                    + bucket.flags.suspicious
                    + bucket.flags.critical
                    + bucket.flags.unflagged,
                bucket.count,
                "bucket: {bucket:?}"
            );
        }
    }
}

#[test]
fn integer_bucket_counts_match_query_bounds_without_overflow() {
    for (values, buckets, range) in [
        (vec![Some("0"), Some("1"), Some("2"), Some("3")], 2, None),
        (vec![Some("0"), Some("1")], 8, None),
        (vec![Some("7"), Some("7"), None], 5, None),
        (vec![Some("-8"), Some("-1"), Some("0"), Some("7")], 3, None),
        (
            vec![Some("0"), Some("1"), Some("2"), Some("3")],
            2,
            Some(("1", "2")),
        ),
        (vec![Some("0"), Some("1"), Some("2")], 3, Some(("1", "1"))),
    ] {
        assert_histogram_intervals(Series::new("v".into(), values), buckets, range);
    }
    let values = [i128::MIN, i128::MIN + 1, -1, 0, 1, i128::MAX - 1, i128::MAX]
        .map(|value| value.to_string());
    let min = i128::MIN.to_string();
    let max = i128::MAX.to_string();
    for range in [None, Some((min.as_str(), max.as_str()))] {
        assert_histogram_intervals(Series::new("v".into(), &values), 200, range);
    }
}

#[test]
fn decimal_bucket_counts_match_their_advertised_query_bounds() {
    assert_histogram_intervals(Series::new("v".into(), ["0", "0.58", "1"]), 50, None);
    for values in [
        ["-1.5", "-0.92", "-0.5", "0.5"],
        ["0.1", "0.2", "0.3", "0.4"],
        ["0.58", "0.58", "0.58", "0.58"],
    ] {
        assert_histogram_intervals(Series::new("v".into(), values), 50, None);
        assert_histogram_intervals(Series::new("v".into(), values), 50, Some(("0.1", "0.9")));
    }
    let flags = HashMap::from([(
        1,
        FlagEntry {
            flag: "critical".into(),
            memo: None,
            tags: vec![],
            bookmarked: false,
        },
    )]);
    let histogram = histogram_series_annotated(
        "v",
        &Series::new("v".into(), [0.0, 0.58, 1.0]),
        &[0, 1, 2],
        50,
        None,
        &flags,
        false,
    )
    .unwrap();
    let marked = &histogram.buckets[29];
    assert_eq!(
        (&marked.start[..], &marked.end[..], marked.count),
        ("0.58", "0.6", 1)
    );
    assert_eq!(marked.flags.critical, 1);
    assert_eq!(histogram.buckets[28].count, 0);
}

#[test]
fn datetime_bucket_bounds_preserve_nanoseconds_and_empty_bins() {
    let values = [
        "2026-11-14T00:00:00.000000001Z",
        "2026-11-14T09:00:00.000000002+09:00",
        "2026-11-14T00:00:00.000000003Z",
        "2026-11-14T00:00:00.000000004Z",
    ];
    for buckets in [2, 8] {
        assert_histogram_intervals(Series::new("v".into(), &values), buckets, None);
    }
    assert_histogram_intervals(Series::new("v".into(), [values[0]; 2]), 5, None);
}

#[test]
fn empty_and_missing_only_selections_keep_the_source_kind() {
    let series = Series::new(
        "time".into(),
        [Some("2026-11-14T00:00:00Z"), None, Some("")],
    );
    for (rows, missing) in [(vec![], 0), (vec![1, 2], 2)] {
        let histogram =
            histogram_series_annotated("time", &series, &rows, 3, None, &HashMap::new(), false)
                .unwrap();
        assert_eq!(histogram.kind, "datetime");
        assert_eq!(histogram.missing, missing);
        assert!(histogram.buckets.iter().all(|bucket| bucket.count == 0));
        let time = analyze_series("time", &series, &rows, 1, 30, 1, 0).unwrap();
        assert!(time.gaps.is_empty() && time.sessions.is_empty() && time.bursts.is_empty());
    }
    let series = Series::new("n".into(), [Some("1"), None, Some("")]);
    let histogram = histogram_series_ranged("n", &series, &[1, 2], 3, None).unwrap();
    assert_eq!(histogram.kind, "int");
    assert_eq!(histogram.missing, 2);
}

#[test]
fn stats_histogram_and_time_analysis_infer_types_from_the_same_filtered_rows() {
    let root = std::env::temp_dir().join(format!("trivium-analysis-scope-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let path = root.join("mixed.jsonl");
    fs::write(
        &path,
        [
            json!({"time":"2026-11-14T00:00:00.000000001Z","n":1,"status":"keep"}),
            json!({"time":"2026-11-14T00:00:01.000000002Z","n":2,"status":"keep"}),
            json!({"time":"not-a-time","n":"not-a-number","status":"drop"}),
            json!({"time":null,"n":null,"status":"keep"}),
            json!({"status":"keep"}),
        ]
        .iter()
        .map(|record| format!("{record}\n"))
        .collect::<String>(),
    )
    .unwrap();
    let store = ProjectsStore::new(root.join("workspace")).unwrap();
    let project = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: path.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    let id = project.meta.id;
    let columns = common::column_ids(&store, id);
    let status = common::id(&columns, "/status");
    let mut params = json!({"projectId":id,"search":"keep","searchColumns":[status]});
    let query =
        ops::rows::query_project_rows(&store, serde_json::from_value(params.clone()).unwrap())
            .unwrap();
    assert_eq!(query.total_filtered_rows, 4);
    for (name, kind) in [("/time", "datetime"), ("/n", "int")] {
        params["column"] = json!(common::id(&columns, name));
        let stats =
            ops::stats::column_stats(&store, serde_json::from_value(params.clone()).unwrap())
                .unwrap();
        assert_eq!(
            (stats.kind.as_str(), stats.unique, stats.missing),
            (kind, 2, 2)
        );
        let histogram =
            ops::histogram::histogram(&store, serde_json::from_value(params.clone()).unwrap())
                .unwrap();
        assert_eq!(histogram.kind, kind);
        assert_eq!(histogram.missing, 2);
        assert_eq!(
            histogram
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            2
        );
        if name == "/time" {
            let time =
                ops::time::time_analysis(&store, serde_json::from_value(params.clone()).unwrap())
                    .unwrap();
            assert_eq!(
                time.sessions
                    .iter()
                    .map(|session| session.count)
                    .sum::<u64>(),
                2
            );
            assert_eq!(time.sessions[0].start, "2026-11-14T00:00:00.000000001Z");
            assert_eq!(time.sessions[0].end, "2026-11-14T00:00:01.000000002Z");
        }
    }
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

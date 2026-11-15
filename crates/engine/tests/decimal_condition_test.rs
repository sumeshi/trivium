//! Exact decimal ordering is shared by filtering, histogram bounds and sort.
use std::{collections::HashMap, fs};

use polars::prelude::*;
use serde_json::json;
use trivium_engine::{
    analysis::histogram::{histogram_series_annotated, histogram_series_ranged},
    condition::{Condition, Node, Op},
    jobs::JobContext,
    ops::{
        self,
        rows::{resolve_target, QuerySpec},
    },
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

fn predicate(column: &str, op: Op, value: &str) -> Node {
    Node::Condition(Condition {
        column: column.into(),
        op,
        values: vec![value.into()],
        negated: false,
    })
}

#[test]
fn unparseable_numeric_sources_do_not_match_but_invalid_bounds_remain_errors() {
    for invalid in ["", " ", "NaN", "Infinity", "not numeric", "1.2.3", "1e"] {
        for (source, value) in [(invalid, "0"), ("1.5", invalid)] {
            let frame = DataFrame::new(vec![Series::new("n", [source])]).unwrap();
            let spec = QuerySpec {
                condition: Some(predicate("n", Op::Ge, value)),
                ..Default::default()
            };
            let result = resolve_target(
                &frame,
                &spec,
                &["n".into()],
                &HashMap::new(),
                &JobContext::none(),
            );
            if value == invalid {
                assert!(result.is_err(), "source={source:?} value={value:?}");
            } else {
                assert!(result.unwrap().filtered.is_empty(), "source={source:?}");
            }
        }
    }
}

#[test]
fn single_value_float_histograms_use_one_inclusive_bin_and_reject_infinite_spans() {
    for series in [Series::new("n", ["1.5"; 3]), Series::new("n", [1.5_f64; 3])] {
        let frame = DataFrame::new(vec![series.clone()]).unwrap();
        for range in [None, Some(("1.5", "1.5"))] {
            for histogram in [
                histogram_series_ranged("n", &series, &[0, 1, 2], 3, range).unwrap(),
                histogram_series_annotated(
                    "n",
                    &series,
                    &[0, 1, 2],
                    3,
                    range,
                    &HashMap::new(),
                    false,
                )
                .unwrap(),
            ] {
                for (index, bucket) in histogram.buckets.iter().enumerate() {
                    assert_eq!(bucket.end_inclusive, index == 0);
                    let spec = QuerySpec {
                        condition: Some(Node::Group(trivium_engine::condition::Group {
                            kind: trivium_engine::condition::GroupKind::And,
                            children: vec![
                                predicate("n", Op::Ge, &bucket.start),
                                predicate(
                                    "n",
                                    if bucket.end_inclusive { Op::Le } else { Op::Lt },
                                    &bucket.end,
                                ),
                            ],
                        })),
                        ..Default::default()
                    };
                    let target = resolve_target(
                        &frame,
                        &spec,
                        &["n".into()],
                        &HashMap::new(),
                        &JobContext::none(),
                    )
                    .unwrap();
                    assert_eq!(target.filtered.len() as u64, bucket.count, "{bucket:?}");
                }
            }
        }
    }
    let series = Series::new("n", ["-1e308", "1e308"]);
    for range in [None, Some(("-1e308", "1e308"))] {
        let error = histogram_series_ranged("n", &series, &[0, 1], 3, range).unwrap_err();
        assert!(error.contains("span is too wide"), "{error}");
    }
    let series = Series::new("n", ["1e307", "1e308"]);
    let histogram = histogram_series_ranged("n", &series, &[0, 1], 50, None).unwrap();
    assert!(histogram.buckets.iter().all(|bucket| bucket
        .start
        .parse::<f64>()
        .unwrap()
        .is_finite()
        && bucket.end.parse::<f64>().unwrap().is_finite()));
}

#[test]
fn decimal_ordering_keeps_digits_exponents_and_integer_boundaries_exact() {
    let frame = DataFrame::new(vec![Series::new(
        "n",
        [
            Some("-2.5"),
            Some("-0"),
            Some("0.1"),
            Some("1.5"),
            Some("1.500000000000000000001"),
            Some("2.5"),
            Some("10"),
            Some("1e1"),
            Some("170141183460469231731687303715884105728"),
            Some("170141183460469231731687303715884105729"),
            None,
            Some("9007199254740993.000000000000000000"),
            Some("9007199254740993.000000000000000001"),
            Some("1.50"),
        ],
    )])
    .unwrap();
    for (op, value, expected) in [
        (Op::Ge, "1.5", vec![3, 4, 5, 6, 7, 8, 9, 11, 12, 13]),
        (Op::Gt, "1.5", vec![4, 5, 6, 7, 8, 9, 11, 12]),
        (Op::Lt, "1.5", vec![0, 1, 2]),
        (Op::Le, "1.5", vec![0, 1, 2, 3, 13]),
        (Op::Ge, "0", vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13]),
        (
            Op::Gt,
            "9007199254740993.000000000000000000",
            vec![8, 9, 12],
        ),
        (Op::Gt, "170141183460469231731687303715884105728", vec![9]),
        (Op::Lt, "-2.500000000000000000001", vec![]),
        (Op::Eq, "1.5", vec![3]),
        (Op::Eq, "1e1", vec![7]),
        (
            Op::Ne,
            "1.5",
            vec![0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        ),
        (Op::In, "1.50", vec![13]),
    ] {
        let spec = QuerySpec {
            condition: Some(predicate("n", op, value)),
            ..Default::default()
        };
        let target = resolve_target(
            &frame,
            &spec,
            &["n".into()],
            &HashMap::new(),
            &JobContext::none(),
        )
        .unwrap();
        assert_eq!(target.filtered, expected, "{op:?} {value}");
    }
    // Native float columns from older projects use their existing source text
    // adapter too, without turning string-backed CSV values into f64.
    let frame = DataFrame::new(vec![Series::new("n", [0.1_f64, 1.5, 2.5])]).unwrap();
    let target = resolve_target(
        &frame,
        &QuerySpec {
            condition: Some(predicate("n", Op::Ge, "1.5")),
            ..Default::default()
        },
        &["n".into()],
        &HashMap::new(),
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(target.filtered, [1, 2]);
}

#[test]
fn decimal_histogram_bounds_query_export_and_bulk_resolve_the_same_rows() {
    let root = std::env::temp_dir().join(format!("trivium-decimal-condition-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("decimals.csv");
    fs::write(&source, "n\n0.1\n1.5\n2.5\n").unwrap();
    let store = ProjectsStore::new(root.join("workspace")).unwrap();
    let project = ops::projects::create_project(
        &store,
        serde_json::from_value(json!({"path":source})).unwrap(),
    )
    .unwrap();
    let id = project.meta.id;
    let n = common::id(&common::column_ids(&store, id), "n");
    let histogram = ops::histogram::histogram(
        &store,
        serde_json::from_value(json!({
            "projectId":id,"column":n,"buckets":2,
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(histogram.kind, "float");
    for bucket in &histogram.buckets {
        let condition = json!({"kind":"and","children":[
            {"column":n,"op":"ge","values":[bucket.start]},
            {"column":n,"op":if bucket.end_inclusive {"le"} else {"lt"},"values":[bucket.end]},
        ]});
        let query = ops::rows::query_project_rows(
            &store,
            serde_json::from_value(json!({
                "projectId":id,"condition":condition,
            }))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(query.total_filtered_rows as u64, bucket.count);
    }
    let bucket = &histogram.buckets[1];
    let condition = json!({"kind":"and","children":[
        {"column":n,"op":"ge","values":[bucket.start]},
        {"column":n,"op":"le","values":[bucket.end]},
    ]});
    let destination = root.join("filtered.csv");
    let exported = ops::export::export_project(&store, serde_json::from_value(json!({
        "projectId":id,"destination":destination,"condition":condition,"includeAnnotations":false,
    })).unwrap()).unwrap();
    assert_eq!(exported.rows, 2);
    let values: Vec<_> = csv::Reader::from_path(&destination)
        .unwrap()
        .records()
        .map(|row| row.unwrap()[0].to_owned())
        .collect();
    assert_eq!(values, ["1.5", "2.5"]);

    let mut bulk =
        json!({"projectId":id,"target":"filtered","condition":condition,"flag":"critical"});
    let preview =
        ops::bulk::bulk_preview(&store, serde_json::from_value(bulk.clone()).unwrap()).unwrap();
    assert_eq!(preview.count, 2);
    bulk["previewToken"] = json!(preview.preview_token);
    assert_eq!(
        ops::bulk::bulk_update(&store, serde_json::from_value(bulk).unwrap())
            .unwrap()
            .updated,
        2
    );
    let flagged = ops::rows::query_project_rows(
        &store,
        serde_json::from_value(json!({
            "projectId":id,"flagFilter":"critical",
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        flagged
            .rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(fs::read_to_string(&source).unwrap(), "n\n0.1\n1.5\n2.5\n");
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

//! Histogram labels, membership and flags share exact numeric query semantics.
use std::{collections::HashMap, fs, path::PathBuf};

use polars::prelude::{NamedFrom, Series};
use serde_json::json;
use trivium_engine::{
    analysis::histogram::{histogram_series_annotated, histogram_series_ranged, Histogram},
    models::FlagEntry,
    ops::{self, projects::CreateProjectPayload, rows::QueryRowsPayload},
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

struct Fixture {
    _root: tempfile::TempDir,
    store: ProjectsStore,
    id: Uuid,
    column: String,
    series: Series,
    flags: HashMap<usize, FlagEntry>,
    source: PathBuf,
    data: PathBuf,
    source_bytes: Vec<u8>,
    data_bytes: Vec<u8>,
}

impl Fixture {
    fn new(values: &[&str]) -> Self {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("decimals.csv");
        fs::write(&source, format!("n\n{}\n", values.join("\n"))).unwrap();
        let store = ProjectsStore::new(root.path().join("cases")).unwrap();
        let id = ops::projects::create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into_owned(),
                import_options: None,
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        let column = common::id(&common::column_ids(&store, id), "n");
        let flags = ["safe", "critical"]
            .into_iter()
            .enumerate()
            .map(|(row, flag)| {
                ops::flags::update_flag(
                    &store,
                    serde_json::from_value(json!({"projectId":id,"row_index":row,"flag":flag}))
                        .unwrap(),
                )
                .unwrap();
                (
                    row,
                    FlagEntry {
                        flag: flag.into(),
                        memo: None,
                        tags: vec![],
                        bookmarked: false,
                    },
                )
            })
            .collect();
        let series = Series::new(&column, values);
        let data = store.project_dir(&id).join("data.parquet");
        let source_bytes = fs::read(&source).unwrap();
        let data_bytes = fs::read(&data).unwrap();
        Self {
            _root: root,
            store,
            id,
            column,
            series,
            flags,
            source,
            data,
            source_bytes,
            data_bytes,
        }
    }

    fn histogram(&self, range: Option<(&str, &str)>) -> Result<Histogram, String> {
        let mut payload = json!({"projectId":self.id,"column":self.column,"buckets":3});
        if let Some((min, max)) = range {
            payload["range"] = json!({"min":min,"max":max});
        }
        ops::histogram::histogram(&self.store, serde_json::from_value(payload).unwrap())
            .map_err(|error| error.to_string())
    }

    fn assert_intervals(&self, histogram: &Histogram, annotated: bool) {
        assert_eq!(histogram.missing, 0);
        assert_eq!(histogram.invalid, 0);
        assert_eq!(
            histogram
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>()
                + histogram.outside,
            self.series.len() as u64
        );
        for bucket in &histogram.buckets {
            let payload: QueryRowsPayload = serde_json::from_value(json!({
                "projectId":self.id,"limit":10,
                "condition":{"kind":"and","children":[
                    {"column":self.column,"op":"ge","values":[bucket.start]},
                    {"column":self.column,"op":if bucket.end_inclusive {"le"} else {"lt"},"values":[bucket.end]}
                ]}
            })).unwrap();
            let result = ops::rows::query_project_rows(&self.store, payload).unwrap();
            let ids = result
                .rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>();
            assert_eq!(
                result.total_filtered_rows as u64, bucket.count,
                "{bucket:?}"
            );
            assert_eq!(bucket.flags.suspicious, 0);
            if annotated {
                assert_eq!(bucket.flags.safe, u64::from(ids.contains(&0)), "{bucket:?}");
                assert_eq!(
                    bucket.flags.critical,
                    u64::from(ids.contains(&1)),
                    "{bucket:?}"
                );
                assert_eq!(bucket.flags.unflagged, 0);
            } else {
                assert_eq!(bucket.flags.safe + bucket.flags.critical, 0);
                assert_eq!(bucket.flags.unflagged, bucket.count);
            }
        }
        assert_eq!(fs::read(&self.source).unwrap(), self.source_bytes);
        assert_eq!(fs::read(&self.data).unwrap(), self.data_bytes);
    }

    fn all_histograms(&self, range: Option<(&str, &str)>) -> Vec<(Histogram, bool)> {
        let rows = [0, 1];
        vec![
            (
                histogram_series_ranged(&self.column, &self.series, &rows, 3, range).unwrap(),
                false,
            ),
            (
                histogram_series_annotated(
                    &self.column,
                    &self.series,
                    &rows,
                    3,
                    range,
                    &self.flags,
                    false,
                )
                .unwrap(),
                true,
            ),
            (self.histogram(range).unwrap(), true),
        ]
    }
}

#[test]
fn explicit_decimal_ranges_keep_distinct_values_that_round_to_the_same_float() {
    for values in [
        ["0.1", "0.10000000000000000001"],
        ["-0.10000000000000000001", "-0.1"],
        ["9007199254740992.1", "9007199254740992.2"],
        ["1e-400", "2e-400"],
    ] {
        let fixture = Fixture::new(&values);
        for bound in values {
            for (histogram, annotated) in fixture.all_histograms(Some((bound, bound))) {
                assert_eq!(histogram.outside, 1, "{bound}: {histogram:?}");
                fixture.assert_intervals(&histogram, annotated);
            }
        }
    }
}

#[test]
fn reversed_exact_decimal_ranges_are_rejected_before_float_rounding() {
    let fixture = Fixture::new(&["0.1", "0.10000000000000000001"]);
    let range = Some(("0.10000000000000000001", "0.1"));
    assert!(histogram_series_ranged(&fixture.column, &fixture.series, &[0, 1], 3, range).is_err());
    assert!(histogram_series_annotated(
        &fixture.column,
        &fixture.series,
        &[0, 1],
        3,
        range,
        &fixture.flags,
        false,
    )
    .is_err());
    assert!(fixture.histogram(range).is_err());
    assert_eq!(fs::read(&fixture.source).unwrap(), fixture.source_bytes);
    assert_eq!(fs::read(&fixture.data).unwrap(), fixture.data_bytes);
}

#[test]
fn automatic_decimal_bucket_labels_preserve_exact_source_extents_and_query_counts() {
    for values in [
        ["0.1", "0.10000000000000000001"],
        ["-0.10000000000000000001", "-0.1"],
        ["9007199254740992.1", "9007199254740992.2"],
        ["1e-400", "2e-400"],
    ] {
        let fixture = Fixture::new(&values);
        for (histogram, annotated) in fixture.all_histograms(None) {
            assert_eq!(histogram.outside, 0);
            assert_eq!(histogram.buckets.first().unwrap().start, values[0]);
            assert_eq!(histogram.buckets.last().unwrap().end, values[1]);
            fixture.assert_intervals(&histogram, annotated);
        }
    }
}

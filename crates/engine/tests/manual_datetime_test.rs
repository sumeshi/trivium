//! Explicit datetime analysis is independent of whole-project type hints.
use std::fs;

use serde_json::{json, Value};
use trivium_engine::{
    ops::{
        self,
        projects::{CreateProjectPayload, ProjectRequest},
    },
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

#[test]
fn mixed_datetime_analysis_counts_invalid_and_missing_without_changing_source_values() {
    for (extension, source, timestamp_name) in [
        (
            "csv",
            concat!(
                "Timestamp,Status\n",
                "2026-11-14T09:00:00.000000001+09:00,keep\n",
                "2026-11-14T00:00:01.000000002Z,keep\n",
                "N/A,exclude\ninvalid,exclude\n,exclude\n   ,exclude\n,exclude\n,exclude\n"
            ),
            "Timestamp",
        ),
        (
            "jsonl",
            concat!(
                "{\"Timestamp\":\"2026-11-14T09:00:00.000000001+09:00\",\"Status\":\"keep\"}\n",
                "{\"Timestamp\":\"2026-11-14T00:00:01.000000002Z\",\"Status\":\"keep\"}\n",
                "{\"Timestamp\":\"N/A\",\"Status\":\"exclude\"}\n",
                "{\"Timestamp\":\"invalid\",\"Status\":\"exclude\"}\n",
                "{\"Timestamp\":\"\",\"Status\":\"exclude\"}\n",
                "{\"Timestamp\":\"   \",\"Status\":\"exclude\"}\n",
                "{\"Timestamp\":null,\"Status\":\"exclude\"}\n",
                "{\"Status\":\"exclude\"}\n"
            ),
            "/Timestamp",
        ),
    ] {
        let root = std::env::temp_dir().join(format!("trivium-manual-time-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("mixed.{extension}"));
        fs::write(&path, source).unwrap();
        let store = ProjectsStore::new(root.join("workspace")).unwrap();
        let id = ops::projects::create_project(
            &store,
            CreateProjectPayload {
                path: path.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        let ids = common::column_ids(&store, id);
        let column = common::id(&ids, timestamp_name);
        let loaded =
            ops::projects::load_project(&store, ProjectRequest { project_id: id }).unwrap();
        assert_eq!(loaded.column_kinds[&column], "string");
        let params = json!({"projectId":id,"column":column,"buckets":2,"asDatetime":true});
        let histogram =
            ops::histogram::histogram(&store, serde_json::from_value(params.clone()).unwrap())
                .unwrap();
        assert_eq!(histogram.kind, "datetime");
        assert_eq!(
            (histogram.missing, histogram.invalid, histogram.outside),
            (4, 2, 0)
        );
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
        let range = histogram.full_range.unwrap();
        assert_eq!(range.min, "2026-11-14T00:00:00.000000001");
        assert_eq!(range.max, "2026-11-14T00:00:01.000000002");
        let analysis =
            ops::time::time_analysis(&store, serde_json::from_value(params.clone()).unwrap())
                .unwrap();
        assert_eq!((analysis.missing, analysis.invalid), (4, 2));
        assert_eq!(
            analysis
                .sessions
                .iter()
                .map(|session| session.count)
                .sum::<u64>(),
            2
        );
        assert_eq!(
            analysis.sessions[0].start,
            "2026-11-14T09:00:00.000000001+09:00"
        );
        assert_eq!(analysis.sessions[0].end, "2026-11-14T00:00:01.000000002Z");

        // Existing API callers retain automatic type validation.
        let mut automatic = params.clone();
        automatic.as_object_mut().unwrap().remove("asDatetime");
        assert!(ops::histogram::histogram(
            &store,
            serde_json::from_value(automatic.clone()).unwrap()
        )
        .is_err());
        assert!(ops::time::time_analysis(
            &store,
            serde_json::from_value(automatic.clone()).unwrap()
        )
        .is_err());
        // Filtered type inference is reflected by stats and remains usable without an override.
        automatic["search"] = json!("keep");
        let stats =
            ops::stats::column_stats(&store, serde_json::from_value(automatic.clone()).unwrap())
                .unwrap();
        assert_eq!(stats.kind, "datetime");
        let filtered =
            ops::histogram::histogram(&store, serde_json::from_value(automatic).unwrap()).unwrap();
        assert_eq!((filtered.missing, filtered.invalid), (0, 0));
        assert_eq!(
            filtered
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            2
        );

        let queried = ops::rows::query_project_rows(
            &store,
            serde_json::from_value(json!({"projectId":id})).unwrap(),
        )
        .unwrap();
        assert_eq!(
            common::cell_text(&queried.rows[0], &column),
            Some("2026-11-14T09:00:00.000000001+09:00")
        );
        assert_eq!(common::cell_text(&queried.rows[2], &column), Some("N/A"));
        assert_eq!(
            ops::projects::load_project(&store, ProjectRequest { project_id: id })
                .unwrap()
                .column_kinds[&column],
            "string"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), source);
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn explicit_datetime_analysis_reports_all_unparseable_rows_and_validates_bounds() {
    let root = std::env::temp_dir().join(format!("trivium-invalid-time-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let path = root.join("invalid.csv");
    fs::write(&path, "Timestamp\nN/A\nunknown\n").unwrap();
    let store = ProjectsStore::new(root.join("workspace")).unwrap();
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap()
    .meta
    .id;
    let column = common::id(&common::column_ids(&store, id), "Timestamp");
    let mut params = json!({"projectId":id,"column":column,"asDatetime":true,"focus":true});
    let histogram =
        ops::histogram::histogram(&store, serde_json::from_value(params.clone()).unwrap()).unwrap();
    assert_eq!((histogram.missing, histogram.invalid), (0, 2));
    assert!(histogram.full_range.is_none());
    assert!(histogram
        .buckets
        .iter()
        .all(|bucket| bucket.start.is_empty() && bucket.count == 0));
    let analysis =
        ops::time::time_analysis(&store, serde_json::from_value(params.clone()).unwrap()).unwrap();
    assert_eq!((analysis.missing, analysis.invalid), (0, 2));
    assert!(analysis.gaps.is_empty() && analysis.sessions.is_empty() && analysis.bursts.is_empty());
    params["range"] = json!({"min":"not a date","max":"2026-11-14T01:00:00Z"});
    assert!(
        ops::histogram::histogram(&store, serde_json::from_value(params.clone()).unwrap()).is_err()
    );
    params["range"] = Value::Null;
    params["asDatetime"] = json!("datetime");
    assert!(serde_json::from_value::<ops::histogram::HistogramPayload>(params).is_err());
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

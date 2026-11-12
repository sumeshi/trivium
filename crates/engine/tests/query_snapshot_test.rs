//! End-to-end raw-source semantics and coherent membership/count snapshots.
use std::{
    fs,
    sync::{Arc, Mutex},
};

use polars::prelude::*;
use trivium_engine::{
    condition::{Condition, Node, Op},
    jobs::JobContext,
    ops::{
        self,
        flags::{update_flag, UpdateFlagPayload},
        projects::CreateProjectPayload,
        rows::{resolve_project_target, QuerySpec},
    },
    project_io::{last_column_read_projection, project_read_stats, reset_project_read_counters},
    store::ProjectsStore,
};
use trivium_engine::{models::ProjectMeta, project_io::write_project_dataframe};
use uuid::Uuid;

mod common;

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn annotation_only_and_constant_group_queries_work_on_legacy_parquet_without_row_ids() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let root = std::env::temp_dir().join(format!("trivium-query-legacy-{}", Uuid::new_v4()));
    let store = ProjectsStore::new(root.clone()).unwrap();
    let id = Uuid::new_v4();
    fs::create_dir_all(store.project_dir(&id)).unwrap();
    let mut frame = DataFrame::new(vec![Series::new("value", [5i64, 8, 13])]).unwrap();
    write_project_dataframe(&store.project_dir(&id).join("data.parquet"), &mut frame).unwrap();
    store
        .insert(ProjectMeta {
            id,
            name: "legacy".into(),
            description: None,
            created_at: chrono::Utc::now(),
            total_records: 3,
            flagged_records: 0,
            ioc_applied_records: 0,
            source_sha256: None,
            source_file_name: None,
            source_format: None,
            source_size: None,
            source_path: None,
            hidden_columns: vec![],
            storage_version: 0,
            columns: vec![],
        })
        .unwrap();
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 1,
            flag: Some("critical".into()),
            memo: None,
            tags: None,
            bookmarked: Some(true),
        },
    )
    .unwrap();
    reset_project_read_counters();
    let target = resolve_project_target(
        &store,
        id,
        &QuerySpec {
            flag_filter: Some("critical".into()),
            bookmarked_only: Some(true),
            ..Default::default()
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(&*target.ids, &[1]);
    assert_eq!(
        project_read_stats().decoded_rows,
        0,
        "annotation-only query decoded source values"
    );
    let constant = QuerySpec {
        condition: Some(Node::Group(trivium_engine::condition::Group {
            kind: trivium_engine::condition::GroupKind::And,
            children: vec![],
        })),
        ..Default::default()
    };
    let target = resolve_project_target(&store, id, &constant, &JobContext::none()).unwrap();
    assert_eq!(
        &*target.ids,
        &[0, 1, 2],
        "constant query lost legacy source row count"
    );
    fs::remove_dir_all(root).unwrap();
}

fn condition(column: String, op: Op, value: &str) -> QuerySpec {
    QuerySpec {
        condition: Some(Node::Condition(Condition {
            column,
            op,
            values: vec![value.into()],
            negated: false,
        })),
        ..Default::default()
    }
}

#[test]
fn csv_and_jsonl_preserve_raw_values_and_share_cached_ids_without_mixing_annotation_snapshots() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    for (extension, source, text_source, number_source, time_source, empty_ids) in [
        ("csv", "text,number,time,big\n,0007,2026-11-08T01:02:03.000000001+09:00,hidden\nΟΣ,9007199254740993,2026-11-07T16:02:03.000000002Z,hidden\n", "text", "number", "time", vec![0]),
        ("jsonl", "{\"text\":null,\"number\":\"0007\",\"time\":\"2026-11-08T01:02:03.000000001+09:00\",\"big\":\"hidden\"}\n{\"text\":\"ΟΣ\",\"number\":9007199254740993,\"time\":\"2026-11-07T16:02:03.000000002Z\",\"big\":\"hidden\"}\n{}\n{\"text\":\"\"}\n", "/text", "/number", "/time", vec![3]),
    ] {
        let root = std::env::temp_dir().join(format!("trivium-query-snapshot-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("source.{extension}"));
        fs::write(&path, source).unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let id = ops::projects::create_project(&store, CreateProjectPayload {
            path: path.to_string_lossy().into_owned(), description: None, delimiter: None, restore_annotations: None,
        }).unwrap().meta.id;
        let columns = common::column_ids(&store, id);
        let job = JobContext::none();
        let text = common::id(&columns, text_source);
        let number = common::id(&columns, number_source);
        let time = common::id(&columns, time_source);
        for (spec, expected) in [
            (condition(text.clone(), Op::Eq, ""), empty_ids),
            (condition(text, Op::Contains, "ος"), vec![1]),
            (condition(number.clone(), Op::Eq, "0007"), vec![0]),
            (condition(number.clone(), Op::Eq, "9007199254740993"), vec![1]),
            (condition(number, Op::Gt, "9007199254740992"), vec![1]),
            (condition(time.clone(), Op::Gt, "2026-11-07T16:02:03.000000001Z"), vec![1]),
        ] {
            let target = resolve_project_target(&store, id, &spec, &job).unwrap();
            assert_eq!(&*target.ids, &expected, "{extension}: {spec:?}");
        }
        reset_project_read_counters();
        let unfiltered = resolve_project_target(&store, id, &QuerySpec::default(), &job).unwrap();
        assert_eq!(project_read_stats().decoded_rows, 0, "source order must use metadata, not a full frame");
        assert_eq!(unfiltered.ids.len(), unfiltered.total_rows);

        let scoped = condition(time, Op::Gt, "2026-11-07T16:02:03.000000001Z");
        // This result is still among the two cached entries from the loop.
        let before = resolve_project_target(&store, id, &scoped, &job).unwrap();
        assert_eq!(before.annotated_filtered, 0);
        update_flag(&store, UpdateFlagPayload {
            project_id: id, row_index: 1, flag: Some("critical".into()), memo: None,
            tags: None, bookmarked: Some(true),
        }).unwrap();
        let after = resolve_project_target(&store, id, &scoped, &job).unwrap();
        assert!(Arc::ptr_eq(&before.ids, &after.ids), "data-only query reran after annotation edit");
        assert!(before.flags.is_empty(), "an old snapshot changed underneath its caller");
        assert_eq!(after.flags.get(&1).unwrap().flag, "critical");
        assert_eq!((after.annotated_total, after.annotated_filtered), (1, 1));
        assert!(after.annotation_revision > before.annotation_revision);

        reset_project_read_counters();
        let flagged = QuerySpec { flag_filter: Some("critical".into()), ..Default::default() };
        let target = resolve_project_target(&store, id, &flagged, &job).unwrap();
        assert_eq!(&*target.ids, &[1]);
        assert!(!last_column_read_projection().contains(&common::id(&columns, if extension == "csv" { "big" } else { "/big" })), "annotation-only membership decoded an unrelated column");
        update_flag(&store, UpdateFlagPayload {
            project_id: id, row_index: 1, flag: Some("safe".into()), memo: None, tags: None, bookmarked: None,
        }).unwrap();
        let changed = resolve_project_target(&store, id, &flagged, &job).unwrap();
        assert!(changed.ids.is_empty(), "annotation-dependent IDs were reused after an edit");
        assert_eq!(target.flags.get(&1).unwrap().flag, "critical");
        fs::remove_dir_all(root).unwrap();
    }
}

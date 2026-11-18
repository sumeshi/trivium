//! End-to-end raw-source semantics and coherent membership/count snapshots.
use std::{
    fs,
    sync::{Arc, Mutex},
};

use polars::prelude::*;
use trivium_engine::{
    annotation::AnnotationPatch,
    condition::{Condition, Node, Op},
    jobs::JobContext,
    ops::{
        self,
        bulk::{bulk_preview, bulk_update, BulkAnnotatePayload},
        flags::{update_flag, UpdateFlagPayload},
        projects::CreateProjectPayload,
        rows::{resolve_project_target, QuerySpec},
    },
    project_io::{
        last_column_read_projection, project_data_reads, project_read_stats,
        reset_project_read_counters,
    },
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

#[test]
fn single_and_selected_bulk_annotations_never_decode_source_cells() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("annotations.csv");
    fs::write(&source, "id,value\n0,zero\n1,one\n2,two\n3,three\n").unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap()
    .meta
    .id;
    for (row_index, flag, memo, bookmarked) in [
        (0, Some("safe"), None, None),
        (1, None, Some("note"), None),
        (2, None, None, Some(true)),
    ] {
        reset_project_read_counters();
        let result = update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index,
                flag: flag.map(str::to_string),
                memo: memo.map(|value| Some(value.to_string())),
                tags: None,
                bookmarked,
            },
        )
        .unwrap();
        let entry = result.entry.unwrap();
        assert_eq!(entry.flag, flag.unwrap_or_default());
        assert_eq!(entry.memo.as_deref(), memo);
        assert_eq!(entry.bookmarked, bookmarked.unwrap_or(false));
        assert_eq!(result.flagged_records, 1);
        assert_eq!(
            project_read_stats().decoded_rows,
            0,
            "single annotation decoded source cells"
        );
        assert_eq!(project_data_reads(), 0);
    }
    let mut request = BulkAnnotatePayload {
        project_id: id,
        target: "selection".into(),
        row_indices: Some(vec![1, 3]),
        preview_token: None,
        spec: QuerySpec::default(),
        patch: AnnotationPatch {
            flag: Some("suspicious".into()),
            ..Default::default()
        },
    };
    reset_project_read_counters();
    let preview = bulk_preview(&store, request.clone()).unwrap();
    assert_eq!(preview.count, 2);
    assert_eq!(
        project_read_stats().decoded_rows,
        0,
        "selection preview decoded source cells"
    );
    assert_eq!(project_data_reads(), 0);
    request.preview_token = Some(preview.preview_token);
    reset_project_read_counters();
    assert_eq!(bulk_update(&store, request).unwrap().updated, 2);
    assert_eq!(
        project_read_stats().decoded_rows,
        0,
        "selection apply decoded source cells"
    );
    assert_eq!(project_data_reads(), 0);
    let saved = trivium_engine::storage::load_flags(&store.flags_path(&id)).unwrap();
    assert_eq!(saved.len(), 4);
    assert_eq!(saved[&0].flag, "safe");
    assert_eq!(saved[&1].flag, "suspicious");
    assert_eq!(saved[&1].memo.as_deref(), Some("note"));
    assert_eq!(saved[&2].flag, "");
    assert!(saved[&2].bookmarked);
    assert_eq!(saved[&3].flag, "suspicious");
    assert_eq!(saved[&3].memo, None);
}

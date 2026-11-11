//! Original-row lookup must share the displayed query order and stay bounded.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use polars::prelude::*;
use uuid::Uuid;

use trivium_engine::{
    jobs::{request_cancel, try_begin_job, JobContext, JobKind, ProgressFn},
    models::ProjectMeta,
    ops::{
        flags::{update_flag, UpdateFlagPayload},
        projects::{create_project, CreateProjectPayload},
        rows::{
            locate_project_row, locate_project_row_with_job, query_project_rows,
            reset_resolve_count, resolve_count, LocateRowPayload, LocateRowResponse,
            QueryRowsPayload, QuerySpec,
        },
    },
    project_io::{
        last_column_read_projection, project_data_reads, project_full_reads, project_page_reads,
        reset_project_read_counters,
    },
    store::ProjectsStore,
};

mod common;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn csv_fixture() -> (PathBuf, ProjectsStore, Uuid, HashMap<String, String>) {
    let root = std::env::temp_dir().join(format!("trivium-locate-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("rows.csv");
    let mut output = BufWriter::new(File::create(&source).unwrap());
    writeln!(output, "key,group,note").unwrap();
    for index in 0..5199 {
        writeln!(
            output,
            "{index},{},original-{index}",
            if index % 3 == 0 { "keep" } else { "drop" }
        )
        .unwrap();
    }
    output.flush().unwrap();
    drop(output);
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
    (root, store, id, ids)
}

fn locate(store: &ProjectsStore, id: Uuid, original: usize, spec: QuerySpec) -> LocateRowResponse {
    locate_project_row(
        store,
        LocateRowPayload {
            project_id: id,
            row_index: original,
            spec,
        },
    )
    .unwrap()
}

fn update(store: &ProjectsStore, id: Uuid, row: usize, flag: Option<&str>, bookmark: Option<bool>) {
    update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index: row,
            flag: flag.map(str::to_owned),
            memo: None,
            tags: None,
            bookmarked: bookmark,
        },
    )
    .unwrap();
}

#[test]
fn lookup_5199_source_rows_and_bounds_needs_no_frame_page_or_resolution() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let (root, store, id, _) = csv_fixture();
    reset_project_read_counters();
    reset_resolve_count();
    for row in [0, 2599, 5198] {
        let found = locate(&store, id, row, QuerySpec::default());
        assert_eq!(found.position, Some(row));
        assert_eq!((found.total_rows, found.total_filtered_rows), (5199, 5199));
    }
    for row in [5199, usize::MAX] {
        assert_eq!(locate(&store, id, row, QuerySpec::default()).position, None);
    }
    assert_eq!(project_data_reads(), 0);
    assert_eq!(project_page_reads(), 0);
    assert_eq!(resolve_count(), 0);
    drop(store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn filtered_descending_lookup_matches_actual_pages_and_reuses_their_ids() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let (root, store, id, ids) = csv_fixture();
    let key = common::id(&ids, "key");
    let group = common::id(&ids, "group");
    let note = common::id(&ids, "note");
    let spec = QuerySpec {
        search: Some("keep".into()),
        search_columns: Some(vec![group.clone()]),
        sort_key: Some(key.clone()),
        sort_direction: Some("desc".into()),
        ..Default::default()
    };
    reset_project_read_counters();
    reset_resolve_count();
    let first = locate(&store, id, 5196, spec.clone());
    assert_eq!(first.position, Some(0));
    assert_eq!((first.total_rows, first.total_filtered_rows), (5199, 1733));
    assert_eq!(resolve_count(), 1);
    assert_eq!(
        project_page_reads(),
        0,
        "lookup must not materialize a page"
    );
    assert_eq!(project_full_reads(), 0);
    let projection = last_column_read_projection();
    assert!(projection.contains(&key) && projection.contains(&group));
    assert!(!projection.contains(&note));

    let page = query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: spec.clone(),
            offset: Some(0),
            limit: Some(2000),
        },
    )
    .unwrap();
    let ordered: Vec<usize> = page.rows.iter().map(|row| row.row_index).collect();
    assert_eq!(
        ordered,
        (0..5199)
            .filter(|row| row % 3 == 0)
            .rev()
            .collect::<Vec<_>>()
    );
    reset_project_read_counters();
    for row in [0, 3, 2598, 5196, 1, 5198, 5199] {
        let found = locate(&store, id, row, spec.clone());
        assert_eq!(
            found.position,
            ordered.iter().position(|original| *original == row)
        );
        assert_eq!(found.total_filtered_rows, page.total_filtered_rows);
    }
    assert_eq!(
        resolve_count(),
        1,
        "lookup and page share the cached result"
    );
    assert_eq!(
        project_data_reads(),
        0,
        "cache hits must not read source values"
    );
    assert_eq!(project_page_reads(), 0);
    drop(store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn annotation_dependent_lookup_invalidates_flag_and_bookmark_results() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let (root, store, id, ids) = csv_fixture();
    update(&store, id, 5, Some("critical"), None);
    update(&store, id, 50, Some("suspicious"), None);
    let spec = QuerySpec {
        flag_filter: Some("priority".into()),
        sort_key: Some(common::id(&ids, "key")),
        sort_direction: Some("desc".into()),
        ..Default::default()
    };
    assert_eq!(locate(&store, id, 5, spec.clone()).position, Some(1));
    update(&store, id, 50, Some("safe"), None);
    assert_eq!(locate(&store, id, 50, spec.clone()).position, None);
    assert_eq!(locate(&store, id, 5, spec).position, Some(0));

    let bookmarked = QuerySpec {
        bookmarked_only: Some(true),
        ..Default::default()
    };
    assert_eq!(
        locate(&store, id, 5, bookmarked.clone()).total_filtered_rows,
        0
    );
    update(&store, id, 5, None, Some(true));
    assert_eq!(locate(&store, id, 5, bookmarked.clone()).position, Some(0));
    update(&store, id, 5, None, Some(false));
    assert_eq!(locate(&store, id, 5, bookmarked).position, None);
    drop(store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn running_lookup_observes_cancellation_before_source_data_reads() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let (root, store, id, _) = csv_fixture();
    let guard = try_begin_job(JobKind::Query, Some(id)).unwrap();
    let callback: ProgressFn = Arc::new(|progress| {
        request_cancel(progress.id);
    });
    let job = JobContext::with_job(guard, Some(callback));
    reset_project_read_counters();
    let error = locate_project_row_with_job(
        &store,
        LocateRowPayload {
            project_id: id,
            row_index: 2599,
            spec: QuerySpec::default(),
        },
        &job,
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "Cancelled.");
    assert_eq!(project_data_reads(), 0);
    drop(job);
    drop(store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn two_million_row_lookup_reaches_source_start_middle_and_end_without_full_reads() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let root = std::env::temp_dir().join(format!("trivium-locate-2m-{}", Uuid::new_v4()));
    let store = ProjectsStore::new(root.clone()).unwrap();
    let id = Uuid::new_v4();
    let directory = store.project_dir(&id);
    fs::create_dir_all(&directory).unwrap();
    let mut frame = DataFrame::new(vec![Series::new(
        "seq",
        (0..2_000_000u64).collect::<Vec<_>>(),
    )])
    .unwrap();
    ParquetWriter::new(File::create(directory.join("data.parquet")).unwrap())
        .finish(&mut frame)
        .unwrap();
    drop(frame);
    store
        .insert(ProjectMeta {
            id,
            name: "2m source rows".into(),
            description: None,
            created_at: Utc::now(),
            total_records: 2_000_000,
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
    reset_project_read_counters();
    reset_resolve_count();
    for row in [0, 999_999, 1_999_999] {
        let found = locate(&store, id, row, QuerySpec::default());
        assert_eq!(found.position, Some(row));
        assert_eq!(
            (found.total_rows, found.total_filtered_rows),
            (2_000_000, 2_000_000)
        );
        let page = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: id,
                spec: QuerySpec::default(),
                offset: found.position,
                limit: Some(1),
            },
        )
        .unwrap();
        assert_eq!(page.rows[0].row_index, row);
        assert_eq!(
            common::cell_text(&page.rows[0], "seq"),
            Some(row.to_string().as_str())
        );
    }
    assert_eq!(project_full_reads(), 0);
    assert_eq!(resolve_count(), 0);
    assert_eq!(project_page_reads(), 3);
    drop(store);
    let _ = fs::remove_dir_all(root);
}

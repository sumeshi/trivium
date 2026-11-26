//! Persisted query scopes and bounded source-order search, including annotations.
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use trivium_engine::{
    annotation::AnnotationPatch,
    condition::{Condition, Node, Op},
    import::csv::DEFAULT_BATCH_ROWS,
    jobs::{queue_job, request_cancel, JobContext, JobKind},
    ops::{
        self,
        bulk::BulkAnnotatePayload,
        export::ExportProjectPayload,
        flags::UpdateFlagPayload,
        projects::CreateProjectPayload,
        rows::{
            clear_result_cache, locate_project_row, query_project_rows, resolve_project_target,
            resolve_target, LocateRowPayload, QueryRowsPayload, QuerySpec,
        },
    },
    project_io::{
        last_column_read_projection, project_read_stats, read_project_dataframe,
        reset_project_read_counters, PROJECT_BATCH_ROWS,
    },
    storage::load_flags,
    store::ProjectsStore,
};
use uuid::Uuid;
mod common;
static SERIAL: Mutex<()> = Mutex::new(());

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    columns: HashMap<String, String>,
}
impl Fixture {
    fn new(extension: &str, contents: &str) -> Self {
        let root = std::env::temp_dir().join(format!("trivium-query-stream-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let source = root.join(format!("input.{extension}"));
        fs::write(&source, contents).unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let id = ops::projects::create_project(
            &store,
            CreateProjectPayload {
                import_options: None,
                path: source.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        // Exercise a reopened saved project, rather than an all-column frame.
        drop(store);
        let store = ProjectsStore::new(root.clone()).unwrap();
        let columns = common::column_ids(&store, id);
        Self {
            root,
            store,
            id,
            columns,
        }
    }
    fn query(&self, spec: QuerySpec, offset: usize, limit: usize) -> Vec<usize> {
        query_project_rows(
            &self.store,
            QueryRowsPayload {
                project_id: self.id,
                spec,
                offset: Some(offset),
                limit: Some(limit),
            },
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.row_index)
        .collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn unscoped() -> Node {
    Node::Condition(Condition {
        column: String::new(),
        op: Op::Contains,
        values: vec!["powershell".into()],
        negated: false,
    })
}

#[test]
fn persisted_unscoped_conditions_share_projection_for_query_export_bulk_and_locate() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    for (extension, contents, id_name, command_name, host_name) in [
        ("csv", "id,host,command,extra\nr0,ws01,powershell,noise\nr1,powershell,cmd,noise\nr2,ws02,notepad,PowerShell -enc\nr3,ws03,net,noise\n", "id", "command", "host"),
        ("jsonl", "{\"id\":\"r0\",\"host\":\"ws01\",\"command\":\"powershell\",\"extra\":\"noise\"}\n{\"id\":\"r1\",\"host\":\"powershell\",\"command\":\"cmd\",\"extra\":\"noise\"}\n{\"id\":\"r2\",\"host\":\"ws02\",\"command\":\"notepad\",\"extra\":\"PowerShell -enc\"}\n{\"id\":\"r3\",\"host\":\"ws03\",\"command\":\"net\",\"extra\":\"noise\"}\n", "/id", "/command", "/host"),
    ] {
        let fixture = Fixture::new(extension, contents);
        let id_column = common::id(&fixture.columns, id_name);
        for (scope, expected) in [
            (None, vec![0, 1, 2]),
            (Some(vec![]), vec![0, 1, 2]),
            (Some(vec![common::id(&fixture.columns, command_name)]), vec![0]),
            (Some(vec![common::id(&fixture.columns, host_name)]), vec![1]),
        ] {
            for sorted in [false, true] {
                let mut expected = expected.clone();
                if sorted { expected.reverse(); }
                let spec = QuerySpec { condition: Some(unscoped()), search_columns: scope.clone(),
                    sort_key: sorted.then(|| id_column.clone()), sort_direction: Some("desc".into()),
                    ..Default::default() };
                clear_result_cache();
                assert_eq!(fixture.query(spec.clone(), 0, 20), expected, "{extension} {spec:?}");
                let projection = last_column_read_projection();
                if let Some(scope) = scope.as_ref().filter(|scope| !scope.is_empty()) {
                    assert!(scope.iter().all(|column| projection.contains(column)));
                    assert!(projection.iter().all(|column| scope.contains(column)
                        || (sorted && column == &id_column)), "{projection:?}");
                }
                // Each consumer must prepare its own cold persisted target.
                clear_result_cache();
                let destination = fixture.root.join("filtered.csv");
                ops::export::export_project(&fixture.store, ExportProjectPayload {
                    project_id: fixture.id, destination: destination.to_string_lossy().into_owned(),
                    scope: Some("filtered".into()), spec: spec.clone(), columns: Some(vec![id_column.clone()]),
                    row_indices: None, include_annotations: Some(false), allow_overwrite: true,
                }).unwrap();
                let mut reader = csv::Reader::from_path(&destination).unwrap();
                let values: Vec<String> = reader.records().map(|record| record.unwrap()[0].to_owned()).collect();
                assert_eq!(values, expected.iter().map(|row| format!("r{row}")).collect::<Vec<_>>());
                clear_result_cache();
                let preview = ops::bulk::bulk_preview(&fixture.store, BulkAnnotatePayload {
                    project_id: fixture.id, target: "filtered".into(), spec: spec.clone(), row_indices: None,
                    patch: AnnotationPatch { flag: Some("critical".into()), ..Default::default() }, preview_token: None,
                }).unwrap();
                assert_eq!(preview.count, expected.len());
                clear_result_cache();
                let stats = ops::stats::column_stats(&fixture.store, ops::stats::ColumnStatsPayload {
                    project_id: fixture.id, column: id_column.clone(), rare_max: 1, spec: spec.clone(),
                }).unwrap();
                assert_eq!(stats.unique, expected.len() as u64);
                assert_eq!(stats.top.iter().map(|entry| entry.value.clone()).collect::<std::collections::BTreeSet<_>>(),
                    expected.iter().map(|row| format!("r{row}")).collect());
                clear_result_cache();
                for row_index in 0..4 {
                    let located = locate_project_row(&fixture.store, LocateRowPayload {
                        project_id: fixture.id, row_index, spec: spec.clone(),
                    }).unwrap();
                    assert_eq!(located.position, expected.iter().position(|row| *row == row_index));
                }
            }
        }
    }
}

#[test]
fn streaming_search_matches_full_frame_across_batch_boundaries_and_annotation_revisions() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    const ROWS: usize = DEFAULT_BATCH_ROWS + PROJECT_BATCH_ROWS + 13;
    let mut contents = String::from("id,cohort,unused\n");
    for row in 0..ROWS {
        contents.push_str(&format!(
            "r{row},{},unused-{row}-{}\n",
            if row % 3 == 0 { "keep" } else { "drop" },
            "x".repeat(80)
        ));
    }
    let fixture = Fixture::new("csv", &contents);
    for row in [
        0,
        PROJECT_BATCH_ROWS - 1,
        PROJECT_BATCH_ROWS,
        PROJECT_BATCH_ROWS + 1,
        PROJECT_BATCH_ROWS * 2,
        DEFAULT_BATCH_ROWS - 1,
        DEFAULT_BATCH_ROWS,
        DEFAULT_BATCH_ROWS + 1,
        ROWS - 1,
    ] {
        ops::flags::update_flag(
            &fixture.store,
            UpdateFlagPayload {
                project_id: fixture.id,
                row_index: row,
                flag: Some(if row % 2 == 0 { "critical" } else { "safe" }.into()),
                memo: None,
                tags: Some(vec!["review".into()]),
                bookmarked: Some(row % 2 == 0),
            },
        )
        .unwrap();
    }
    let frame =
        read_project_dataframe(&fixture.store.project_dir(&fixture.id).join("data.parquet"))
            .unwrap();
    let flags = load_flags(&fixture.store.database_path(&fixture.id)).unwrap();
    let all_columns = fixture.columns.values().cloned().collect::<Vec<_>>();
    let cohort = common::id(&fixture.columns, "cohort");
    for spec in [
        QuerySpec {
            search: Some("keep".into()),
            search_columns: Some(vec![cohort.clone()]),
            ..Default::default()
        },
        QuerySpec {
            search: Some("keep @tag:review".into()),
            search_columns: Some(vec![cohort.clone()]),
            ..Default::default()
        },
        QuerySpec {
            search: Some("@tag:review".into()),
            ..Default::default()
        },
        QuerySpec {
            search: Some("-@tag:review".into()),
            ..Default::default()
        },
        QuerySpec {
            search: Some("keep".into()),
            search_columns: Some(vec![cohort.clone()]),
            flag_filter: Some("critical|none".into()),
            ..Default::default()
        },
        QuerySpec {
            flag_filter: Some("critical".into()),
            bookmarked_only: Some(true),
            ..Default::default()
        },
    ] {
        let reference =
            resolve_target(&frame, &spec, &all_columns, &flags, &JobContext::none()).unwrap();
        clear_result_cache();
        reset_project_read_counters();
        let target =
            resolve_project_target(&fixture.store, fixture.id, &spec, &JobContext::none()).unwrap();
        assert_eq!(&*target.ids, &reference.filtered, "{spec:?}");
        assert_eq!(
            target.annotated_filtered, reference.annotated_filtered,
            "{spec:?}"
        );
        assert_eq!(
            target.annotated_total, reference.annotated_total,
            "{spec:?}"
        );
        let reads = project_read_stats();
        assert!(reads.max_batch_rows <= PROJECT_BATCH_ROWS as u64);
        if spec.search_columns.is_some() {
            assert_eq!(reads.decoded_rows, ROWS as u64);
            assert!(
                reads.decoded_row_groups >= 2,
                "fixture must cross Parquet row groups"
            );
            assert_eq!(last_column_read_projection(), vec![cohort.clone()]);
        } else {
            assert_eq!(reads.decoded_rows, 0, "metadata query read source rows");
        }
        for offset in [
            0,
            reference.filtered.len() / 2,
            reference.filtered.len().saturating_sub(7),
        ] {
            assert_eq!(
                fixture.query(spec.clone(), offset, 7),
                reference
                    .filtered
                    .iter()
                    .skip(offset)
                    .take(7)
                    .copied()
                    .collect::<Vec<_>>()
            );
        }
        let cached =
            resolve_project_target(&fixture.store, fixture.id, &spec, &JobContext::none()).unwrap();
        assert!(Arc::ptr_eq(&target.ids, &cached.ids));
    }
    let spec = QuerySpec {
        search: Some("keep".into()),
        search_columns: Some(vec![cohort]),
        ..Default::default()
    };
    let before =
        resolve_project_target(&fixture.store, fixture.id, &spec, &JobContext::none()).unwrap();
    ops::flags::update_flag(
        &fixture.store,
        UpdateFlagPayload {
            project_id: fixture.id,
            row_index: PROJECT_BATCH_ROWS + 4,
            flag: Some("critical".into()),
            memo: None,
            tags: None,
            bookmarked: None,
        },
    )
    .unwrap();
    let after =
        resolve_project_target(&fixture.store, fixture.id, &spec, &JobContext::none()).unwrap();
    assert!(Arc::ptr_eq(&before.ids, &after.ids));
    assert_eq!(after.annotated_filtered, before.annotated_filtered + 1);
}

#[test]
fn cancellation_after_decoding_never_publishes_a_partial_streamed_result() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    const ROWS: usize = PROJECT_BATCH_ROWS * 4;
    let mut contents = String::from("id,message\n");
    for row in 0..ROWS {
        contents.push_str(&format!("r{row},keep {}\n", "x".repeat(512)));
    }
    let fixture = Fixture::new("csv", &contents);
    let spec = QuerySpec {
        search: Some("keep".into()),
        ..Default::default()
    };
    clear_result_cache();
    reset_project_read_counters();
    let guard = queue_job(JobKind::Query, Some(fixture.id)).unwrap();
    let job_id = guard.id();
    let context = JobContext::with_job(guard, None);
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let monitor_finished = finished.clone();
    let cancel = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !monitor_finished.load(std::sync::atomic::Ordering::Acquire)
            && std::time::Instant::now() < deadline
        {
            if project_read_stats().decoded_rows > 0 {
                return request_cancel(job_id);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        false
    });
    let outcome = resolve_project_target(&fixture.store, fixture.id, &spec, &context);
    finished.store(true, std::sync::atomic::Ordering::Release);
    assert!(
        cancel.join().unwrap(),
        "query finished before cancellation after first decoded batch"
    );
    let error = match outcome {
        Ok(_) => panic!("cancelled scan returned a result"),
        Err(error) => error,
    };
    assert_eq!(error.to_string(), "Cancelled.");
    assert!(
        project_read_stats().decoded_rows > 0,
        "cancelled before reading the batch"
    );
    assert!(
        project_read_stats().decoded_rows < ROWS as u64,
        "cancellation did not stop the scan early"
    );
    drop(context);
    reset_project_read_counters();
    let target =
        resolve_project_target(&fixture.store, fixture.id, &spec, &JobContext::none()).unwrap();
    assert_eq!(&*target.ids, &(0..ROWS).collect::<Vec<_>>());
    assert_eq!(
        project_read_stats().decoded_rows,
        ROWS as u64,
        "cancelled result entered the cache"
    );
}

#[test]
fn page_responses_report_the_annotation_snapshot_revision_on_all_query_paths() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new("csv", "id,message\nr0,keep\nr1,drop\nr2,keep\n");
    let query = |spec: QuerySpec| {
        query_project_rows(
            &fixture.store,
            QueryRowsPayload {
                project_id: fixture.id,
                spec,
                offset: Some(0),
                limit: Some(20),
            },
        )
        .unwrap()
    };
    let update = |row_index: usize, flag: &str, memo: &str| {
        ops::flags::update_flag(
            &fixture.store,
            UpdateFlagPayload {
                project_id: fixture.id,
                row_index,
                flag: Some(flag.into()),
                memo: Some(Some(memo.into())),
                tags: None,
                bookmarked: None,
            },
        )
        .unwrap()
    };
    let initial = query(QuerySpec::default());
    assert_eq!(
        initial.annotation_revision,
        trivium_engine::storage::project_revision(&fixture.store.project_dir(&fixture.id)).unwrap()
    );
    let first = update(0, "critical", "first");
    let unfiltered = query(QuerySpec::default());
    assert_eq!(unfiltered.annotation_revision, first.revision);
    assert_eq!(unfiltered.rows[0].flag, "critical");
    assert_eq!(
        (unfiltered.annotated_total, unfiltered.annotated_filtered),
        (1, 1)
    );

    let spec = QuerySpec {
        search: Some("keep".into()),
        ..Default::default()
    };
    clear_result_cache();
    let cold = query(spec.clone());
    let cached = query(spec.clone());
    assert_eq!(cold.annotation_revision, first.revision);
    assert_eq!(cached.annotation_revision, first.revision);
    assert_eq!(
        cold.rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );

    // A second writer changes the annotation after the data-only IDs cached.
    // The returned revision must describe the updated rows and their counts.
    let second = update(0, "safe", "second");
    for response in [query(spec.clone()), query(spec.clone())] {
        assert_eq!(response.annotation_revision, second.revision);
        assert_eq!(response.rows[0].flag, "safe");
        assert_eq!(response.rows[0].memo.as_deref(), Some("second"));
        assert_eq!(
            (response.annotated_total, response.annotated_filtered),
            (1, 1)
        );
    }
    let third = update(1, "critical", "outside results");
    for response in [query(spec.clone()), query(spec)] {
        assert_eq!(response.annotation_revision, third.revision);
        assert_eq!(
            (response.annotated_total, response.annotated_filtered),
            (2, 1)
        );
    }
}

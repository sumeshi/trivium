//! Auxiliary scans obey the same native cancellation and bounded-reader contract.
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use trivium_engine::{
    jobs::{request_cancel, try_begin_job, JobContext, JobKind, ProgressFn},
    ops::{
        self,
        diff::DiffProjectsPayload,
        dupes::{DupeMode, DupesPayload},
        projects::CreateProjectPayload,
        surround::{SurroundMode, SurroundingRowsPayload},
    },
    project_io::{
        project_full_reads, project_read_stats, reset_project_read_counters, PROJECT_BATCH_ROWS,
    },
    store::ProjectsStore,
};
use uuid::Uuid;
mod common;
static LOCK: Mutex<()> = Mutex::new(());

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    key: String,
    time: String,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-auxiliary-jobs-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source.csv");
        let mut csv = String::from("key,time,extra\n");
        for index in 0..(PROJECT_BATCH_ROWS * 3 + 17) {
            csv.push_str(&format!(
                "key-{},2025-01-01T00:00:00.{:09}Z,hidden-{}\n",
                index % 5,
                index,
                index
            ));
        }
        fs::write(&source, csv).unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
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
        let ids = common::column_ids(&store, id);
        Self {
            root,
            store,
            id,
            key: common::id(&ids, "key"),
            time: common::id(&ids, "time"),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn cancel_after_first_batch() -> JobContext {
    let guard = try_begin_job(JobKind::Analysis, None).unwrap();
    let emit: ProgressFn = Arc::new(|progress| {
        assert!(progress.done > 0);
        request_cancel(progress.id);
    });
    JobContext::with_job(guard, Some(emit))
}

fn assert_cancelled_after_one_batch(error: impl std::fmt::Display) {
    assert_eq!(error.to_string(), "Cancelled.");
    let io = project_read_stats();
    assert_eq!(io.decoded_rows, PROJECT_BATCH_ROWS as u64);
    assert_eq!(io.max_batch_rows, PROJECT_BATCH_ROWS as u64);
    assert_eq!(project_full_reads(), 0);
}

#[test]
fn duplicate_scan_cancels_at_batch_boundary_before_full_row_payloads() {
    let _lock = LOCK.lock().unwrap();
    let data = Fixture::new();
    reset_project_read_counters();
    let context = cancel_after_first_batch();
    let error = ops::dupes::duplicate_rows_with_job(
        &data.store,
        DupesPayload {
            project_id: data.id,
            columns: vec![data.key.clone()],
            mode: DupeMode::Duplicates,
        },
        &context,
    )
    .unwrap_err();
    assert_cancelled_after_one_batch(error);
}

#[test]
fn diff_scan_cancels_before_reading_the_second_project_or_replacing_output() {
    let _lock = LOCK.lock().unwrap();
    let data = Fixture::new();
    let output = data.root.join("existing.csv");
    fs::write(&output, "previous report").unwrap();
    reset_project_read_counters();
    let context = cancel_after_first_batch();
    let error = ops::diff::diff_projects_with_job(
        &data.store,
        DiffProjectsPayload {
            project_id_a: data.id,
            project_id_b: data.id,
            key_columns: vec![data.key.clone()],
            destination: Some(output.to_string_lossy().into()),
        },
        &context,
    )
    .unwrap_err();
    assert_cancelled_after_one_batch(error);
    assert_eq!(fs::read_to_string(output).unwrap(), "previous report");
    assert!(fs::read_dir(&data.root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".trivium-export-")));
}

#[test]
fn surrounding_scan_cancels_before_sort_or_wide_row_reads() {
    let _lock = LOCK.lock().unwrap();
    let data = Fixture::new();
    reset_project_read_counters();
    let context = cancel_after_first_batch();
    let error = ops::surround::surrounding_rows_with_job(
        &data.store,
        SurroundingRowsPayload {
            project_id: data.id,
            row_index: 12_000,
            column: data.time.clone(),
            mode: SurroundMode::Neighbors,
            count: 5,
        },
        &context,
    )
    .unwrap_err();
    assert_cancelled_after_one_batch(error);
}

#[test]
fn bounded_duplicate_scan_preserves_ids_and_hidden_cells_across_batches() {
    let _lock = LOCK.lock().unwrap();
    let data = Fixture::new();
    reset_project_read_counters();
    let response = ops::dupes::duplicate_rows(
        &data.store,
        DupesPayload {
            project_id: data.id,
            columns: vec![data.key.clone(), data.key.clone()],
            mode: DupeMode::Duplicates,
        },
    )
    .unwrap();
    assert_eq!(response.rows.len(), PROJECT_BATCH_ROWS * 3 + 17);
    let extra = common::id(&common::column_ids(&data.store, data.id), "extra");
    for (index, row) in response.rows.iter().enumerate() {
        assert_eq!(row.row_index, index);
        assert_eq!(
            common::cell_text(row, &extra),
            Some(format!("hidden-{index}").as_str())
        );
    }
    assert_eq!(project_full_reads(), 0);
    assert!(project_read_stats().max_batch_rows <= PROJECT_BATCH_ROWS as u64);
}

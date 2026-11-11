//! T15: job registry, cancellation targeting, and progress throttling.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use uuid::Uuid;

use trivium_engine::jobs::{
    active_jobs, is_cancelled, request_cancel, try_begin_job, JobContext, JobKind, JobProgress,
    ProgressFn, ThrottledProgress, MAX_CONCURRENT_JOBS,
};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{query_project_rows_with_job, QueryRowsPayload, QuerySpec};
use trivium_engine::store::ProjectsStore;

/// The job registry is process-global, so tests that change it run one at a
/// time even though the test harness uses threads.
static REGISTRY_TEST_LOCK: Mutex<()> = Mutex::new(());

fn registry_test_lock() -> std::sync::MutexGuard<'static, ()> {
    REGISTRY_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn concurrency_limit_rejects_a_third_job() {
    let _lock = registry_test_lock();
    assert!(active_jobs().is_empty());

    let first = try_begin_job(JobKind::Query, None).expect("first job");
    let second = try_begin_job(JobKind::Export, None).expect("second job");
    assert_eq!(active_jobs().len(), MAX_CONCURRENT_JOBS);

    let error = try_begin_job(JobKind::Import, None)
        .err()
        .expect("third job must be rejected");
    assert!(
        error
            .to_string()
            .contains("Too many long-running operations"),
        "unexpected error: {error}"
    );

    // Finishing one job makes room for exactly one more.
    drop(first);
    assert_eq!(active_jobs().len(), 1);
    let third = try_begin_job(JobKind::Import, None).expect("job after releasing one slot");
    assert_eq!(third.kind(), JobKind::Import);
    assert_eq!(active_jobs().len(), MAX_CONCURRENT_JOBS);

    drop(second);
    drop(third);
    assert!(active_jobs().is_empty());
}

#[test]
fn cancel_targets_only_the_requested_job() {
    let _lock = registry_test_lock();
    let project_id = Uuid::new_v4();
    let first = try_begin_job(JobKind::Query, Some(project_id)).expect("first job");
    let second = try_begin_job(JobKind::Export, None).expect("second job");

    assert_eq!(first.info().project_id, Some(project_id));
    assert!(request_cancel(first.id()));
    assert!(first.is_cancelled());
    assert!(!second.is_cancelled());
    assert!(!is_cancelled(second.id()));

    let error = first
        .check_cancelled()
        .err()
        .expect("the cancelled job must fail its checkpoint");
    assert_eq!(error.to_string(), "Cancelled.");
    assert!(second.check_cancelled().is_ok());

    assert!(!request_cancel(u64::MAX));
    assert_eq!(active_jobs().len(), 2);
}

#[test]
fn finished_job_cannot_cancel_or_change_a_new_job() {
    let _lock = registry_test_lock();
    let old = try_begin_job(JobKind::Query, None).expect("old job");
    let old_id = old.id();
    drop(old);

    // The ID is gone: a late cancel is rejected and cannot affect anything.
    assert!(!request_cancel(old_id));
    assert!(!is_cancelled(old_id));

    let new = try_begin_job(JobKind::Query, None).expect("new job");
    assert_ne!(new.id(), old_id);
    assert!(!new.is_cancelled());

    // Repeating the old cancel after the new job started still does nothing.
    assert!(!request_cancel(old_id));
    assert!(!new.is_cancelled());
    assert!(new.check_cancelled().is_ok());
}

#[test]
fn progress_is_throttled_to_the_interval() {
    let clock = Arc::new(AtomicU64::new(0));
    let emitted = Arc::new(Mutex::new(Vec::<JobProgress>::new()));

    let clock_for_progress = Arc::clone(&clock);
    let sink = Arc::clone(&emitted);
    let emit: ProgressFn = Arc::new(move |progress| {
        sink.lock().expect("progress sink").push(progress);
    });
    let mut progress = ThrottledProgress::with_clock(
        7,
        JobKind::Query,
        Duration::from_millis(200),
        move || Duration::from_millis(clock_for_progress.load(Ordering::SeqCst)),
        Some(emit),
    );

    // First report is immediate.
    assert!(progress.report(1, Some(10), None));
    clock.store(100, Ordering::SeqCst);
    assert!(!progress.report(2, Some(10), None));
    clock.store(199, Ordering::SeqCst);
    assert!(!progress.report(3, Some(10), None));
    clock.store(200, Ordering::SeqCst);
    assert!(progress.report(4, Some(10), Some("half".into())));
    clock.store(399, Ordering::SeqCst);
    assert!(!progress.report(5, Some(10), None));
    // The final report is emitted even inside the throttle window.
    clock.store(410, Ordering::SeqCst);
    assert!(progress.report(10, Some(10), Some("done".into())));
    clock.store(420, Ordering::SeqCst);
    assert!(!progress.report(9, Some(10), None));

    let values = emitted.lock().expect("emitted progress");
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].id, 7);
    assert_eq!(values[0].kind, JobKind::Query);
    assert_eq!(values[1].done, 4);
    assert_eq!(values[1].message.as_deref(), Some("half"));
    assert_eq!(values[2].done, 10);
}

#[test]
fn empty_context_never_cancels_or_emits() {
    let context = JobContext::none();
    assert_eq!(context.id(), None);
    assert!(!context.is_cancelled());
    assert!(context.check_cancelled().is_ok());
    assert!(!context.report_progress(1, Some(1), None));
}

#[test]
fn job_context_observes_cancel_and_emits_progress() {
    let _lock = registry_test_lock();
    let guard = try_begin_job(JobKind::Query, None).expect("job");
    let id = guard.id();
    let emitted = Arc::new(Mutex::new(Vec::<JobProgress>::new()));
    let sink = Arc::clone(&emitted);
    let emit: ProgressFn = Arc::new(move |progress| {
        sink.lock().expect("progress sink").push(progress);
    });
    let context = JobContext::with_job(guard, Some(emit));

    assert!(context.report_progress(1, Some(2), Some("half".into())));
    assert!(context.check_cancelled().is_ok());
    assert!(request_cancel(id));

    let error = context
        .check_cancelled()
        .err()
        .expect("cancelled context must fail");
    assert_eq!(error.to_string(), "Cancelled.");

    let values = emitted.lock().expect("emitted progress");
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].id, id);
}

#[test]
fn cancelled_query_stops_before_reading_project_data() {
    let _lock = registry_test_lock();
    let root = std::env::temp_dir().join(format!("trivium-jobs-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&root).expect("temp dir");
    let store = ProjectsStore::new(root.clone()).expect("store");

    let guard = try_begin_job(JobKind::Query, None).expect("job");
    assert!(request_cancel(guard.id()));
    let context = JobContext::with_job(guard, None);
    let payload = QueryRowsPayload {
        project_id: Uuid::new_v4(),
        spec: QuerySpec::default(),
        offset: None,
        limit: None,
    };

    let error = query_project_rows_with_job(&store, payload, &context)
        .err()
        .expect("cancelled query must fail");
    assert_eq!(error.to_string(), "Cancelled.");

    // A cancelled import is rejected before touching the selected path.
    let guard = try_begin_job(JobKind::Import, None).expect("import job");
    assert!(request_cancel(guard.id()));
    let context = JobContext::with_job(guard, None);
    let error = trivium_engine::ops::projects::create_project_with_job(
        &store,
        CreateProjectPayload {
            path: root.join("missing.csv").to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
        &context,
    )
    .err()
    .expect("cancelled import must fail");
    assert_eq!(error.to_string(), "Cancelled.");

    let _ = std::fs::remove_dir_all(root);
}

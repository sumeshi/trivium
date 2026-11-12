//! Native admission, cancellation ownership and cleanup without a Tokio runtime.
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use trivium_engine::jobs::{
    active_jobs, cancel_request_jobs, queue_job, queue_job_with_request, request_cancel,
    scheduler_stats, JobContext, JobKind, MAX_CONCURRENT_JOBS, MAX_QUEUED_JOBS,
};

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct ThreadWake(thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

#[test]
fn concurrent_work_is_bounded_and_waiting_work_eventually_runs() {
    let _lock = TEST_LOCK.lock().unwrap();
    let guards = (0..12)
        .map(|_| queue_job(JobKind::Analysis, None).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(scheduler_stats().running, MAX_CONCURRENT_JOBS);
    assert_eq!(scheduler_stats().queued, 10);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let first_pair = Arc::new(std::sync::Barrier::new(2));
    let threads = guards
        .into_iter()
        .enumerate()
        .map(|(index, mut guard)| {
            let first_pair = first_pair.clone();
            let (active, peak, completed) = (active.clone(), peak.clone(), completed.clone());
            thread::spawn(move || {
                block_on(guard.wait_for_turn()).unwrap();
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                if index < 2 {
                    first_pair.wait();
                }
                thread::sleep(Duration::from_millis(10));
                active.fetch_sub(1, Ordering::SeqCst);
                completed.fetch_add(1, Ordering::SeqCst);
                drop(guard);
            })
        })
        .collect::<Vec<_>>();
    for worker in threads {
        worker.join().unwrap();
    }
    assert_eq!(peak.load(Ordering::SeqCst), MAX_CONCURRENT_JOBS);
    assert_eq!(completed.load(Ordering::SeqCst), 12);
    assert!(active_jobs().is_empty());
}

#[test]
fn cancelled_queued_work_wakes_without_starting_cpu_work() {
    let _lock = TEST_LOCK.lock().unwrap();
    let first = queue_job(JobKind::Query, None).unwrap();
    let second = queue_job(JobKind::Query, None).unwrap();
    let mut queued = queue_job(JobKind::Analysis, None).unwrap();
    let id = queued.id();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        ready_tx.send(()).unwrap();
        let result = block_on(queued.wait_for_turn());
        assert!(!queued.is_running());
        done_tx.send(result.unwrap_err().to_string()).unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(request_cancel(id));
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "Cancelled."
    );
    worker.join().unwrap();
    assert_eq!(scheduler_stats().queued, 0);
    assert_eq!(scheduler_stats().running, 2);
    drop((first, second));
    assert!(active_jobs().is_empty());
}

#[test]
fn interactive_fifo_has_priority_without_starving_background_work() {
    let _lock = TEST_LOCK.lock().unwrap();
    let first = queue_job(JobKind::Query, None).unwrap();
    let second = queue_job(JobKind::Query, None).unwrap();
    let background = queue_job(JobKind::Export, None).unwrap();
    let queries = (0..4)
        .map(|_| queue_job(JobKind::Query, None).unwrap())
        .collect::<Vec<_>>();
    drop(first);
    assert!(queries[0].is_running());
    assert!(!background.is_running());
    drop(second);
    assert!(queries[1].is_running());
    // Four interactive admissions (the two originals included) earn the
    // oldest background request a turn ahead of the remaining queries.
    let mut queries = queries.into_iter();
    drop(queries.next());
    assert!(background.is_running());
    let second_query = queries.next().unwrap();
    assert!(second_query.is_running());
    drop(second_query);
    assert!(queries.as_slice()[0].is_running());
    drop(queries);
    drop(background);
    assert!(active_jobs().is_empty());
}

#[test]
fn errors_panics_and_dropped_waiters_release_every_permit() {
    let _lock = TEST_LOCK.lock().unwrap();
    let first = queue_job(JobKind::Query, None).unwrap();
    let second = queue_job(JobKind::Query, None).unwrap();
    let queued = queue_job(JobKind::Analysis, None).unwrap();
    drop(queued);
    assert_eq!(scheduler_stats().queued, 0);
    let next = queue_job(JobKind::Analysis, None).unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = first;
        panic!("worker panic");
    }));
    assert!(panic.is_err());
    assert!(next.is_running());
    let failed: Result<(), &str> = {
        let _guard = second;
        Err("worker error")
    };
    assert!(failed.is_err());
    drop(next);
    assert!(active_jobs().is_empty());
}

#[test]
fn pending_queue_is_bounded_and_reusable_after_cancellation() {
    let _lock = TEST_LOCK.lock().unwrap();
    let first = queue_job(JobKind::Query, None).unwrap();
    let second = queue_job(JobKind::Query, None).unwrap();
    let mut queued = (0..MAX_QUEUED_JOBS)
        .map(|_| queue_job(JobKind::Analysis, None).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(active_jobs().len(), MAX_QUEUED_JOBS + MAX_CONCURRENT_JOBS);
    assert!(queue_job(JobKind::Analysis, None)
        .err()
        .unwrap()
        .to_string()
        .contains("queue is full"));
    let mut cancelled = queued.pop().unwrap();
    request_cancel(cancelled.id());
    assert_eq!(
        block_on(cancelled.wait_for_turn()).unwrap_err().to_string(),
        "Cancelled."
    );
    drop(cancelled);
    let replacement = queue_job(JobKind::Analysis, None).unwrap();
    assert_eq!(scheduler_stats().queued, MAX_QUEUED_JOBS);
    drop((queued, replacement, first, second));
    assert!(active_jobs().is_empty());
}

#[test]
fn request_ownership_handles_early_cancel_and_preserves_writes() {
    let _lock = TEST_LOCK.lock().unwrap();
    let owner = uuid::Uuid::new_v4().to_string();
    let other = uuid::Uuid::new_v4().to_string();
    let read = queue_job_with_request(JobKind::Analysis, None, Some(owner.clone())).unwrap();
    let unrelated = queue_job_with_request(JobKind::Query, None, Some(other)).unwrap();
    let mutation = queue_job_with_request(JobKind::Mutation, None, Some(owner.clone())).unwrap();
    assert_eq!(cancel_request_jobs(&owner), 1);
    assert!(read.is_cancelled());
    assert!(!unrelated.is_cancelled());
    assert!(!mutation.is_cancelled());
    assert_eq!(cancel_request_jobs(&owner), 0);
    assert_eq!(
        queue_job_with_request(JobKind::Load, None, Some(owner.clone()))
            .err()
            .unwrap()
            .to_string(),
        "Cancelled."
    );
    let export = queue_job_with_request(JobKind::Export, None, Some(owner.clone())).unwrap();
    let import = queue_job_with_request(JobKind::Import, None, Some(owner)).unwrap();
    assert!(!export.is_cancelled());
    assert!(!import.is_cancelled());
    assert!(request_cancel(mutation.id()));
    assert!(mutation.is_cancelled());
    drop((read, unrelated, mutation, export, import));
    assert!(queue_job_with_request(JobKind::Query, None, Some(String::new())).is_err());
    assert!(queue_job_with_request(JobKind::Query, None, Some("a".repeat(129))).is_err());
    assert!(active_jobs().is_empty());
}

#[test]
fn early_cancel_tombstones_have_bounded_retention() {
    let _lock = TEST_LOCK.lock().unwrap();
    let prefix = uuid::Uuid::new_v4().to_string();
    let oldest = format!("{prefix}-0");
    for index in 0..1025 {
        cancel_request_jobs(&format!("{prefix}-{index}"));
    }
    let admitted = queue_job_with_request(JobKind::Query, None, Some(oldest)).unwrap();
    assert!(
        queue_job_with_request(JobKind::Analysis, None, Some(format!("{prefix}-1024"))).is_err()
    );
    drop(admitted);
}

#[test]
fn running_context_releases_permit_after_batch_boundary_cancellation() {
    let _lock = TEST_LOCK.lock().unwrap();
    let guard = queue_job(JobKind::Analysis, None).unwrap();
    let id = guard.id();
    let context = JobContext::with_job(guard, None);
    let (batch_tx, batch_rx) = mpsc::channel();
    let (continue_tx, continue_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        context.check_cancelled().unwrap();
        batch_tx.send(()).unwrap();
        continue_rx.recv().unwrap();
        context.check_cancelled()
    });
    batch_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    request_cancel(id);
    assert_eq!(scheduler_stats().running, 1);
    continue_tx.send(()).unwrap();
    assert_eq!(
        worker.join().unwrap().unwrap_err().to_string(),
        "Cancelled."
    );
    assert!(active_jobs().is_empty());
}

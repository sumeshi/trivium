use super::*;
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

fn receive<T: std::fmt::Debug>(receiver: &mut Receiver<Result<T, String>>) -> Result<T, String> {
    receive_value(receiver)
}

fn receive_value<T>(receiver: &mut Receiver<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Ok(value) = receiver.try_recv() {
            return value;
        }
        assert!(Instant::now() < deadline, "storage result did not arrive");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn sealing_drains_running_and_queued_work_without_accepting_more() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let case = Uuid::new_v4();
    let (started, running) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let mut first = pool
        .submit(case, move || {
            started.send(()).unwrap();
            gate.recv().unwrap();
            Ok(1)
        })
        .unwrap();
    running.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut next = pool.submit(case, || Ok(2)).unwrap();
    let mut drained = pool.seal_and_drain().unwrap();
    assert!(drained.try_recv().is_err());
    assert!(pool.submit(case, || Ok(3)).unwrap_err().contains("closing"));
    release.send(()).unwrap();
    assert_eq!(receive(&mut first).unwrap(), 1);
    assert_eq!(receive(&mut next).unwrap(), 2);
    assert!(receive_value(&mut drained).result.is_ok());
}

#[test]
fn failed_write_is_reported_even_when_its_reply_was_dropped() {
    let pool = StorageWorker::new(1, 8).unwrap();
    drop(
        pool.submit::<(), _>(Uuid::new_v4(), || {
            Err(AppError::Message("disk failed".into()))
        })
        .unwrap(),
    );
    let mut drained = pool.seal_and_drain().unwrap();
    let failure = receive_value(&mut drained);
    assert!(failure.result.unwrap_err().contains("disk failed"));
    pool.resume(Some(failure.failure_sequence));
    let mut next = pool.submit(Uuid::new_v4(), || Ok(7)).unwrap();
    assert_eq!(receive(&mut next).unwrap(), 7);
    let mut drained = pool.seal_and_drain().unwrap();
    assert!(receive_value(&mut drained).result.is_ok());
}

#[test]
fn read_errors_do_not_poison_the_save_failure_ledger() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let mut read = pool
        .submit_inner::<(), _>(Uuid::new_v4(), false, || {
            Err(AppError::Message("read failed".into()))
        })
        .unwrap();
    assert!(receive(&mut read).is_err());
    let mut drained = pool.seal_and_drain().unwrap();
    assert!(receive_value(&mut drained).result.is_ok());
}

#[test]
fn acknowledging_one_close_attempt_never_clears_a_later_failure() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let mut first = pool
        .submit::<(), _>(Uuid::new_v4(), || Err(AppError::Message("first".into())))
        .unwrap();
    assert!(receive(&mut first).is_err());
    let mut drained = pool.seal_and_drain().unwrap();
    let observed = receive_value(&mut drained);
    pool.resume(None);
    let mut next = pool
        .submit::<(), _>(Uuid::new_v4(), || Err(AppError::Message("later".into())))
        .unwrap();
    assert!(receive(&mut next).is_err());
    // A reply can arrive just before the worker records completion. The drain
    // barrier includes that completion, not merely the received reply.
    let mut latest = pool.seal_and_drain().unwrap();
    let latest = receive_value(&mut latest);
    assert!(latest.failure_sequence > observed.failure_sequence);
    pool.resume(Some(observed.failure_sequence));
    let mut drained = pool.seal_and_drain().unwrap();
    assert!(receive_value(&mut drained)
        .result
        .unwrap_err()
        .contains("later"));
}

#[test]
fn cancelling_a_drain_retains_failures_from_still_running_work() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let (started, running) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let mut write = pool
        .submit::<(), _>(Uuid::new_v4(), move || {
            started.send(()).unwrap();
            gate.recv().unwrap();
            Err(AppError::Message("late failure".into()))
        })
        .unwrap();
    running.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut drained = pool.seal_and_drain().unwrap();
    pool.resume(None);
    assert!(receive_value(&mut drained)
        .result
        .unwrap_err()
        .contains("cancelled"));
    release.send(()).unwrap();
    assert!(receive(&mut write).is_err());
    let mut drained = pool.seal_and_drain().unwrap();
    assert!(receive_value(&mut drained)
        .result
        .unwrap_err()
        .contains("late failure"));
}

#[test]
fn storage_runs_off_caller_thread_and_is_independent_of_heavy_job_admission() {
    use trivium_engine::jobs::{try_begin_job, JobKind, MAX_CONCURRENT_JOBS};
    let jobs: Vec<_> = (0..MAX_CONCURRENT_JOBS)
        .map(|_| try_begin_job(JobKind::Query, None).unwrap())
        .collect();
    let pool = StorageWorker::new(2, 8).unwrap();
    let caller = thread::current().id();
    let mut reply = pool
        .submit(Uuid::new_v4(), move || Ok(thread::current().id()))
        .unwrap();
    assert_ne!(receive(&mut reply).unwrap(), caller);
    drop(jobs);
}

#[test]
fn case_writes_stay_fifo_while_another_case_can_save() {
    let pool = StorageWorker::new(2, 8).unwrap();
    let case = Uuid::new_v4();
    let (started, running) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let sequence = Arc::new(Mutex::new(Vec::new()));
    let first_sequence = sequence.clone();
    let mut first = pool
        .submit(case, move || {
            started.send(()).unwrap();
            gate.recv().unwrap();
            first_sequence.lock().push(1);
            Ok(1)
        })
        .unwrap();
    running.recv_timeout(Duration::from_secs(2)).unwrap();
    let next_sequence = sequence.clone();
    let mut second = pool
        .submit(case, move || {
            next_sequence.lock().push(2);
            Ok(2)
        })
        .unwrap();
    let mut other = pool.submit(Uuid::new_v4(), || Ok(3)).unwrap();
    assert_eq!(
        receive(&mut other).unwrap(),
        3,
        "a blocked case must not block another case"
    );
    assert!(second.try_recv().is_err(), "same-case work must still wait");
    release.send(()).unwrap();
    assert_eq!(receive(&mut first).unwrap(), 1);
    assert_eq!(receive(&mut second).unwrap(), 2);
    assert_eq!(*sequence.lock(), vec![1, 2]);
}

#[test]
fn bounded_queue_rejects_excess_work_without_losing_accepted_saves() {
    let pool = StorageWorker::new(1, 1).unwrap();
    let case = Uuid::new_v4();
    let (started, running) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let mut first = pool
        .submit(case, move || {
            started.send(()).unwrap();
            gate.recv().unwrap();
            Ok(1)
        })
        .unwrap();
    running.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut second = pool.submit(case, || Ok(2)).unwrap();
    let rejected = pool.submit(case, || Ok(3));
    assert!(matches!(rejected, Err(ref error) if error.contains("storage queue is full")));
    release.send(()).unwrap();
    assert_eq!(receive(&mut first).unwrap(), 1);
    assert_eq!(receive(&mut second).unwrap(), 2);
}

#[test]
fn failed_or_panicking_save_releases_the_case_for_later_writes() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let case = Uuid::new_v4();
    let mut failed = pool
        .submit::<(), _>(case, || Err(AppError::Message("Write failed".into())))
        .unwrap();
    let mut panicked = pool
        .submit::<(), _>(case, || panic!("synthetic storage panic"))
        .unwrap();
    let mut later = pool.submit(case, || Ok(7)).unwrap();
    assert_eq!(receive(&mut failed).unwrap_err(), "Write failed");
    assert_eq!(
        receive(&mut panicked).unwrap_err(),
        "Storage operation panicked."
    );
    assert_eq!(receive(&mut later).unwrap(), 7);
}

#[test]
fn dropping_a_reply_does_not_cancel_an_accepted_save() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let case = Uuid::new_v4();
    let saved = Arc::new(Mutex::new(false));
    let write_saved = saved.clone();
    drop(
        pool.submit(case, move || {
            *write_saved.lock() = true;
            Ok(())
        })
        .unwrap(),
    );
    let mut after = pool.submit(case, move || Ok(*saved.lock())).unwrap();
    assert!(receive(&mut after).unwrap());
}

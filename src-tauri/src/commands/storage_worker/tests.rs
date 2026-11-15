use super::*;
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

fn receive<T: std::fmt::Debug>(receiver: &mut Receiver<Result<T, String>>) -> Result<T, String> {
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

//! Short storage work has its own bounded workers, separate from heavy readers.
use std::{
    collections::{HashSet, VecDeque},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, OnceLock},
    thread::{self, JoinHandle},
};

use parking_lot::{Condvar, Mutex};
use tauri::async_runtime::{channel, Receiver, Sender};
use trivium_engine::error::AppError;
use uuid::Uuid;

const WORKERS: usize = 2;
const MAX_PENDING: usize = 128;

struct Task {
    project: Uuid,
    work: Box<dyn FnOnce() -> Option<String> + Send>,
}

pub(super) struct DrainOutcome {
    pub result: Result<(), String>,
    pub failure_sequence: u64,
}

#[derive(Default)]
struct Queue {
    tasks: VecDeque<Task>,
    running: HashSet<Uuid>,
    closed: bool,
    sealed: bool,
    failure: Option<(u64, String)>,
    failure_sequence: u64,
    drain: Option<Sender<DrainOutcome>>,
}

struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
    max_pending: usize,
}

pub(super) struct StorageWorker {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
}

impl StorageWorker {
    pub(super) fn new(workers: usize, max_pending: usize) -> Result<Self, String> {
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue::default()),
            ready: Condvar::new(),
            max_pending,
        });
        let mut pool = Self {
            shared,
            threads: Vec::new(),
        };
        for index in 0..workers {
            let shared = pool.shared.clone();
            pool.threads.push(
                thread::Builder::new()
                    .name(format!("trivium-storage-{index}"))
                    .spawn(move || worker(shared))
                    .map_err(|error| format!("Failed to start storage worker: {error}"))?,
            );
        }
        Ok(pool)
    }

    pub(super) fn submit<T, F>(
        &self,
        project: Uuid,
        work: F,
    ) -> Result<Receiver<Result<T, String>>, String>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, AppError> + Send + 'static,
    {
        self.submit_inner(project, true, work)
    }

    fn submit_inner<T, F>(
        &self,
        project: Uuid,
        writes: bool,
        work: F,
    ) -> Result<Receiver<Result<T, String>>, String>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, AppError> + Send + 'static,
    {
        let (sender, receiver) = channel(1);
        let mut queue = self.shared.queue.lock();
        if queue.sealed {
            return Err("The application is closing. Wait or keep it open before editing.".into());
        }
        if queue.closed || queue.tasks.len() >= self.shared.max_pending {
            return Err("The storage queue is full. Wait for a save to finish.".into());
        }
        queue.tasks.push_back(Task {
            project,
            work: Box::new(move || {
                let result = catch_unwind(AssertUnwindSafe(work))
                    .map_err(|_| "Storage operation panicked.".to_string())
                    .and_then(|result| result.map_err(String::from));
                let failure = writes.then(|| result.as_ref().err().cloned()).flatten();
                // Closing a tab drops its reply, never an accepted save.
                let _ = sender.blocking_send(result);
                failure
            }),
        });
        drop(queue);
        self.shared.ready.notify_all();
        Ok(receiver)
    }

    /// Seal admission and obtain a nonblocking receipt for all accepted work,
    /// including running tasks and failures whose IPC reply was dropped.
    pub(super) fn seal_and_drain(&self) -> Result<Receiver<DrainOutcome>, String> {
        let (sender, receiver) = channel(1);
        let mut queue = self.shared.queue.lock();
        if queue.closed || queue.sealed {
            return Err("The application is already closing.".into());
        }
        queue.sealed = true;
        if queue.tasks.is_empty() && queue.running.is_empty() {
            let _ = sender.try_send(drain_result(&queue));
        } else {
            queue.drain = Some(sender);
        }
        Ok(receiver)
    }

    /// A cancellation acknowledges errors already reported by this attempt;
    /// failures from still-running work are recorded when that work completes.
    pub(super) fn resume(&self, acknowledge_through: Option<u64>) {
        let mut queue = self.shared.queue.lock();
        queue.sealed = false;
        if queue.failure.as_ref().is_some_and(|(sequence, _)| {
            acknowledge_through.is_some_and(|acknowledged| *sequence <= acknowledged)
        }) {
            queue.failure = None;
        }
        if let Some(waiter) = queue.drain.take() {
            let _ = waiter.try_send(DrainOutcome {
                result: Err("Closing was cancelled.".into()),
                failure_sequence: 0,
            });
        }
    }
}

fn drain_result(queue: &Queue) -> DrainOutcome {
    DrainOutcome {
        result: match &queue.failure {
            Some((_, error)) => Err(format!("A storage write failed: {error}")),
            None => Ok(()),
        },
        failure_sequence: queue.failure_sequence,
    }
}

fn worker(shared: Arc<Shared>) {
    loop {
        let task = {
            let mut queue = shared.queue.lock();
            loop {
                // Keep FIFO within each case. A locked case must not occupy
                // both workers or prevent another case from saving.
                if let Some(index) = queue
                    .tasks
                    .iter()
                    .position(|task| !queue.running.contains(&task.project))
                {
                    let task = queue.tasks.remove(index).expect("queued task");
                    queue.running.insert(task.project);
                    break task;
                }
                if queue.closed {
                    return;
                }
                shared.ready.wait(&mut queue);
            }
        };
        let failure = (task.work)();
        let mut queue = shared.queue.lock();
        if let Some(failure) = failure {
            queue.failure_sequence += 1;
            queue.failure = Some((queue.failure_sequence, failure));
        }
        queue.running.remove(&task.project);
        if queue.tasks.is_empty() && queue.running.is_empty() {
            if let Some(waiter) = queue.drain.take() {
                let _ = waiter.try_send(drain_result(&queue));
            }
        }
        drop(queue);
        shared.ready.notify_all();
    }
}

impl Drop for StorageWorker {
    fn drop(&mut self) {
        self.shared.queue.lock().closed = true;
        self.shared.ready.notify_all();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

/// Only queue admission and awaiting a reply run on the async runtime. DB/file
/// calls execute on two dedicated threads and retain case submission order.
pub(super) fn storage_worker() -> Result<&'static StorageWorker, String> {
    static POOL: OnceLock<Result<StorageWorker, String>> = OnceLock::new();
    POOL.get_or_init(|| StorageWorker::new(WORKERS, MAX_PENDING))
        .as_ref()
        .map_err(Clone::clone)
}

pub(crate) async fn run_storage<T, F>(project: Uuid, work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    let pool = storage_worker()?;
    let mut reply = pool.submit(project, work)?;
    reply
        .recv()
        .await
        .ok_or_else(|| "Storage worker stopped before returning a result.".to_string())?
}

pub(crate) async fn run_storage_read<T, F>(project: Uuid, work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    let mut reply = storage_worker()?.submit_inner(project, false, work)?;
    reply
        .recv()
        .await
        .ok_or_else(|| "Storage worker stopped before returning a result.".to_string())?
}

#[cfg(test)]
mod tests;

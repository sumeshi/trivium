//! Short storage work has its own bounded workers, separate from heavy readers.
use std::{
    collections::{HashSet, VecDeque},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, OnceLock},
    thread::{self, JoinHandle},
};

use parking_lot::{Condvar, Mutex};
use tauri::async_runtime::{channel, Receiver};
use trivium_engine::error::AppError;
use uuid::Uuid;

const WORKERS: usize = 2;
const MAX_PENDING: usize = 128;

struct Task {
    project: Uuid,
    work: Box<dyn FnOnce() + Send>,
}

#[derive(Default)]
struct Queue {
    tasks: VecDeque<Task>,
    running: HashSet<Uuid>,
    closed: bool,
}

struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
    max_pending: usize,
}

struct StorageWorker {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
}

impl StorageWorker {
    fn new(workers: usize, max_pending: usize) -> Result<Self, String> {
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

    fn submit<T, F>(&self, project: Uuid, work: F) -> Result<Receiver<Result<T, String>>, String>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, AppError> + Send + 'static,
    {
        let (sender, receiver) = channel(1);
        let mut queue = self.shared.queue.lock();
        if queue.closed || queue.tasks.len() >= self.shared.max_pending {
            return Err("The storage queue is full. Wait for a save to finish.".into());
        }
        queue.tasks.push_back(Task {
            project,
            work: Box::new(move || {
                let result = catch_unwind(AssertUnwindSafe(work))
                    .map_err(|_| "Storage operation panicked.".to_string())
                    .and_then(|result| result.map_err(String::from));
                // Closing a tab drops its reply, never an accepted save.
                let _ = sender.blocking_send(result);
            }),
        });
        drop(queue);
        self.shared.ready.notify_all();
        Ok(receiver)
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
        (task.work)();
        shared.queue.lock().running.remove(&task.project);
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
pub(crate) async fn run_storage<T, F>(project: Uuid, work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    static POOL: OnceLock<Result<StorageWorker, String>> = OnceLock::new();
    let pool = POOL
        .get_or_init(|| StorageWorker::new(WORKERS, MAX_PENDING))
        .as_ref()
        .map_err(Clone::clone)?;
    let mut reply = pool.submit(project, work)?;
    reply
        .recv()
        .await
        .ok_or_else(|| "Storage worker stopped before returning a result.".to_string())?
}

#[cfg(test)]
mod tests;

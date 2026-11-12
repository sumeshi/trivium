//! Bounded admission for heavy work. Waiting jobs hold no worker threads.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use tokio::sync::Notify;
use uuid::Uuid;

use super::{cancelled_error, JobId, JobInfo, JobKind, MAX_CONCURRENT_JOBS, MAX_QUEUED_JOBS};
use crate::error::AppError;

const INTERACTIVE_BURST: usize = 4;
const CANCELLED_REQUEST_CAPACITY: usize = 1024;
const REQUEST_ID_MAX_BYTES: usize = 128;

struct JobEntry {
    info: JobInfo,
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
    running: bool,
}

struct Registry {
    next_id: JobId,
    jobs: HashMap<JobId, JobEntry>,
    interactive_burst: usize,
    cancelled_requests: HashSet<String>,
    cancelled_order: VecDeque<String>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            next_id: 1,
            jobs: HashMap::new(),
            interactive_burst: 0,
            cancelled_requests: HashSet::new(),
            cancelled_order: VecDeque::new(),
        }
    }
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

fn interactive(kind: JobKind) -> bool {
    matches!(kind, JobKind::Query | JobKind::Load)
}

fn owned_read(kind: JobKind) -> bool {
    matches!(kind, JobKind::Query | JobKind::Load | JobKind::Analysis)
}

impl Registry {
    fn running(&self) -> usize {
        self.jobs.values().filter(|entry| entry.running).count()
    }

    fn insert(
        &mut self,
        kind: JobKind,
        project_id: Option<Uuid>,
        running: bool,
        request_id: Option<String>,
    ) -> JobGuard {
        let info = JobInfo {
            id: self.next_id,
            kind,
            project_id,
            request_id,
        };
        self.next_id += 1;
        let cancelled = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        self.jobs.insert(
            info.id,
            JobEntry {
                info: info.clone(),
                cancelled: cancelled.clone(),
                notify: notify.clone(),
                running,
            },
        );
        JobGuard {
            info,
            cancelled,
            notify,
        }
    }

    /// Interactive FIFO has preference, with an oldest-background turn after
    /// four interactive admissions so continuous scrolling cannot starve export.
    fn admit(&mut self) -> Vec<Arc<Notify>> {
        let mut wake = Vec::new();
        while self.running() < MAX_CONCURRENT_JOBS {
            let waiting = || {
                self.jobs
                    .values()
                    .filter(|entry| !entry.running && !entry.cancelled.load(Ordering::SeqCst))
            };
            let oldest = waiting()
                .min_by_key(|entry| entry.info.id)
                .map(|entry| entry.info.id);
            let background = waiting()
                .filter(|entry| !interactive(entry.info.kind))
                .min_by_key(|entry| entry.info.id)
                .map(|entry| entry.info.id);
            let foreground = waiting()
                .filter(|entry| interactive(entry.info.kind))
                .min_by_key(|entry| entry.info.id)
                .map(|entry| entry.info.id);
            let next = if self.interactive_burst >= INTERACTIVE_BURST && background.is_some() {
                background
            } else {
                foreground.or(oldest)
            };
            let Some(id) = next else {
                break;
            };
            let entry = self.jobs.get_mut(&id).expect("selected waiting job");
            entry.running = true;
            if interactive(entry.info.kind) {
                self.interactive_burst = (self.interactive_burst + 1).min(INTERACTIVE_BURST);
            } else {
                self.interactive_burst = 0;
            }
            wake.push(entry.notify.clone());
        }
        if self.jobs.is_empty() {
            self.interactive_burst = 0;
        }
        wake
    }
}

fn wake_all(notifications: Vec<Arc<Notify>>) {
    for notify in notifications {
        notify.notify_one();
    }
}

/// Legacy immediate admission for direct callers; desktop commands use queue_job.
pub fn try_begin_job(kind: JobKind, project_id: Option<Uuid>) -> Result<JobGuard, AppError> {
    let mut state = registry().lock();
    if state.running() >= MAX_CONCURRENT_JOBS {
        return Err(AppError::Message(
            "Too many long-running operations. Wait for one to finish.".into(),
        ));
    }
    Ok(state.insert(kind, project_id, true, None))
}

/// Registers a cancellable request before it waits for a CPU/memory permit.
/// A full bounded queue is reported explicitly instead of growing indefinitely.
pub fn queue_job(kind: JobKind, project_id: Option<Uuid>) -> Result<JobGuard, AppError> {
    queue_job_with_request(kind, project_id, None)
}

pub fn queue_job_with_request(
    kind: JobKind,
    project_id: Option<Uuid>,
    request_id: Option<String>,
) -> Result<JobGuard, AppError> {
    if request_id
        .as_ref()
        .is_some_and(|id| id.is_empty() || id.len() > REQUEST_ID_MAX_BYTES)
    {
        return Err(AppError::Message(
            "Request id must contain 1 to 128 bytes.".into(),
        ));
    }
    let (guard, wake) = {
        let mut state = registry().lock();
        if owned_read(kind)
            && request_id
                .as_ref()
                .is_some_and(|id| state.cancelled_requests.contains(id))
        {
            return Err(cancelled_error());
        }
        if state.jobs.values().filter(|entry| !entry.running).count() >= MAX_QUEUED_JOBS {
            return Err(AppError::Message(
                "The operation queue is full. Wait for an operation to finish.".into(),
            ));
        }
        let guard = state.insert(kind, project_id, false, request_id);
        let wake = state.admit();
        (guard, wake)
    };
    wake_all(wake);
    Ok(guard)
}

/// Read-scope disposal can beat IPC admission. Mutation/import/export are not
/// aborted by closing a pane; explicit user cancellation uses request_cancel.
/// Bounded tombstones reject delayed reads for the disposed request.
pub fn cancel_request_jobs(request_id: &str) -> usize {
    if request_id.is_empty() || request_id.len() > REQUEST_ID_MAX_BYTES {
        return 0;
    }
    let (count, wake) = {
        let mut state = registry().lock();
        if state.cancelled_requests.insert(request_id.to_string()) {
            state.cancelled_order.push_back(request_id.to_string());
            if state.cancelled_order.len() > CANCELLED_REQUEST_CAPACITY {
                if let Some(expired) = state.cancelled_order.pop_front() {
                    state.cancelled_requests.remove(&expired);
                }
            }
        }
        let mut count = 0;
        let mut wake = Vec::new();
        for entry in state.jobs.values() {
            if owned_read(entry.info.kind) && entry.info.request_id.as_deref() == Some(request_id) {
                if !entry.cancelled.swap(true, Ordering::SeqCst) {
                    count += 1;
                }
                wake.push(entry.notify.clone());
            }
        }
        (count, wake)
    };
    wake_all(wake);
    count
}

pub fn request_cancel(id: JobId) -> bool {
    let notify = {
        let state = registry().lock();
        let Some(entry) = state.jobs.get(&id) else {
            return false;
        };
        entry.cancelled.store(true, Ordering::SeqCst);
        entry.notify.clone()
    };
    notify.notify_one();
    true
}

pub fn is_cancelled(id: JobId) -> bool {
    registry()
        .lock()
        .jobs
        .get(&id)
        .is_some_and(|entry| entry.cancelled.load(Ordering::SeqCst))
}

/// Includes queued requests so the caller can cancel them before execution.
pub fn active_jobs() -> Vec<JobInfo> {
    let mut jobs: Vec<_> = registry()
        .lock()
        .jobs
        .values()
        .map(|entry| entry.info.clone())
        .collect();
    jobs.sort_by_key(|info| info.id);
    jobs
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerStats {
    pub running: usize,
    pub queued: usize,
    pub max_running: usize,
    pub max_queued: usize,
}

pub fn scheduler_stats() -> SchedulerStats {
    let state = registry().lock();
    let running = state.running();
    SchedulerStats {
        running,
        queued: state.jobs.len() - running,
        max_running: MAX_CONCURRENT_JOBS,
        max_queued: MAX_QUEUED_JOBS,
    }
}

/// Owns a queued or running request. Drop releases its permit on every path.
pub struct JobGuard {
    info: JobInfo,
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl JobGuard {
    pub fn id(&self) -> JobId {
        self.info.id
    }
    pub fn kind(&self) -> JobKind {
        self.info.kind
    }
    pub fn project_id(&self) -> Option<Uuid> {
        self.info.project_id
    }
    pub fn info(&self) -> JobInfo {
        self.info.clone()
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
    pub fn is_running(&self) -> bool {
        registry()
            .lock()
            .jobs
            .get(&self.id())
            .is_some_and(|entry| entry.running)
    }
    pub fn check_cancelled(&self) -> Result<(), AppError> {
        if self.is_cancelled() {
            Err(cancelled_error())
        } else {
            Ok(())
        }
    }

    /// Exclusive access gives each request one waiter. Notify's stored permit
    /// closes the gap between checking admission and polling the future.
    pub async fn wait_for_turn(&mut self) -> Result<(), AppError> {
        loop {
            let notified = self.notify.notified();
            self.check_cancelled()?;
            if self.is_running() {
                return Ok(());
            }
            notified.await;
        }
    }
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        let wake = {
            let mut state = registry().lock();
            state.jobs.remove(&self.id());
            state.admit()
        };
        wake_all(wake);
    }
}

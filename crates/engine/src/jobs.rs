//! Background-job registry and cancellation/progress plumbing.
//!
//! This module is intentionally Tauri-independent: the desktop commands create
//! a [`JobGuard`] on the async side, move a [`JobContext`] into a blocking
//! worker, and forward [`JobProgress`] values to the frontend.
//!
//! Cancellation is cooperative. T15 places `check_cancelled` calls before and
//! after the heavy library calls in import, query, and export. The large
//! in-library loops (CSV/JSON import, search batches, export writing) are not
//! interruptible yet; those checkpoints belong to T16–T18/T23.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Serialize;
use uuid::Uuid;

use crate::error::AppError;

pub type JobId = u64;

/// Maximum number of heavy jobs that may run at once. Import, query, and
/// export all count against this limit.
pub const MAX_CONCURRENT_JOBS: usize = 2;

/// Minimum interval between two progress notifications from one job.
pub const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    Import,
    Query,
    Export,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobInfo {
    pub id: JobId,
    pub kind: JobKind,
    pub project_id: Option<Uuid>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobProgress {
    pub id: JobId,
    pub kind: JobKind,
    pub done: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
}

/// Callback used by the desktop layer to forward a progress value, typically
/// as a Tauri event.
pub type ProgressFn = Arc<dyn Fn(JobProgress) + Send + Sync>;

struct JobEntry {
    info: JobInfo,
    cancelled: Arc<AtomicBool>,
}

struct Registry {
    next_id: JobId,
    jobs: HashMap<JobId, JobEntry>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            // IDs are monotonic for the process lifetime and act as the job
            // generation: an old ID can never select a newer job.
            next_id: 1,
            jobs: HashMap::new(),
        }
    }
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

/// Creates a job entry or rejects the request when the concurrency limit is
/// reached. Dropping the returned guard removes the entry.
pub fn try_begin_job(kind: JobKind, project_id: Option<Uuid>) -> Result<JobGuard, AppError> {
    let mut registry = registry().lock();
    if registry.jobs.len() >= MAX_CONCURRENT_JOBS {
        return Err(AppError::Message(
            "Too many long-running operations. Wait for one to finish.".into(),
        ));
    }
    let id = registry.next_id;
    registry.next_id += 1;
    let cancelled = Arc::new(AtomicBool::new(false));
    registry.jobs.insert(
        id,
        JobEntry {
            info: JobInfo {
                id,
                kind,
                project_id,
            },
            cancelled: Arc::clone(&cancelled),
        },
    );
    Ok(JobGuard {
        id,
        kind,
        project_id,
        cancelled,
    })
}

/// Requests cancellation of one job. Returns `false` when the ID is unknown or
/// the job has already finished, so a finished job cannot affect a newer one.
pub fn request_cancel(id: JobId) -> bool {
    let registry = registry().lock();
    match registry.jobs.get(&id) {
        Some(entry) => {
            entry.cancelled.store(true, Ordering::SeqCst);
            true
        }
        None => false,
    }
}

/// Reports whether a live job has been asked to cancel.
pub fn is_cancelled(id: JobId) -> bool {
    registry()
        .lock()
        .jobs
        .get(&id)
        .map(|entry| entry.cancelled.load(Ordering::SeqCst))
        .unwrap_or(false)
}

/// Lists active jobs ordered by ID.
pub fn active_jobs() -> Vec<JobInfo> {
    let registry = registry().lock();
    let mut jobs: Vec<JobInfo> = registry
        .jobs
        .values()
        .map(|entry| entry.info.clone())
        .collect();
    jobs.sort_by_key(|job| job.id);
    jobs
}

/// The fixed error text the frontend uses to distinguish a cancelled job from
/// a failure.
pub fn cancelled_error() -> AppError {
    AppError::Message("Cancelled.".into())
}

/// Owns one active job entry. The entry is removed on drop, whether the job
/// succeeded, failed, or panicked inside the blocking worker.
pub struct JobGuard {
    id: JobId,
    kind: JobKind,
    project_id: Option<Uuid>,
    cancelled: Arc<AtomicBool>,
}

impl JobGuard {
    pub fn id(&self) -> JobId {
        self.id
    }

    pub fn kind(&self) -> JobKind {
        self.kind
    }

    pub fn project_id(&self) -> Option<Uuid> {
        self.project_id
    }

    pub fn info(&self) -> JobInfo {
        JobInfo {
            id: self.id,
            kind: self.kind,
            project_id: self.project_id,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub fn check_cancelled(&self) -> Result<(), AppError> {
        if self.is_cancelled() {
            Err(cancelled_error())
        } else {
            Ok(())
        }
    }
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        registry().lock().jobs.remove(&self.id);
    }
}

/// Cooperative cancellation and throttled progress for an engine operation.
///
/// `JobContext::none()` keeps the old engine behaviour for direct callers and
/// tests. Tauri commands pass a context backed by a [`JobGuard`].
pub struct JobContext {
    guard: Option<JobGuard>,
    progress: Mutex<Option<ThrottledProgress>>,
}

impl JobContext {
    pub fn none() -> Self {
        Self {
            guard: None,
            progress: Mutex::new(None),
        }
    }

    pub fn with_job(guard: JobGuard, emit: Option<ProgressFn>) -> Self {
        let progress =
            emit.map(|emit| ThrottledProgress::new(guard.id(), guard.kind(), Some(emit)));
        Self {
            guard: Some(guard),
            progress: Mutex::new(progress),
        }
    }

    pub fn id(&self) -> Option<JobId> {
        self.guard.as_ref().map(|guard| guard.id())
    }

    pub fn kind(&self) -> Option<JobKind> {
        self.guard.as_ref().map(|guard| guard.kind())
    }

    pub fn project_id(&self) -> Option<Uuid> {
        self.guard.as_ref().and_then(|guard| guard.project_id())
    }

    pub fn is_cancelled(&self) -> bool {
        self.guard
            .as_ref()
            .map(|guard| guard.is_cancelled())
            .unwrap_or(false)
    }

    /// Cancellation checkpoint. Call this at stage boundaries, not inside a
    /// long-running library call that cannot observe it yet.
    pub fn check_cancelled(&self) -> Result<(), AppError> {
        if self.is_cancelled() {
            Err(cancelled_error())
        } else {
            Ok(())
        }
    }

    /// Reports progress when at least [`PROGRESS_INTERVAL`] has elapsed since
    /// the previous report. Returns whether a notification was emitted.
    pub fn report_progress(&self, done: u64, total: Option<u64>, message: Option<String>) -> bool {
        self.progress
            .lock()
            .as_mut()
            .map(|progress| progress.report(done, total, message))
            .unwrap_or(false)
    }
}

/// Throttles progress notifications to one per interval and injects the job
/// identity into every value.
pub struct ThrottledProgress {
    id: JobId,
    kind: JobKind,
    interval: Duration,
    clock: Box<dyn Fn() -> Duration + Send + Sync>,
    last_emitted: Option<Duration>,
    emit: Option<ProgressFn>,
}

impl ThrottledProgress {
    pub fn new(id: JobId, kind: JobKind, emit: Option<ProgressFn>) -> Self {
        let start = Instant::now();
        Self::with_clock(id, kind, PROGRESS_INTERVAL, move || start.elapsed(), emit)
    }

    /// Constructor with an injectable clock (elapsed time since an arbitrary
    /// origin) used by tests to check the throttle without sleeping.
    pub fn with_clock(
        id: JobId,
        kind: JobKind,
        interval: Duration,
        clock: impl Fn() -> Duration + Send + Sync + 'static,
        emit: Option<ProgressFn>,
    ) -> Self {
        Self {
            id,
            kind,
            interval,
            clock: Box::new(clock),
            last_emitted: None,
            emit,
        }
    }

    /// Returns `true` when the report passed the throttle and was emitted. The
    /// first report is always emitted.
    pub fn report(&mut self, done: u64, total: Option<u64>, message: Option<String>) -> bool {
        let now = (self.clock)();
        let due = match self.last_emitted {
            None => true,
            Some(last) => now.saturating_sub(last) >= self.interval,
        };
        if !due {
            return false;
        }
        self.last_emitted = Some(now);
        if let Some(emit) = &self.emit {
            emit(JobProgress {
                id: self.id,
                kind: self.kind,
                done,
                total,
                message,
            });
        }
        true
    }
}

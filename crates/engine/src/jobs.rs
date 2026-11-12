//! Background-job registry and cancellation/progress plumbing.
//!
//! This module is intentionally Tauri-independent: the desktop commands create
//! a [`JobGuard`] on the async side, move a [`JobContext`] into a blocking
//! worker, and forward [`JobProgress`] values to the frontend.
//!
//! Cancellation is cooperative. `check_cancelled` calls surround the heavy
//! library calls, CSV/JSON import checks every batch boundary, and search
//! evaluation checks each fixed row batch. Heavy desktop work shares bounded
//! async admission, including load, analysis, mutation and atomic exports.

mod scheduler;

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Serialize;
use uuid::Uuid;

use crate::error::AppError;

pub type JobId = u64;

/// Maximum number of heavy jobs that may run at once across every command.
pub const MAX_CONCURRENT_JOBS: usize = 2;
/// Bound accepted waiting work as well as executing work.
pub const MAX_QUEUED_JOBS: usize = 32;

pub use scheduler::{
    active_jobs, cancel_request_jobs, is_cancelled, queue_job, queue_job_with_request,
    request_cancel, scheduler_stats, try_begin_job, JobGuard, SchedulerStats,
};

/// Minimum interval between two progress notifications from one job.
pub const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ts_rs::TS)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    Import,
    Query,
    Export,
    Load,
    Analysis,
    Mutation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct JobInfo {
    #[ts(type = "number")]
    pub id: JobId,
    pub kind: JobKind,
    pub project_id: Option<Uuid>,
    /// Transport-level ownership; never part of QuerySpec or result-cache keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct JobProgress {
    #[ts(type = "number")]
    pub id: JobId,
    pub kind: JobKind,
    #[ts(type = "number")]
    pub done: u64,
    #[ts(type = "number | null")]
    pub total: Option<u64>,
    pub message: Option<String>,
}

/// Callback used by the desktop layer to forward a progress value, typically
/// as a Tauri event.
pub type ProgressFn = Arc<dyn Fn(JobProgress) + Send + Sync>;

/// The fixed error text the frontend uses to distinguish a cancelled job from
/// a failure.
pub fn cancelled_error() -> AppError {
    AppError::Message("Cancelled.".into())
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
        let final_report = total.is_some_and(|total| done >= total);
        let due = final_report
            || match self.last_emitted {
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

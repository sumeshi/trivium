use std::sync::Arc;

use tauri::{AppHandle, Emitter};
use trivium_engine::error::AppError;
use trivium_engine::jobs::{self, JobContext, JobId, JobInfo, JobKind, ProgressFn};
use uuid::Uuid;

pub const JOB_STARTED_EVENT: &str = "trivium:job-started";
pub const JOB_PROGRESS_EVENT: &str = "trivium:job-progress";
pub const JOB_FINISHED_EVENT: &str = "trivium:job-finished";

/// Creates a job guard on the async side, then runs `work` on the blocking
/// pool. The store handle is owned by the closure, so no Tauri state guard is
/// held while the engine operation runs.
pub(crate) async fn run_job<T, F>(
    app: AppHandle,
    kind: JobKind,
    project_id: Option<Uuid>,
    work: F,
) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(JobContext) -> Result<T, AppError> + Send + 'static,
{
    let guard = jobs::try_begin_job(kind, project_id).map_err(String::from)?;
    let info = guard.info();
    let _ = app.emit(JOB_STARTED_EVENT, info.clone());

    let progress_app = app.clone();
    let emit: ProgressFn = Arc::new(move |progress| {
        let _ = progress_app.emit(JOB_PROGRESS_EVENT, progress);
    });
    let context = JobContext::with_job(guard, Some(emit));

    let result = match tauri::async_runtime::spawn_blocking(move || work(context)).await {
        Ok(result) => result.map_err(String::from),
        Err(error) => Err(format!("Background task failed: {}", error)),
    };
    // The frontend uses this to drop finished ids from its cancellation set.
    let _ = app.emit(JOB_FINISHED_EVENT, info);
    result
}

/// Requests cancellation of one background job. Returns `false` when the ID is
/// unknown or the job already finished.
#[tauri::command]
pub fn cancel_job(job_id: JobId) -> bool {
    jobs::request_cancel(job_id)
}

/// Lists the background jobs currently running.
#[tauri::command]
pub fn active_jobs() -> Vec<JobInfo> {
    jobs::active_jobs()
}

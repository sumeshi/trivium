use std::sync::Arc;

use tauri::{AppHandle, Emitter};
use trivium_engine::error::AppError;
use trivium_engine::jobs::{self, JobContext, JobId, JobInfo, JobKind, ProgressFn};
use uuid::Uuid;

pub const JOB_STARTED_EVENT: &str = "trivium:job-started";
pub const JOB_PROGRESS_EVENT: &str = "trivium:job-progress";
pub const JOB_FINISHED_EVENT: &str = "trivium:job-finished";

/// Registers on the async side, then obtains bounded admission before creating
/// a blocking worker. Queued requests hold no blocking-pool threads.
pub(crate) async fn run_job_with_request<T, F>(
    app: AppHandle,
    kind: JobKind,
    project_id: Option<Uuid>,
    request_id: Option<String>,
    work: F,
) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(JobContext) -> Result<T, AppError> + Send + 'static,
{
    let mut guard =
        jobs::queue_job_with_request(kind, project_id, request_id).map_err(String::from)?;
    let info = guard.info();
    let _ = app.emit(JOB_STARTED_EVENT, info.clone());
    let finished = FinishedEvent {
        app: app.clone(),
        info: info.clone(),
    };
    let mut cancel = CancelOnDrop {
        id: info.id,
        armed: true,
    };
    if !guard.is_running() {
        let _ = app.emit(
            JOB_PROGRESS_EVENT,
            jobs::JobProgress {
                id: info.id,
                kind,
                done: 0,
                total: None,
                message: Some("Waiting for another operation".into()),
            },
        );
    }
    if let Err(error) = guard.wait_for_turn().await {
        drop(guard);
        cancel.armed = false;
        return Err(error.to_string());
    }

    let progress_app = app.clone();
    let emit: ProgressFn = Arc::new(move |progress| {
        let _ = progress_app.emit(JOB_PROGRESS_EVENT, progress);
    });
    let context = JobContext::with_job(guard, Some(emit));

    let result = match tauri::async_runtime::spawn_blocking(move || {
        // The worker owns the finish notification after admission, including
        // when the IPC future is dropped while native work is still unwinding.
        let result = match context.check_cancelled() {
            Ok(()) => work(context),
            Err(error) => {
                drop(context);
                Err(error)
            }
        };
        drop(finished);
        result
    })
    .await
    {
        Ok(result) => result.map_err(String::from),
        Err(error) => Err(format!("Background task failed: {}", error)),
    };
    cancel.armed = false;
    result
}

struct FinishedEvent {
    app: AppHandle,
    info: JobInfo,
}

impl Drop for FinishedEvent {
    fn drop(&mut self) {
        let _ = self.app.emit(JOB_FINISHED_EVENT, self.info.clone());
    }
}

struct CancelOnDrop {
    id: JobId,
    armed: bool,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            jobs::request_cancel(self.id);
        }
    }
}

/// Short metadata/file operations only. Heavy readers and writers use run_job_with_request
/// so they share admission, ownership, cancellation and progress.
pub(crate) async fn run_blocking<T, F>(work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    match tauri::async_runtime::spawn_blocking(work).await {
        Ok(result) => result.map_err(String::from),
        Err(error) => Err(format!("Background task failed: {}", error)),
    }
}

/// Requests cancellation of one background job. Returns `false` when the ID is
/// unknown or the job already finished.
#[tauri::command]
pub fn cancel_job(job_id: JobId) -> bool {
    jobs::request_cancel(job_id)
}

#[tauri::command]
pub fn cancel_request_jobs(request_id: String) -> usize {
    jobs::cancel_request_jobs(&request_id)
}

/// Lists queued and running background jobs.
#[tauri::command]
pub fn active_jobs() -> Vec<JobInfo> {
    jobs::active_jobs()
}

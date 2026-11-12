use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::bulk::{BulkAnnotatePayload, BulkApplied, BulkCount, BulkUndoPayload};

/// Counts the rows a bulk update would touch.
#[tauri::command]
pub async fn bulk_preview(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: BulkAnnotatePayload,
    request_id: Option<String>,
) -> Result<BulkCount, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Analysis, project_id, request_id, move |job| {
        ops::bulk::bulk_preview_with_job(&store, payload, &job)
    })
    .await
}

/// Applies a bulk flag/memo change over the resolved target rows.
#[tauri::command]
pub async fn bulk_update(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: BulkAnnotatePayload,
    request_id: Option<String>,
) -> Result<BulkApplied, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Mutation, project_id, request_id, move |job| {
        ops::bulk::bulk_update_with_job(&store, payload, &job)
    })
    .await
}

/// Restores the rows touched by the last bulk update.
#[tauri::command]
pub async fn bulk_undo(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: BulkUndoPayload,
    request_id: Option<String>,
) -> Result<BulkApplied, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Mutation, project_id, request_id, move |job| {
        ops::bulk::bulk_undo_with_job(&store, payload, &job)
    })
    .await
}

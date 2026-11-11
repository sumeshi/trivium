use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::bulk::{BulkAnnotatePayload, BulkApplied, BulkCount, BulkUndoPayload};

/// Counts the rows a bulk update would touch.
#[tauri::command]
pub async fn bulk_preview(
    state: State<'_, AppState>,
    payload: BulkAnnotatePayload,
) -> Result<BulkCount, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::bulk::bulk_preview(&store, payload)).await
}

/// Applies a bulk flag/memo change over the resolved target rows.
#[tauri::command]
pub async fn bulk_update(
    state: State<'_, AppState>,
    payload: BulkAnnotatePayload,
) -> Result<BulkApplied, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::bulk::bulk_update(&store, payload)).await
}

/// Restores the rows touched by the last bulk update.
#[tauri::command]
pub async fn bulk_undo(
    state: State<'_, AppState>,
    payload: BulkUndoPayload,
) -> Result<BulkApplied, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::bulk::bulk_undo(&store, payload)).await
}

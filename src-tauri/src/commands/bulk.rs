use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::bulk::{BulkAnnotatePayload, BulkApplied, BulkCount, BulkUndoPayload};

/// Counts the rows a bulk update would touch.
#[tauri::command]
pub fn bulk_preview(
    state: State<AppState>,
    payload: BulkAnnotatePayload,
) -> Result<BulkCount, String> {
    ops::bulk::bulk_preview(&state.projects, payload).map_err(String::from)
}

/// Applies a bulk flag/memo change over the resolved target rows.
#[tauri::command]
pub fn bulk_update(
    state: State<AppState>,
    payload: BulkAnnotatePayload,
) -> Result<BulkApplied, String> {
    ops::bulk::bulk_update(&state.projects, payload).map_err(String::from)
}

/// Restores the rows touched by the last bulk update.
#[tauri::command]
pub fn bulk_undo(state: State<AppState>, payload: BulkUndoPayload) -> Result<BulkApplied, String> {
    ops::bulk::bulk_undo(&state.projects, payload).map_err(String::from)
}

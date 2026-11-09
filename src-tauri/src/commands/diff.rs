use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::diff::{DiffProjectsPayload, DiffResult};

/// Diffs two projects by key columns, preserving duplicate counts.
#[tauri::command]
pub fn diff_projects(
    state: State<AppState>,
    payload: DiffProjectsPayload,
) -> Result<DiffResult, String> {
    ops::diff::diff_projects(&state.projects, payload).map_err(String::from)
}

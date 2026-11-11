use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::diff::{DiffProjectsPayload, DiffResult};

/// Diffs two projects by key columns, preserving duplicate counts.
#[tauri::command]
pub async fn diff_projects(
    state: State<'_, AppState>,
    payload: DiffProjectsPayload,
) -> Result<DiffResult, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::diff::diff_projects(&store, payload)).await
}

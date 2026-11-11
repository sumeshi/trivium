use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::group::{GroupTreePayload, GroupTreeResponse};

/// Builds a hierarchical grouping over rows that match the current filter.
#[tauri::command]
pub async fn group_tree(
    state: State<'_, AppState>,
    payload: GroupTreePayload,
) -> Result<GroupTreeResponse, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::group::group_tree(&store, payload)).await
}

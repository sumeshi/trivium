use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::group::{GroupTreePayload, GroupTreeResponse};

/// Builds a hierarchical grouping over rows that match the current filter.
#[tauri::command]
pub fn group_tree(
    state: State<AppState>,
    payload: GroupTreePayload,
) -> Result<GroupTreeResponse, String> {
    ops::group::group_tree(&state.projects, payload).map_err(String::from)
}

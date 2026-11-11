use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::cross::{CrossSearchHit, CrossSearchPayload};

/// One condition applied to each project. Each hit count matches a single-project query.
#[tauri::command]
pub async fn search_projects(
    state: State<'_, AppState>,
    payload: CrossSearchPayload,
) -> Result<Vec<CrossSearchHit>, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::cross::search_projects(&store, payload)).await
}

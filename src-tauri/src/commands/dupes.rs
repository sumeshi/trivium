use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::dupes::{DupesPayload, DupesResponse};

/// Returns every row whose key is repeated, or every row whose key appears once.
#[tauri::command]
pub async fn duplicate_rows(
    state: State<'_, AppState>,
    payload: DupesPayload,
) -> Result<DupesResponse, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::dupes::duplicate_rows(&store, payload)).await
}

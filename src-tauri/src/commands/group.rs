use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::group::{GroupTreePayload, GroupTreeResponse};

/// Builds a hierarchical grouping over rows that match the current filter.
#[tauri::command]
pub async fn group_tree(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: GroupTreePayload,
    request_id: Option<String>,
) -> Result<GroupTreeResponse, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Analysis, project_id, request_id, move |job| {
        ops::group::group_tree_with_job(&store, payload, &job)
    })
    .await
}

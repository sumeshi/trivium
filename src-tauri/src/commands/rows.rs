use tauri::State;

use crate::commands::job::run_job;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::rows::{
    LocateRowPayload, LocateRowResponse, QueryRowsPayload, QueryRowsResponse,
};

/// Streams project rows with filtering, sorting, IOC application, and pagination.
///
/// Search and sort run on the blocking pool so the window stays responsive.
#[tauri::command]
pub async fn query_project_rows(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: QueryRowsPayload,
) -> Result<QueryRowsResponse, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job(app, JobKind::Query, project_id, move |job| {
        ops::rows::query_project_rows_with_job(&store, payload, &job)
    })
    .await
}

/// Locates an original row id without changing its current query population.
#[tauri::command]
pub async fn locate_project_row(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: LocateRowPayload,
) -> Result<LocateRowResponse, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job(app, JobKind::Query, project_id, move |job| {
        ops::rows::locate_project_row_with_job(&store, payload, &job)
    })
    .await
}

use tauri::State;

use crate::{commands::job::run_job_with_request, state::AppState};
use trivium_engine::{
    jobs::JobKind,
    ops::{import_preview, import_report},
};

#[tauri::command]
pub async fn preview_import(
    app: tauri::AppHandle,
    payload: import_preview::PreviewImportPayload,
    request_id: Option<String>,
) -> Result<import_preview::ImportPreview, String> {
    run_job_with_request(app, JobKind::Load, None, request_id, move |job| {
        import_preview::preview_import_with_job(payload, &job)
    })
    .await
}

#[tauri::command]
pub async fn export_import_report(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: import_report::ExportImportReportPayload,
    request_id: Option<String>,
) -> Result<import_report::ExportImportReportResult, String> {
    let store = state.projects.clone();
    let project_id = payload.project_id;
    run_job_with_request(
        app,
        JobKind::Export,
        Some(project_id),
        request_id,
        move |job| import_report::export_import_report_with_job(&store, payload, &job),
    )
    .await
}

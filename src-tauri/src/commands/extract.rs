use tauri::State;
use uuid::Uuid;

use crate::{
    commands::job::{run_blocking, run_job_with_request},
    state::AppState,
};
use extract::{
    ExportExtractPayload, ExtractPage, ExtractPagePayload, ExtractPayload, ExtractResult,
    ExtractSources, ExtractSourcesPayload,
};
use trivium_engine::{
    jobs::JobKind,
    ops::{export::ExportResult, extract},
};

#[tauri::command]
pub async fn extract_values(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: ExtractPayload,
    request_id: Option<String>,
) -> Result<ExtractResult, String> {
    let store = state.projects.clone();
    let project = Some(payload.project_id);
    run_job_with_request(app, JobKind::Analysis, project, request_id, move |job| {
        extract::extract_values(&store, payload, &job)
    })
    .await
}

#[tauri::command]
pub async fn extract_values_page(
    state: State<'_, AppState>,
    payload: ExtractPagePayload,
) -> Result<ExtractPage, String> {
    let store = state.projects.clone();
    run_blocking(move || extract::extract_values_page(&store, payload)).await
}

#[tauri::command]
pub async fn extract_value_sources(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: ExtractSourcesPayload,
    request_id: Option<String>,
) -> Result<ExtractSources, String> {
    let store = state.projects.clone();
    run_job_with_request(app, JobKind::Analysis, None, request_id, move |job| {
        extract::extract_value_sources(&store, payload, &job)
    })
    .await
}

#[tauri::command]
pub async fn export_extract(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: ExportExtractPayload,
    request_id: Option<String>,
) -> Result<ExportResult, String> {
    let store = state.projects.clone();
    run_job_with_request(app, JobKind::Export, None, request_id, move |job| {
        extract::export_extract(&store, payload, &job)
    })
    .await
}

#[tauri::command]
pub async fn release_extract_result(result_id: Uuid) -> Result<bool, String> {
    run_blocking(move || Ok(extract::release_extract_result(result_id))).await
}

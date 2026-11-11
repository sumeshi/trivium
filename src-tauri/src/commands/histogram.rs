use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::analysis::histogram::Histogram;
use trivium_engine::ops;
use trivium_engine::ops::histogram::HistogramPayload;

/// Equal-width buckets for a numeric or datetime column over the filtered rows.
#[tauri::command]
pub async fn histogram(
    state: State<'_, AppState>,
    payload: HistogramPayload,
) -> Result<Histogram, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::histogram::histogram(&store, payload)).await
}

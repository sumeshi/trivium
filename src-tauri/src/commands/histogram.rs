use tauri::State;

use crate::state::AppState;
use trivium_engine::analysis::histogram::Histogram;
use trivium_engine::ops;
use trivium_engine::ops::histogram::HistogramPayload;

/// Equal-width buckets for a numeric or datetime column over the filtered rows.
#[tauri::command]
pub fn histogram(
    state: State<AppState>,
    payload: HistogramPayload,
) -> Result<Histogram, String> {
    ops::histogram::histogram(&state.projects, payload).map_err(String::from)
}

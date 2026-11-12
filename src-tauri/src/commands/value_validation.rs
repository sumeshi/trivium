use crate::commands::job::run_blocking;
use trivium_engine::ops::value_validation::{
    self, ConditionValuesValidation, ValidateConditionValuesPayload,
};

/// Parses input only; it never scans evidence or rewrites a saved condition.
#[tauri::command]
pub async fn validate_condition_values(
    payload: ValidateConditionValuesPayload,
) -> Result<ConditionValuesValidation, String> {
    run_blocking(move || value_validation::validate_condition_values(payload)).await
}

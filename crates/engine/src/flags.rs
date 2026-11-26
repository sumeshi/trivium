use crate::error::AppError;

pub fn normalize_flag_value(flag: &str) -> String {
    recognized_flag_value(flag).unwrap_or_default().to_string()
}

/// Annotation writes accept only the canonical values used by the UI.
pub fn validate_flag_value(flag: &str) -> Result<(), AppError> {
    recognized_flag_value(flag)
        .map(|_| ())
        .ok_or_else(|| AppError::Message(format!("Invalid flag value: {flag:?}.")))
}

fn recognized_flag_value(flag: &str) -> Option<&'static str> {
    match flag {
        "" => Some(""),
        "safe" => Some("safe"),
        "suspicious" => Some("suspicious"),
        "critical" => Some("critical"),
        _ => None,
    }
}

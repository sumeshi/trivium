use crate::error::AppError;

pub fn normalize_flag_value(flag: &str) -> String {
    recognized_flag_value(flag).unwrap_or_default().to_string()
}

/// Annotation writes reject unknown values. Reading legacy annotations still
/// uses the forgiving normalizer above, without rewriting stored content.
pub fn validate_flag_value(flag: &str) -> Result<(), AppError> {
    recognized_flag_value(flag)
        .map(|_| ())
        .ok_or_else(|| AppError::Message(format!("Invalid flag value: {flag:?}.")))
}

fn recognized_flag_value(flag: &str) -> Option<&'static str> {
    let flag = flag.trim();
    if flag.is_empty() {
        Some("")
    } else if flag.eq_ignore_ascii_case("safe") || flag == "◯" {
        Some("safe")
    } else if flag.eq_ignore_ascii_case("suspicious") || flag == "?" {
        Some("suspicious")
    } else if flag.eq_ignore_ascii_case("critical") || flag == "✗" {
        Some("critical")
    } else {
        None
    }
}

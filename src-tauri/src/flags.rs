pub fn normalize_flag_value(flag: &str) -> String {
    let trimmed = flag.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    match trimmed.to_lowercase().as_str() {
        "safe" => "safe".to_string(),
        "suspicious" => "suspicious".to_string(),
        "critical" => "critical".to_string(),
        "◯" => "safe".to_string(),
        "?" => "suspicious".to_string(),
        "✗" => "critical".to_string(),
        _ => String::new(),
    }
}

pub fn severity_rank(value: &str) -> u8 {
    match value {
        "critical" => 3,
        "suspicious" => 2,
        "safe" => 1,
        _ => 0,
    }
}

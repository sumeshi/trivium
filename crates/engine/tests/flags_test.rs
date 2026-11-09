use trivium_engine::flags::{normalize_flag_value, severity_rank};

#[test]
fn normalize_flag_value_accepts_all_canonical_forms() {
    let cases: Vec<(&str, &str)> = vec![
        ("safe", "safe"),
        ("suspicious", "suspicious"),
        ("critical", "critical"),
        ("◯", "safe"),
        ("?", "suspicious"),
        ("✗", "critical"),
        // Case-insensitive
        ("SAFE", "safe"),
        ("Suspicious", "suspicious"),
        ("CRITICAL", "critical"),
        // Surrounding whitespace is trimmed
        ("  critical  ", "critical"),
        (" ◯ ", "safe"),
        // Empty and unknown values become empty strings
        ("", ""),
        ("   ", ""),
        ("unknown", ""),
        ("!", ""),
    ];

    for (input, expected) in cases {
        assert_eq!(
            normalize_flag_value(input),
            expected,
            "input: {input:?}"
        );
    }
}

#[test]
fn severity_rank_orders_critical_suspicious_safe() {
    let cases: Vec<(&str, u8)> = vec![
        ("critical", 3),
        ("suspicious", 2),
        ("safe", 1),
        ("", 0),
        ("unknown", 0),
        // Case-sensitive: normalization must happen before ranking
        ("Critical", 0),
        ("SAFE", 0),
    ];

    for (input, expected) in cases {
        assert_eq!(severity_rank(input), expected, "input: {input:?}");
    }
}

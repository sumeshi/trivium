use trivium_engine::flags::normalize_flag_value;

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
        assert_eq!(normalize_flag_value(input), expected, "input: {input:?}");
    }
}

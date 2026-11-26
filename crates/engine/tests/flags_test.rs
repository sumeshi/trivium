use trivium_engine::flags::{normalize_flag_value, validate_flag_value};

#[test]
fn only_canonical_flag_values_are_recognized() {
    let cases: Vec<(&str, &str)> = vec![
        ("safe", "safe"),
        ("suspicious", "suspicious"),
        ("critical", "critical"),
        ("", ""),
    ];

    for (input, expected) in cases {
        assert_eq!(normalize_flag_value(input), expected, "input: {input:?}");
        validate_flag_value(input).unwrap();
    }
    for input in [
        "◯", "?", "✗", "SAFE", "Suspicious", "CRITICAL", "  critical  ", " ◯ ",
        "   ", "unknown", "!",
    ] {
        assert_eq!(normalize_flag_value(input), "", "input: {input:?}");
        assert!(validate_flag_value(input).is_err(), "input: {input:?}");
    }
}

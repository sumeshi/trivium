use std::collections::HashMap;

use serde_json::json;
use trivium_engine::{
    condition::{eval, Condition, Node, Op},
    ops::value_validation::{
        validate_condition_values, ConditionValuesValidation, ValidateConditionValuesPayload,
    },
};

fn validate(kind: &str, op: Op, values: &[&str]) -> ConditionValuesValidation {
    validate_condition_values(ValidateConditionValuesPayload {
        kind: kind.into(),
        op,
        values: values.iter().map(|value| (*value).into()).collect(),
    })
    .unwrap()
}

fn evaluated(
    cell: &str,
    op: Op,
    value: &str,
) -> Result<bool, trivium_engine::condition::EvalError> {
    eval(
        &Node::Condition(Condition {
            column: "timestamp".into(),
            op,
            values: vec![value.into()],
            negated: false,
        }),
        &HashMap::from([("timestamp".into(), json!(cell))]),
    )
}

#[test]
fn exact_nanoseconds_and_offsets_match_the_query_parser_without_rewriting_input() {
    let values = [
        "2026-11-12T03:04:05.123456789Z",
        "2026-11-12T12:04:05.123456789+09:00",
        "2026-11-12 03:04:05.123456789",
        " 2026-11-12T03:04:05.123456789Z ",
    ];
    let result = validate("datetime", Op::Ge, &values);
    assert!(result.valid);
    let parsed = result.values[0].parsed_nanos.as_deref().unwrap();
    assert!(parsed.parse::<i64>().unwrap() > 9_007_199_254_740_991);
    for (value, parsed_value) in values.iter().zip(&result.values) {
        assert_eq!(parsed_value.parsed_nanos.as_deref(), Some(parsed));
        assert!(evaluated(values[0], Op::Ge, value).unwrap());
        assert!(evaluated(values[0], Op::Le, value).unwrap());
    }
    let wire = serde_json::to_value(&result).unwrap();
    assert!(wire["values"][0]["parsedNanos"].is_string());
    assert!(result.hint.unwrap().contains("UTC"));
}

#[test]
fn calendar_errors_unsupported_formats_and_out_of_nanosecond_range_are_reported_per_value() {
    let values = [
        "2024-02-29",
        "2026-02-29",
        "2026-02-30T12:00:00Z",
        "12/11/2026",
        "1600-01-01",
        "2300-01-01",
        "2026-11-12T03:04:05+25:00",
        "",
    ];
    let result = validate("datetime", Op::Lt, &values);
    assert!(!result.valid);
    assert!(result.values[0].valid);
    for (value, checked) in values[1..].iter().zip(&result.values[1..]) {
        assert!(!checked.valid, "{value}");
        assert!(checked.error.is_some());
        assert!(checked.parsed_nanos.is_none());
        assert!(
            evaluated("2026-11-12T03:04:05Z", Op::Lt, value).is_err(),
            "{value}"
        );
    }
}

#[test]
fn date_only_and_fractional_naive_values_use_the_existing_utc_interpretation() {
    let result = validate(
        "datetime",
        Op::Le,
        &[
            "2026-11-12",
            "2026-11-12T00:00:00Z",
            "2026-11-12 00:00:00.000000000",
        ],
    );
    assert!(result.valid);
    assert_eq!(result.values[0].parsed_nanos, result.values[1].parsed_nanos);
    assert_eq!(result.values[1].parsed_nanos, result.values[2].parsed_nanos);
}

#[test]
fn ordered_integer_validation_has_the_same_exact_i128_limits_as_evaluation() {
    let result = validate(
        "int",
        Op::Gt,
        &[
            "170141183460469231731687303715884105727",
            "-170141183460469231731687303715884105728",
            " +0001 ",
            "170141183460469231731687303715884105728",
            "1.5",
            "1e3",
        ],
    );
    assert!(!result.valid);
    for checked in &result.values[..3] {
        assert!(checked.valid);
    }
    for checked in &result.values[3..] {
        assert!(!checked.valid);
    }
    assert!(evaluated("0", Op::Gt, "170141183460469231731687303715884105728").is_err());
}

#[test]
fn equality_and_text_operators_keep_original_source_text_semantics() {
    for op in [
        Op::Eq,
        Op::Ne,
        Op::In,
        Op::Contains,
        Op::StartsWith,
        Op::EndsWith,
    ] {
        let result = validate(
            "datetime",
            op,
            &["not a timestamp", "", "2026-11-12T12:04:05.123456789+09:00"],
        );
        assert!(result.valid);
        assert!(result
            .values
            .iter()
            .all(|value| value.parsed_nanos.is_none()));
    }
    let result = validate("datetime", Op::Eq, &["2026-11-12T12:04:05.123456789+09:00"]);
    assert!(result
        .hint
        .unwrap()
        .contains("original timestamp text exactly"));
    assert!(!evaluated(
        "2026-11-12T03:04:05.123456789Z",
        Op::Eq,
        "2026-11-12T12:04:05.123456789+09:00"
    )
    .unwrap());
    assert!(validate("float", Op::Eq, &["1.5"]).valid);
}

#[test]
fn regex_validation_uses_the_prepared_query_patterns() {
    let result = validate("string", Op::Regex, &["(?i)error|warn", "(", "\\p{Greek}+"]);
    assert!(!result.valid);
    assert!(result.values[0].valid);
    assert!(!result.values[1].valid);
    assert!(result.values[2].valid);
    assert_eq!(
        result.values[1].error.as_deref(),
        Some(
            evaluated("example", Op::Regex, "(")
                .unwrap_err()
                .to_string()
                .as_str()
        )
    );
}

#[test]
fn unsupported_ordered_decimals_and_text_are_explained_without_changing_the_query() {
    let decimal = validate("float", Op::Ge, &["1.5"]);
    assert!(!decimal.valid);
    assert!(decimal.values[0]
        .error
        .as_deref()
        .unwrap()
        .contains("decimal"));
    assert!(evaluated("1.5", Op::Ge, "1.5").is_err());
    let mixed = validate("string", Op::Gt, &["42", "2026-11-12", "alpha"]);
    assert!(mixed.values[0].valid && mixed.values[1].valid && !mixed.values[2].valid);
}

#[test]
fn empty_values_and_unknown_kinds_do_not_produce_a_false_success() {
    let empty = validate("datetime", Op::Ge, &[]);
    assert!(!empty.valid);
    assert_eq!(
        empty.hint.as_deref(),
        Some("Enter at least one condition value.")
    );
    let error = validate_condition_values(ValidateConditionValuesPayload {
        kind: "imaginary".into(),
        op: Op::Eq,
        values: vec!["x".into()],
    })
    .unwrap_err();
    assert!(error.to_string().contains("Unknown"));
}

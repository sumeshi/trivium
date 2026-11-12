//! Advisory condition input checks using the query engine's parsers.
//!
//! The caller already has the column kind from the loaded case profile. Input
//! validation never opens evidence, changes a condition, or normalizes source
//! strings. In particular, equality remains an exact textual comparison.
use serde::{Deserialize, Serialize};

use crate::{
    analysis::infer::parse_datetime_nanos,
    condition::{Condition, Node, Op, PreparedNode},
    error::AppError,
};

#[derive(Debug, Deserialize, ts_rs::TS)]
pub struct ValidateConditionValuesPayload {
    pub kind: String,
    pub op: Op,
    pub values: Vec<String>,
}

#[derive(Debug, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ConditionValueValidation {
    pub valid: bool,
    pub error: Option<String>,
    /// Decimal nanoseconds, never a JavaScript number or rewritten input.
    pub parsed_nanos: Option<String>,
}

#[derive(Debug, Serialize, ts_rs::TS)]
pub struct ConditionValuesValidation {
    pub valid: bool,
    pub hint: Option<String>,
    pub values: Vec<ConditionValueValidation>,
}

const DATETIME_HINT: &str = "Use RFC 3339 (2026-11-12T12:34:56.123456789Z or +09:00), YYYY-MM-DD HH:MM:SS[.fraction], or YYYY-MM-DD. An omitted time zone means UTC.";

/// Checks what a condition value means without evaluating or reading rows.
/// The result is advisory: it does not change existing query semantics.
pub fn validate_condition_values(
    payload: ValidateConditionValuesPayload,
) -> Result<ConditionValuesValidation, AppError> {
    if !matches!(
        payload.kind.as_str(),
        "null" | "int" | "float" | "datetime" | "string"
    ) {
        return Err(AppError::Message("Unknown condition column kind.".into()));
    }
    let ordered = matches!(payload.op, Op::Gt | Op::Ge | Op::Lt | Op::Le);
    let hint = match (payload.op, payload.kind.as_str()) {
        (Op::Regex, _) => "Use a Rust regular expression. Patterns are case-sensitive unless a flag such as (?i) is included.".into(),
        (_, "datetime") if ordered => DATETIME_HINT.into(),
        (Op::Eq | Op::Ne | Op::In, "datetime") => "Equality compares the original timestamp text exactly, including its time zone and fractional digits. Use <, ≤, > or ≥ to compare instants.".into(),
        (_, "int") if ordered => "Enter an exact integer within the signed 128-bit range. No decimal or float rounding is used.".into(),
        (_, "float") if ordered => "Ordered comparisons currently support exact integers and datetimes, not decimal-valued columns. Text equality remains available.".into(),
        (_, _) if ordered => "Ordered comparisons require an exact integer or a recognized datetime. Text is not sorted lexicographically by these conditions.".into(),
        (Op::Contains | Op::StartsWith | Op::EndsWith, _) => "Matches text without case sensitivity; this is not a numeric or datetime comparison.".into(),
        _ => "Matches the original text exactly. Empty text, null and missing values remain distinct.".into(),
    };
    let values: Vec<_> = payload
        .values
        .iter()
        .map(|value| {
            let mut parsed_nanos = None;
            let error = if payload.op == Op::Regex {
                let node = Node::Condition(Condition {
                    column: String::new(),
                    op: payload.op,
                    values: vec![value.clone()],
                    negated: false,
                });
                PreparedNode::prepare(&node)
                    .err()
                    .map(|error| error.to_string())
            } else if ordered {
                match payload.kind.as_str() {
                    "datetime" => {
                        parsed_nanos =
                            parse_datetime_nanos(value.trim()).map(|nanos| nanos.to_string());
                        parsed_nanos.is_none().then(|| {
                            "Enter a recognized date/time within the engine's nanosecond range."
                                .into()
                        })
                    }
                    "int" => {
                        value.trim().parse::<i128>().err().map(|_| {
                            "Enter an exact integer within the signed 128-bit range.".into()
                        })
                    }
                    "float" => Some(
                        "Ordered comparison is not supported for a decimal-valued column.".into(),
                    ),
                    _ => {
                        parsed_nanos =
                            parse_datetime_nanos(value.trim()).map(|nanos| nanos.to_string());
                        (value.trim().parse::<i128>().is_err() && parsed_nanos.is_none())
                            .then(|| "Enter an exact integer or a recognized date/time.".into())
                    }
                }
            } else {
                None
            };
            ConditionValueValidation {
                valid: error.is_none(),
                error,
                parsed_nanos,
            }
        })
        .collect();
    Ok(ConditionValuesValidation {
        valid: !values.is_empty() && values.iter().all(|value| value.valid),
        hint: Some(if values.is_empty() {
            "Enter at least one condition value.".into()
        } else {
            hint
        }),
        values,
    })
}

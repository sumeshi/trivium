use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

use serde_json::Value;

use crate::analysis::infer::parse_datetime_nanos;
use crate::error::AppError;

use super::model::{Condition, GroupKind, Node, Op};

/// A condition that cannot be evaluated as requested. Numeric comparisons are
/// limited to signed 128-bit integers so values are never rounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalError {
    message: String,
}

impl EvalError {
    fn numeric(value: &str) -> Self {
        Self {
            message: format!(
                "Numeric comparison requires integers within i128: {}",
                value
            ),
        }
    }

    fn datetime(value: &str) -> Self {
        Self {
            message: format!("Datetime comparison requires a recognized datetime: {}", value),
        }
    }
}

impl fmt::Display for EvalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for EvalError {}

impl From<EvalError> for AppError {
    fn from(error: EvalError) -> Self {
        AppError::Message(error.to_string())
    }
}

/// Evaluates a condition tree against one row.
///
/// Column names match case-insensitively. An empty column reads the `""` entry,
/// which callers set to the joined searchable text when they want an unscoped
/// term. Equality and text operators use the original cell string. Ordered
/// comparisons use i128 integers or parsed datetimes, and return an error
/// instead of falling back to f64.
pub fn eval(node: &Node, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
    match node {
        Node::Condition(condition) => eval_condition(condition, row),
        Node::Group(group) => match group.kind {
            GroupKind::And => {
                for child in &group.children {
                    if !eval(child, row)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            GroupKind::Or => {
                for child in &group.children {
                    if eval(child, row)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        },
    }
}

fn eval_condition(condition: &Condition, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
    let cell = lookup(row, &condition.column);
    let matched = if condition.values.is_empty() {
        false
    } else {
        match condition.op {
            Op::Eq | Op::In => condition.values.iter().any(|value| eq(cell, value)),
            Op::Ne => !condition.values.iter().any(|value| eq(cell, value)),
            Op::Gt => numeric_any(cell, &condition.values, Ordering::Greater, false)?,
            Op::Lt => numeric_any(cell, &condition.values, Ordering::Less, false)?,
            Op::Ge => numeric_any(cell, &condition.values, Ordering::Greater, true)?,
            Op::Le => numeric_any(cell, &condition.values, Ordering::Less, true)?,
            Op::Contains => text_any(cell, &condition.values, |hay, needle| hay.contains(needle)),
            Op::StartsWith => text_any(cell, &condition.values, |hay, needle| hay.starts_with(needle)),
            Op::EndsWith => text_any(cell, &condition.values, |hay, needle| hay.ends_with(needle)),
            Op::Regex => condition.values.iter().any(|value| regex_match(&text(cell), value)),
        }
    };
    Ok(if condition.negated { !matched } else { matched })
}

fn text_any(cell: Option<&Value>, values: &[String], pred: impl Fn(&str, &str) -> bool) -> bool {
    let hay = text(cell).to_lowercase();
    values
        .iter()
        .any(|value| !value.is_empty() && pred(&hay, &value.to_lowercase()))
}

fn lookup<'a>(row: &'a HashMap<String, Value>, column: &str) -> Option<&'a Value> {
    if let Some(value) = row.get(column) {
        return Some(value);
    }
    row.iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column))
        .map(|(_, value)| value)
}

fn text(cell: Option<&Value>) -> String {
    match cell {
        Some(Value::Null) | None => String::new(),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(other) => other.to_string(),
    }
}

/// Equality is exact text equality. Null and a missing value never equal an
/// empty string, and no numeric coercion is applied.
fn eq(cell: Option<&Value>, raw: &str) -> bool {
    match cell {
        None | Some(Value::Null) => false,
        Some(value) => text(Some(value)) == raw,
    }
}

fn numeric_any(
    cell: Option<&Value>,
    values: &[String],
    want: Ordering,
    or_equal: bool,
) -> Result<bool, EvalError> {
    let Some(cell) = cell else {
        return Ok(false);
    };
    if matches!(cell, Value::Null) {
        return Ok(false);
    }
    let mut matched = false;
    for raw in values {
        if let Some(ordering) = numeric_cmp(cell, raw)? {
            if ordering == want || (or_equal && ordering == Ordering::Equal) {
                matched = true;
            }
        }
    }
    Ok(matched)
}

/// Compares an integer cell or a datetime cell without a float round trip.
fn numeric_cmp(cell: &Value, raw: &str) -> Result<Option<Ordering>, EvalError> {
    let left = match cell {
        Value::Null => return Ok(None),
        Value::String(value) => value.trim().to_string(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        other => other.to_string(),
    };

    if let Ok(left) = left.parse::<i128>() {
        let right = parse_i128(raw)?;
        return Ok(Some(left.cmp(&right)));
    }
    if let Some(left) = parse_datetime_nanos(&left) {
        let right_text = raw.trim();
        let right = parse_datetime_nanos(right_text)
            .ok_or_else(|| EvalError::datetime(right_text))?;
        return Ok(Some(left.cmp(&right)));
    }
    Err(EvalError::numeric(&left))
}

fn parse_i128(raw: &str) -> Result<i128, EvalError> {
    let text = raw.trim();
    text.parse::<i128>().map_err(|_| EvalError::numeric(text))
}

fn regex_match(text: &str, pattern: &str) -> bool {
    regex::Regex::new(pattern)
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

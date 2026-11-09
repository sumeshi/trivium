use std::cmp::Ordering;
use std::collections::HashMap;

use serde_json::Value;

use super::model::{Condition, GroupKind, Node, Op};

/// Evaluates a condition tree against one row.
///
/// Column names match case-insensitively. An empty column reads the `""` entry,
/// which callers set to the joined searchable text when they want an unscoped
/// term. Numeric comparisons try `i64` before `f64`.
pub fn eval(node: &Node, row: &HashMap<String, Value>) -> bool {
    match node {
        Node::Condition(condition) => eval_condition(condition, row),
        Node::Group(group) => match group.kind {
            GroupKind::And => group.children.iter().all(|child| eval(child, row)),
            GroupKind::Or => group.children.iter().any(|child| eval(child, row)),
        },
    }
}

fn eval_condition(condition: &Condition, row: &HashMap<String, Value>) -> bool {
    let cell = lookup(row, &condition.column);
    let matched = if condition.values.is_empty() {
        false
    } else {
        match condition.op {
            Op::Eq | Op::In => condition.values.iter().any(|value| eq(cell, value)),
            Op::Ne => !condition.values.iter().any(|value| eq(cell, value)),
            Op::Gt => numeric_any(cell, &condition.values, Ordering::Greater, false),
            Op::Lt => numeric_any(cell, &condition.values, Ordering::Less, false),
            Op::Ge => numeric_any(cell, &condition.values, Ordering::Greater, true),
            Op::Le => numeric_any(cell, &condition.values, Ordering::Less, true),
            Op::Contains => text_any(cell, &condition.values, |hay, needle| hay.contains(needle)),
            Op::StartsWith => text_any(cell, &condition.values, |hay, needle| hay.starts_with(needle)),
            Op::EndsWith => text_any(cell, &condition.values, |hay, needle| hay.ends_with(needle)),
            Op::Regex => condition.values.iter().any(|value| regex_match(&text(cell), value)),
        }
    };
    if condition.negated {
        !matched
    } else {
        matched
    }
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

fn eq(cell: Option<&Value>, raw: &str) -> bool {
    let Some(cell) = cell else {
        return false;
    };
    if matches!(cell, Value::Number(_)) {
        return numeric_cmp(cell, raw) == Some(Ordering::Equal);
    }
    text(Some(cell)) == raw
}

fn numeric_any(cell: Option<&Value>, values: &[String], want: Ordering, or_equal: bool) -> bool {
    let Some(cell) = cell else {
        return false;
    };
    values.iter().any(|raw| {
        numeric_cmp(cell, raw)
            .is_some_and(|ordering| ordering == want || (or_equal && ordering == Ordering::Equal))
    })
}

/// `i64` when both sides are integers, otherwise `f64`.
fn numeric_cmp(cell: &Value, raw: &str) -> Option<Ordering> {
    if let (Some(left), Some(right)) = (as_i64(cell), raw.parse::<i64>().ok()) {
        return Some(left.cmp(&right));
    }
    let left = as_f64(cell)?;
    let right = raw.parse::<f64>().ok()?;
    left.partial_cmp(&right)
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn regex_match(text: &str, pattern: &str) -> bool {
    regex::Regex::new(pattern)
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

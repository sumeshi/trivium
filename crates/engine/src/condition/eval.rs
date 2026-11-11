use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use regex::Regex;
use serde_json::Value;

use crate::analysis::infer::parse_datetime_nanos;
use crate::error::AppError;

use super::model::{GroupKind, Node, Op};

/// Test instrumentation: regex compilations in this process.
///
/// Query evaluation compiles every pattern once during preparation, so the
/// count for one query is proportional to the number of patterns, not rows.
static REGEX_COMPILES: AtomicU64 = AtomicU64::new(0);

pub fn regex_compile_count() -> u64 {
    REGEX_COMPILES.load(AtomicOrdering::SeqCst)
}

/// Resets the test instrumentation counter.
pub fn reset_regex_compile_count() {
    REGEX_COMPILES.store(0, AtomicOrdering::SeqCst);
}

/// A condition tree whose regex patterns are already compiled.
///
/// Preparing once per query keeps compilation independent of the row count.
/// Column validation belongs to the caller, which knows the data columns.
#[derive(Debug)]
pub enum PreparedNode {
    Condition(PreparedCondition),
    Group(PreparedGroup),
}

#[derive(Debug)]
pub struct PreparedCondition {
    column: String,
    op: Op,
    values: Vec<String>,
    regexes: Vec<Regex>,
    negated: bool,
}

#[derive(Debug)]
pub struct PreparedGroup {
    kind: GroupKind,
    children: Vec<PreparedNode>,
}

impl PreparedNode {
    pub fn prepare(node: &Node) -> Result<Self, EvalError> {
        match node {
            Node::Condition(condition) => {
                let regexes = if condition.op == Op::Regex {
                    condition
                        .values
                        .iter()
                        .map(|pattern| compile_regex(pattern))
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    Vec::new()
                };
                Ok(Self::Condition(PreparedCondition {
                    column: condition.column.clone(),
                    op: condition.op,
                    values: condition.values.clone(),
                    regexes,
                    negated: condition.negated,
                }))
            }
            Node::Group(group) => Ok(Self::Group(PreparedGroup {
                kind: group.kind,
                children: group
                    .children
                    .iter()
                    .map(PreparedNode::prepare)
                    .collect::<Result<Vec<_>, _>>()?,
            })),
        }
    }

    /// Evaluates the prepared tree against one row. No regex is compiled here.
    pub fn eval(&self, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
        match self {
            Self::Condition(condition) => condition.eval(row),
            Self::Group(group) => match group.kind {
                GroupKind::And => {
                    for child in &group.children {
                        if !child.eval(row)? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                GroupKind::Or => {
                    for child in &group.children {
                        if child.eval(row)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
            },
        }
    }

    /// Column names referenced by this tree. An empty name is the joined
    /// searchable text.
    pub fn columns(&self) -> Vec<&str> {
        let mut columns = Vec::new();
        self.collect_columns(&mut columns);
        columns
    }

    fn collect_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::Condition(condition) => columns.push(condition.column.as_str()),
            Self::Group(group) => {
                for child in &group.children {
                    child.collect_columns(columns);
                }
            }
        }
    }
}

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

    fn regex(pattern: &str, error: regex::Error) -> Self {
        Self {
            message: format!("Invalid regex {:?}: {}", pattern, error),
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
/// This compatibility entry point prepares the tree for each call, so hot
/// query paths use [`PreparedNode`] instead. Column names match
/// case-insensitively. An empty column reads the `""` entry, which callers set
/// to the joined searchable text when they want an unscoped term. Equality and
/// text operators use the original cell string. Ordered comparisons use i128
/// integers or parsed datetimes, and return an error instead of falling back to
/// f64. Invalid regex patterns are reported as errors.
pub fn eval(node: &Node, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
    PreparedNode::prepare(node)?.eval(row)
}

impl PreparedCondition {
    fn eval(&self, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
        let cell = lookup(row, &self.column);
        let matched = if self.values.is_empty() {
            false
        } else {
            match self.op {
                Op::Eq | Op::In => self.values.iter().any(|value| eq(cell, value)),
                Op::Ne => !self.values.iter().any(|value| eq(cell, value)),
                Op::Gt => numeric_any(cell, &self.values, Ordering::Greater, false)?,
                Op::Lt => numeric_any(cell, &self.values, Ordering::Less, false)?,
                Op::Ge => numeric_any(cell, &self.values, Ordering::Greater, true)?,
                Op::Le => numeric_any(cell, &self.values, Ordering::Less, true)?,
                Op::Contains => {
                    text_any(cell, &self.values, |hay, needle| hay.contains(needle))
                }
                Op::StartsWith => {
                    text_any(cell, &self.values, |hay, needle| hay.starts_with(needle))
                }
                Op::EndsWith => {
                    text_any(cell, &self.values, |hay, needle| hay.ends_with(needle))
                }
                Op::Regex => self.regexes.iter().any(|re| re.is_match(&text(cell))),
            }
        };
        Ok(if self.negated { !matched } else { matched })
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

fn compile_regex(pattern: &str) -> Result<Regex, EvalError> {
    REGEX_COMPILES.fetch_add(1, AtomicOrdering::SeqCst);
    Regex::new(pattern).map_err(|error| EvalError::regex(pattern, error))
}

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use regex::Regex;
use serde_json::Value;

use crate::analysis::infer::parse_datetime_nanos;
use crate::error::AppError;
use crate::search::{TAG_ANY_COLUMN, TAG_EXACT_COLUMN};

use super::model::{GroupKind, Node, Op};

mod bound;
pub use bound::BoundNode;

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
    Tag(PreparedTag),
    Group(PreparedGroup),
}

#[derive(Debug)]
pub struct PreparedTag {
    value: String,
    any: bool,
    negated: bool,
}

impl PreparedTag {
    fn eval(&self, tags: &[String]) -> bool {
        let matched = if self.any {
            !tags.is_empty()
        } else {
            tags.iter().any(|tag| {
                if tag.is_ascii() && self.value.is_ascii() {
                    tag.eq_ignore_ascii_case(&self.value)
                } else {
                    tag.to_lowercase() == self.value
                }
            })
        };
        self.negated ^ matched
    }
}

#[derive(Debug)]
pub struct PreparedCondition {
    column: String,
    op: Op,
    values: Vec<String>,
    lower_values: Vec<String>,
    regexes: Vec<Regex>,
    negated: bool,
}

#[derive(Debug)]
pub struct PreparedGroup {
    kind: GroupKind,
    children: Vec<PreparedNode>,
}

impl PreparedNode {
    /// A lone exact-text predicate can use a string column's equality kernel.
    /// Ordered numeric/datetime operations deliberately keep the exact scalar
    /// evaluator; parsing raw source text must never introduce float coercion.
    pub fn exact_text_comparison(&self) -> Option<(&str, &[String], bool)> {
        let Self::Condition(condition) = self else {
            return None;
        };
        if condition.column.is_empty() || !matches!(condition.op, Op::Eq | Op::Ne | Op::In) {
            return None;
        }
        let inverted = condition.negated ^ (condition.op == Op::Ne && !condition.values.is_empty());
        Some((&condition.column, &condition.values, inverted))
    }

    pub fn prepare(node: &Node) -> Result<Self, EvalError> {
        Self::prepare_inner(node, false)
    }

    /// Metadata markers come only from the search tokenizer; structured
    /// conditions continue to address source columns without a new wire op.
    pub(crate) fn prepare_search(node: &Node) -> Result<Self, EvalError> {
        Self::prepare_inner(node, true)
    }

    pub(crate) fn and(self, other: Self) -> Self {
        Self::Group(PreparedGroup {
            kind: GroupKind::And,
            children: vec![self, other],
        })
    }

    fn prepare_inner(node: &Node, metadata: bool) -> Result<Self, EvalError> {
        match node {
            Node::Condition(condition)
                if metadata
                    && matches!(condition.column.as_str(), TAG_EXACT_COLUMN | TAG_ANY_COLUMN) =>
            {
                Ok(Self::Tag(PreparedTag {
                    value: condition.values.first().cloned().unwrap_or_default(),
                    any: condition.column == TAG_ANY_COLUMN,
                    negated: condition.negated,
                }))
            }
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
                    lower_values: condition
                        .values
                        .iter()
                        .map(|value| value.to_lowercase())
                        .collect(),
                    regexes,
                    negated: condition.negated,
                }))
            }
            Node::Group(group) => Ok(Self::Group(PreparedGroup {
                kind: group.kind,
                children: group
                    .children
                    .iter()
                    .map(|node| Self::prepare_inner(node, metadata))
                    .collect::<Result<Vec<_>, _>>()?,
            })),
        }
    }

    /// Evaluates the prepared tree against one row. No regex is compiled here.
    pub fn eval(&self, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
        match self {
            Self::Condition(condition) => condition.eval(row),
            Self::Tag(condition) => Ok(condition.eval(&[])),
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
            Self::Tag(_) => {}
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
            message: format!(
                "Datetime comparison requires a recognized datetime: {}",
                value
            ),
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
        let cell = lookup(row, &self.column).and_then(|value| match value {
            Value::Null => None,
            Value::String(value) => Some(Cow::Borrowed(value.as_str())),
            other => Some(Cow::Owned(other.to_string())),
        });
        self.eval_text(cell.as_deref())
    }

    /// Evaluate the original textual cell without constructing a JSON row.
    pub fn eval_text(&self, cell: Option<&str>) -> Result<bool, EvalError> {
        let matched = if self.values.is_empty() {
            false
        } else {
            match self.op {
                Op::Eq | Op::In => self.values.iter().any(|value| cell == Some(value.as_str())),
                Op::Ne => !self.values.iter().any(|value| cell == Some(value.as_str())),
                Op::Gt => numeric_any(cell, &self.values, Ordering::Greater, false)?,
                Op::Lt => numeric_any(cell, &self.values, Ordering::Less, false)?,
                Op::Ge => numeric_any(cell, &self.values, Ordering::Greater, true)?,
                Op::Le => numeric_any(cell, &self.values, Ordering::Less, true)?,
                Op::Contains | Op::StartsWith | Op::EndsWith => {
                    text_any(cell, &self.lower_values, self.op)
                }
                Op::Regex => self
                    .regexes
                    .iter()
                    .any(|re| re.is_match(cell.unwrap_or_default())),
            }
        };
        Ok(if self.negated { !matched } else { matched })
    }
}

fn text_any(cell: Option<&str>, values: &[String], op: Op) -> bool {
    let text = cell.unwrap_or_default();
    if text.is_ascii() {
        // ASCII folds one byte to one byte. Borrow the original buffer even
        // for uppercase logs; Unicode keeps Rust's contextual lowercase below.
        return values
            .iter()
            .filter(|value| !value.is_empty() && value.is_ascii())
            .any(|value| {
                let hay = text.as_bytes();
                let needle = value.as_bytes();
                match op {
                    Op::Contains => hay
                        .windows(needle.len())
                        .any(|part| part.eq_ignore_ascii_case(needle)),
                    Op::StartsWith => hay
                        .get(..needle.len())
                        .is_some_and(|part| part.eq_ignore_ascii_case(needle)),
                    Op::EndsWith => hay
                        .get(hay.len().saturating_sub(needle.len())..)
                        .is_some_and(|part| part.eq_ignore_ascii_case(needle)),
                    _ => unreachable!("text operator"),
                }
            });
    }
    let hay = text.to_lowercase();
    values
        .iter()
        .filter(|value| !value.is_empty())
        .any(|value| match op {
            Op::Contains => hay.contains(value),
            Op::StartsWith => hay.starts_with(value),
            Op::EndsWith => hay.ends_with(value),
            _ => unreachable!("text operator"),
        })
}

fn lookup<'a>(row: &'a HashMap<String, Value>, column: &str) -> Option<&'a Value> {
    if let Some(value) = row.get(column) {
        return Some(value);
    }
    row.iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column))
        .map(|(_, value)| value)
}

fn numeric_any(
    cell: Option<&str>,
    values: &[String],
    want: Ordering,
    or_equal: bool,
) -> Result<bool, EvalError> {
    let Some(cell) = cell else {
        return Ok(false);
    };
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
fn numeric_cmp(cell: &str, raw: &str) -> Result<Option<Ordering>, EvalError> {
    let left = cell.trim();

    if let Ok(left) = left.parse::<i128>() {
        let right = parse_i128(raw)?;
        return Ok(Some(left.cmp(&right)));
    }
    if let Some(left) = parse_datetime_nanos(left) {
        let right_text = raw.trim();
        let right =
            parse_datetime_nanos(right_text).ok_or_else(|| EvalError::datetime(right_text))?;
        return Ok(Some(left.cmp(&right)));
    }
    Err(EvalError::numeric(left))
}

fn parse_i128(raw: &str) -> Result<i128, EvalError> {
    let text = raw.trim();
    text.parse::<i128>().map_err(|_| EvalError::numeric(text))
}

fn compile_regex(pattern: &str) -> Result<Regex, EvalError> {
    REGEX_COMPILES.fetch_add(1, AtomicOrdering::SeqCst);
    Regex::new(pattern).map_err(|error| EvalError::regex(pattern, error))
}

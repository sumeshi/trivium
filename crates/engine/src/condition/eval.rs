use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use regex::{Regex, RegexBuilder};
use serde_json::Value;

use crate::error::AppError;
use crate::search::{TAG_ANY_COLUMN, TAG_EXACT_COLUMN};

use super::model::{GroupKind, Node, Op};

mod bound;
mod budget;
mod formats;
mod ordered;
pub use bound::BoundNode;
pub use formats::PreparedFormat;
use ordered::{ordered_matches, OrderedBound};

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
    Format(Box<PreparedFormat>),
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
    ordered_values: Vec<OrderedBound>,
    negated: bool,
}

#[derive(Debug)]
pub struct PreparedGroup {
    kind: GroupKind,
    children: Vec<PreparedNode>,
}

impl PreparedNode {
    pub(crate) fn contains_text_group(&self) -> Option<(GroupKind, &[PreparedNode])> {
        match self {
            Self::Condition(condition) if condition.op == Op::Contains => {
                Some((GroupKind::And, std::slice::from_ref(self)))
            }
            Self::Group(group)
                if group
                    .children
                    .iter()
                    .all(|child| child.contains_text_comparison().is_some()) =>
            {
                Some((group.kind, &group.children))
            }
            _ => None,
        }
    }

    pub(crate) fn contains_text_comparison(&self) -> Option<(&str, &PreparedCondition, bool)> {
        match self {
            Self::Condition(condition) if condition.op == Op::Contains => {
                Some((&condition.column, condition, condition.negated))
            }
            _ => None,
        }
    }

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
        budget::validate(node)?;
        Self::prepare_inner(node, false)
    }

    /// Metadata markers come only from the search tokenizer; structured
    /// conditions continue to address source columns without a new wire op.
    pub(crate) fn prepare_search(node: &Node) -> Result<Self, EvalError> {
        budget::validate(node)?;
        Self::prepare_inner(node, true)
    }

    pub(crate) fn and(self, other: Self) -> Self {
        Self::group(GroupKind::And, vec![self, other])
    }

    fn group(kind: GroupKind, children: Vec<Self>) -> Self {
        let mut flattened = Vec::new();
        for child in children {
            match child {
                Self::Group(group) if group.kind == kind => flattened.extend(group.children),
                child => flattened.push(child),
            }
        }
        // Preparation validates the original tree's budget first. Flatten only
        // associative groups, keeping source order and mixed-group boundaries.
        Self::Group(PreparedGroup {
            kind,
            children: flattened,
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
                let ordered_values = if matches!(condition.op, Op::Gt | Op::Ge | Op::Lt | Op::Le) {
                    condition
                        .values
                        .iter()
                        .map(|value| OrderedBound::parse(value))
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    Vec::new()
                };
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
                    ordered_values,
                    negated: condition.negated,
                }))
            }
            Node::Format(condition) => PreparedFormat::new(condition)
                .map(Box::new)
                .map(Self::Format),
            Node::Group(group) => Ok(Self::group(
                group.kind,
                group
                    .children
                    .iter()
                    .map(|node| Self::prepare_inner(node, metadata))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        }
    }

    /// Evaluates the prepared tree against one row. No regex is compiled here.
    pub fn eval(&self, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
        match self {
            Self::Condition(condition) => condition.eval(row),
            Self::Tag(condition) => Ok(condition.eval(&[])),
            Self::Format(condition) => Ok(condition.eval(row)),
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

    /// Column names referenced by this tree. An empty name is the selected
    /// search scope: Contains matches one cell, other operations use joined text.
    pub fn columns(&self) -> Vec<&str> {
        let mut columns = Vec::new();
        self.collect_columns(&mut columns);
        columns
    }

    pub(crate) fn needs_joined_text(&self) -> bool {
        match self {
            Self::Condition(condition) => {
                condition.column.is_empty() && condition.op != Op::Contains
            }
            Self::Group(group) => group.children.iter().any(Self::needs_joined_text),
            Self::Format(_) | Self::Tag(_) => false,
        }
    }

    fn collect_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::Condition(condition) => columns.push(condition.column.as_str()),
            Self::Tag(_) => {}
            Self::Format(condition) => columns.extend(condition.columns.iter().map(String::as_str)),
            Self::Group(group) => {
                for child in &group.children {
                    child.collect_columns(columns);
                }
            }
        }
    }
}

/// A condition that cannot be evaluated as requested. Numeric comparisons keep
/// decimal digits exact and never round through a floating-point value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalError {
    message: String,
}

impl EvalError {
    fn ordered(value: &str) -> Self {
        Self {
            message: format!(
                "Ordered comparison requires an exact finite number or a recognized datetime: {}",
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
/// This entry point prepares the tree for each call, so hot
/// query paths use [`PreparedNode`] instead. Column names match
/// case-insensitively. An empty Contains column searches the supplied data
/// cells without crossing their boundaries. Other empty-column predicates read
/// the `""` joined-text entry.
/// Equality and text operators use the original cell string. Ordered comparisons use exact
/// base-ten numbers or parsed datetimes according to each bound. Empty/null,
/// missing and unparseable source values do not match an ordered predicate;
/// NOT inverts that result as for any other predicate. Source text is unchanged.
/// Invalid bounds and regex patterns are rejected during preparation.
pub fn eval(node: &Node, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
    PreparedNode::prepare(node)?.eval(row)
}

impl PreparedCondition {
    fn eval(&self, row: &HashMap<String, Value>) -> Result<bool, EvalError> {
        if self.is_unscoped_contains() {
            let matched = row.iter().any(|(column, value)| {
                if column.is_empty() {
                    return false;
                }
                match value {
                    Value::Null => false,
                    Value::String(value) => self.contains_text(Some(value)),
                    other => self.contains_text(Some(&other.to_string())),
                }
            });
            return Ok(self.negated ^ matched);
        }
        let cell = lookup(row, &self.column).and_then(|value| match value {
            Value::Null => None,
            Value::String(value) => Some(Cow::Borrowed(value.as_str())),
            other => Some(Cow::Owned(other.to_string())),
        });
        self.eval_text(cell.as_deref())
    }

    pub(super) fn is_unscoped_contains(&self) -> bool {
        self.column.is_empty() && self.op == Op::Contains
    }

    pub(crate) fn contains_text(&self, cell: Option<&str>) -> bool {
        text_any(cell, &self.lower_values, Op::Contains)
    }

    /// Evaluate the original textual cell without constructing a JSON row.
    pub fn eval_text(&self, cell: Option<&str>) -> Result<bool, EvalError> {
        let matched = if self.values.is_empty() {
            false
        } else {
            match self.op {
                Op::Eq | Op::In => self.values.iter().any(|value| cell == Some(value.as_str())),
                Op::Ne => !self.values.iter().any(|value| cell == Some(value.as_str())),
                Op::Gt => ordered_matches(cell, &self.ordered_values, Ordering::Greater, false),
                Op::Lt => ordered_matches(cell, &self.ordered_values, Ordering::Less, false),
                Op::Ge => ordered_matches(cell, &self.ordered_values, Ordering::Greater, true),
                Op::Le => ordered_matches(cell, &self.ordered_values, Ordering::Less, true),
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
                    Op::Contains => ascii_contains(hay, needle),
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

/// Find candidate positions before comparing the whole ASCII literal. A
/// repeated leading byte uses the first different byte as its anchor, avoiding
/// a full comparison at every position in inputs such as `aaaa...aaab`.
fn ascii_contains(hay: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return false;
    }
    let Some(last_start) = hay.len().checked_sub(needle.len()) else {
        return false;
    };
    let pivot = if needle.len() > 1 && needle[0] == needle[1] {
        needle
            .iter()
            .position(|&byte| byte != needle[0])
            .unwrap_or(0)
    } else {
        0
    };
    let anchor = needle[pivot];
    // An offset in this slice is also the start of a complete candidate in hay.
    let candidates = &hay[pivot..pivot + last_start + 1];
    memchr::memchr2_iter(
        anchor.to_ascii_lowercase(),
        anchor.to_ascii_uppercase(),
        candidates,
    )
    .any(|start| hay[start..start + needle.len()].eq_ignore_ascii_case(needle))
}

fn lookup<'a>(row: &'a HashMap<String, Value>, column: &str) -> Option<&'a Value> {
    if let Some(value) = row.get(column) {
        return Some(value);
    }
    row.iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column))
        .map(|(_, value)| value)
}

fn compile_regex(pattern: &str) -> Result<Regex, EvalError> {
    REGEX_COMPILES.fetch_add(1, AtomicOrdering::SeqCst);
    RegexBuilder::new(pattern)
        .size_limit(2 * 1024 * 1024)
        .dfa_size_limit(2 * 1024 * 1024)
        .build()
        .map_err(|error| EvalError::regex(pattern, error))
}

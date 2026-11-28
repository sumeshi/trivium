use std::borrow::Cow;

use super::{EvalError, GroupKind, PreparedCondition, PreparedFormat, PreparedNode, PreparedTag};
use crate::error::AppError;

/// Prepared predicates reference column slots, not per-row maps. `None` is
/// the selected search cells (or their joined text for other unscoped ops).
pub enum BoundNode<'a> {
    Condition {
        condition: &'a PreparedCondition,
        slot: Option<usize>,
    },
    Format {
        condition: &'a PreparedFormat,
        slots: Vec<usize>,
    },
    Tag(&'a PreparedTag),
    Group {
        kind: GroupKind,
        children: Vec<BoundNode<'a>>,
    },
}

impl PreparedNode {
    pub(crate) fn conjunction(&self) -> &[Self] {
        match self {
            Self::Group(group) if group.kind == GroupKind::And => &group.children,
            _ => std::slice::from_ref(self),
        }
    }

    pub(crate) fn into_conjunction(self) -> Vec<Self> {
        match self {
            Self::Group(group) if group.kind == GroupKind::And => group.children,
            node => vec![node],
        }
    }

    pub(crate) fn from_conjunction(mut children: Vec<Self>) -> Self {
        if children.len() == 1 {
            children.pop().expect("one conjunction child")
        } else {
            Self::group(GroupKind::And, children)
        }
    }

    pub(crate) fn reads_tags(&self) -> bool {
        match self {
            Self::Tag(_) => true,
            Self::Group(group) => group.children.iter().any(Self::reads_tags),
            Self::Condition(_) | Self::Format(_) => false,
        }
    }

    pub fn bind_columns(&self, resolve: &impl Fn(&str) -> usize) -> BoundNode<'_> {
        match self {
            Self::Condition(condition) => BoundNode::Condition {
                slot: (!condition.column.is_empty()).then(|| resolve(&condition.column)),
                condition,
            },
            Self::Format(condition) => BoundNode::Format {
                slots: condition
                    .columns
                    .iter()
                    .map(|column| resolve(column))
                    .collect(),
                condition,
            },
            Self::Tag(condition) => BoundNode::Tag(condition),
            Self::Group(group) => BoundNode::Group {
                kind: group.kind,
                children: group
                    .children
                    .iter()
                    .map(|child| child.bind_columns(resolve))
                    .collect(),
            },
        }
    }
}

impl BoundNode<'_> {
    pub fn eval(
        &self,
        cells: &[Option<Cow<'_, str>>],
        joined: Option<&str>,
    ) -> Result<bool, EvalError> {
        self.eval_with_tags(cells, joined, &[])
    }

    pub(crate) fn eval_with_tags(
        &self,
        cells: &[Option<Cow<'_, str>>],
        joined: Option<&str>,
        tags: &[String],
    ) -> Result<bool, EvalError> {
        self.eval_with_tags_and_check(cells, cells, joined, tags, &mut || Ok(()))
            .map_err(|error| EvalError {
                message: error.to_string(),
            })
    }

    pub(crate) fn eval_with_tags_and_check(
        &self,
        cells: &[Option<Cow<'_, str>>],
        unscoped_cells: &[Option<Cow<'_, str>>],
        joined: Option<&str>,
        tags: &[String],
        check: &mut impl FnMut() -> Result<(), AppError>,
    ) -> Result<bool, AppError> {
        match self {
            Self::Condition {
                condition,
                slot: None,
            } if condition.is_unscoped_contains() => {
                let mut matched = false;
                for cell in unscoped_cells {
                    check()?;
                    if condition.contains_text(cell.as_deref()) {
                        matched = true;
                        break;
                    }
                }
                Ok(condition.negated ^ matched)
            }
            Self::Condition { condition, slot } => condition
                .eval_text(match slot {
                    Some(slot) => cells.get(*slot).and_then(Option::as_deref),
                    None => joined,
                })
                .map_err(AppError::from),
            Self::Format { condition, slots } => condition.eval_cells_with_check(
                slots
                    .iter()
                    .map(|slot| cells.get(*slot).and_then(Option::as_deref)),
                check,
            ),
            Self::Tag(condition) => Ok(condition.eval(tags)),
            Self::Group {
                kind: GroupKind::And,
                children,
            } => {
                for child in children {
                    if !child.eval_with_tags_and_check(
                        cells,
                        unscoped_cells,
                        joined,
                        tags,
                        check,
                    )? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Self::Group {
                kind: GroupKind::Or,
                children,
            } => {
                for child in children {
                    if child.eval_with_tags_and_check(cells, unscoped_cells, joined, tags, check)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::Node;
    use serde_json::json;

    #[test]
    fn cancelled_format_walk_propagates_through_groups_and_negation() {
        let text = "x".repeat(4096);
        let cells = [Some(Cow::Borrowed(text.as_str()))];
        for kind in ["and", "or"] {
            for negated in [false, true] {
                let node: Node = serde_json::from_value(json!({
                    "kind": kind, "children": [{
                        "columns": ["value"], "negated": negated,
                        "format": { "schemaVersion": 1, "id": "rejected", "name": "Rejected candidates",
                            "pattern": "(?P<value>x)", "valueGroup": "value", "validation": "ipv4-like" }
                    }]
                })).unwrap();
                let prepared = PreparedNode::prepare(&node).unwrap();
                let bound = prepared.bind_columns(&|_| 0);
                let mut checks = 0;
                let error = bound
                    .eval_with_tags_and_check(&cells, &cells, None, &[], &mut || {
                        checks += 1;
                        if checks == 2 {
                            Err(AppError::Message("Cancelled.".into()))
                        } else {
                            Ok(())
                        }
                    })
                    .unwrap_err();
                assert_eq!(error.to_string(), "Cancelled.");
                assert_eq!(checks, 2);
                assert_eq!(
                    bound.eval(&cells, None).unwrap(),
                    negated,
                    "uncancelled predicates keep their previous membership"
                );
            }
        }
    }
}

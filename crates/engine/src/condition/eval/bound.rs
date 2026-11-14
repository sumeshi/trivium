use std::borrow::Cow;

use super::{EvalError, GroupKind, PreparedCondition, PreparedNode, PreparedTag};

/// Prepared predicates reference column slots, not per-row maps. `None` is
/// the already joined unscoped search text; values remain original strings.
pub enum BoundNode<'a> {
    Condition {
        condition: &'a PreparedCondition,
        slot: Option<usize>,
    },
    Tag(&'a PreparedTag),
    Group {
        kind: GroupKind,
        children: Vec<BoundNode<'a>>,
    },
}

impl PreparedNode {
    pub fn bind_columns(&self, resolve: &impl Fn(&str) -> usize) -> BoundNode<'_> {
        match self {
            Self::Condition(condition) => BoundNode::Condition {
                slot: (!condition.column.is_empty()).then(|| resolve(&condition.column)),
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
        match self {
            Self::Condition { condition, slot } => condition.eval_text(match slot {
                Some(slot) => cells.get(*slot).and_then(Option::as_deref),
                None => joined,
            }),
            Self::Tag(condition) => Ok(condition.eval(tags)),
            Self::Group {
                kind: GroupKind::And,
                children,
            } => {
                for child in children {
                    if !child.eval_with_tags(cells, joined, tags)? {
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
                    if child.eval_with_tags(cells, joined, tags)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }
}

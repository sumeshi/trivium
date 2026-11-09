use crate::search::{to_rpn, tokenize_search_query, SearchToken};

use super::model::{Condition, Group, GroupKind, Node, Op};

/// Converts the existing search-bar syntax into a condition tree.
/// Empty input and queries with no terms yield `None` (no filter).
pub fn search_to_node(input: &str) -> Option<Node> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }
    let tokens = tokenize_search_query(trimmed);
    let has_term = tokens.iter().any(|token| match token {
        SearchToken::Term { text, .. } | SearchToken::QuotedTerm { text, .. } => !text.is_empty(),
        _ => false,
    });
    if !has_term {
        return None;
    }
    rpn_to_node(&to_rpn(&tokens))
}

pub fn and_nodes(left: Node, right: Node) -> Node {
    Node::Group(Group {
        kind: GroupKind::And,
        children: vec![left, right],
    })
}

fn rpn_to_node(rpn: &[SearchToken]) -> Option<Node> {
    let mut stack: Vec<Node> = Vec::new();
    for token in rpn {
        match token {
            SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } => {
                stack.push(Node::Condition(Condition {
                    column: col.clone().unwrap_or_default(),
                    op: Op::Contains,
                    values: vec![text.clone()],
                    negated: false,
                }));
            }
            SearchToken::Not => {
                if let Some(node) = stack.pop() {
                    stack.push(negate(node));
                }
            }
            SearchToken::And => push_binary(&mut stack, GroupKind::And),
            SearchToken::Or => push_binary(&mut stack, GroupKind::Or),
        }
    }
    match stack.len() {
        0 => None,
        1 => stack.pop(),
        _ => Some(Node::Group(Group {
            kind: GroupKind::And,
            children: stack,
        })),
    }
}

fn push_binary(stack: &mut Vec<Node>, kind: GroupKind) {
    let right = stack.pop();
    let left = stack.pop();
    match (left, right) {
        (Some(left), Some(right)) => stack.push(Node::Group(Group {
            kind,
            children: vec![left, right],
        })),
        (Some(only), None) | (None, Some(only)) => stack.push(only),
        (None, None) => {}
    }
}

fn negate(node: Node) -> Node {
    match node {
        Node::Condition(mut condition) => {
            condition.negated = !condition.negated;
            Node::Condition(condition)
        }
        Node::Group(group) => {
            let kind = match group.kind {
                GroupKind::And => GroupKind::Or,
                GroupKind::Or => GroupKind::And,
            };
            Node::Group(Group {
                kind,
                children: group.children.into_iter().map(negate).collect(),
            })
        }
    }
}

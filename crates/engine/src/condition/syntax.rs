use crate::search::{
    prepare_tag_tokens, to_rpn, tokenize_search_query, validate_search_query,
    validate_search_tokens, SearchToken,
};

use super::model::{Condition, Group, GroupKind, Node, Op};

/// Converts the existing search-bar syntax into a condition tree.
/// Empty input yields `None` (no filter). Invalid or oversized queries return
/// an error before constructing a condition tree.
pub fn search_to_node(input: &str) -> Result<Option<Node>, String> {
    validate_search_query(input)?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    search_tokens_to_node(&tokenize_search_query(trimmed))
}

/// Converts already-tokenized search input into a condition tree.
///
/// Query preparation tokenizes the search string once and uses this entry point
/// so the same tokens feed the prepared condition tree.
pub fn search_tokens_to_node(tokens: &[SearchToken]) -> Result<Option<Node>, String> {
    if tokens.is_empty() {
        return Ok(None);
    }
    validate_search_tokens(tokens)?;
    // The public token helper also rejects malformed tag escapes. Query
    // preparation has already decoded these into internal metadata scopes.
    prepare_tag_tokens(tokens)?;
    Ok(rpn_to_node(&to_rpn(tokens)))
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
        Node::Format(mut condition) => {
            condition.negated = !condition.negated;
            Node::Format(condition)
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

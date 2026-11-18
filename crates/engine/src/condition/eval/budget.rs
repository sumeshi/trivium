//! Bound a whole condition before compiling any of its patterns. Individual
//! regex limits alone permit a tiny rule to retain arbitrarily many large NFAs.

use super::{EvalError, Node, Op};

const MAX_NODES: usize = 1024;
const MAX_DEPTH: usize = 64;
const MAX_PATTERNS: usize = 32;

pub(super) fn validate(root: &Node) -> Result<(), EvalError> {
    let mut pending = vec![(root, 1usize)];
    let mut nodes = 0usize;
    let mut patterns = 0usize;
    while let Some((node, depth)) = pending.pop() {
        nodes += 1;
        if nodes > MAX_NODES {
            return Err(EvalError {
                message: format!("Condition exceeds the limit of {MAX_NODES} nodes."),
            });
        }
        if depth > MAX_DEPTH {
            return Err(EvalError {
                message: format!("Condition exceeds the limit of {MAX_DEPTH} nested levels."),
            });
        }
        match node {
            Node::Condition(condition) if condition.op == Op::Regex => {
                // Compare before adding so even a caller-built vector cannot
                // overflow the aggregate counter.
                if condition.values.len() > MAX_PATTERNS - patterns {
                    return Err(pattern_limit());
                }
                patterns += condition.values.len();
            }
            Node::Format(_) => {
                patterns += 1;
                if patterns > MAX_PATTERNS {
                    return Err(pattern_limit());
                }
            }
            Node::Group(group) => {
                // Bound the traversal stack too. This rejects a wide group
                // before allocating a second vector of all its children.
                if group.children.len() > MAX_NODES - nodes - pending.len() {
                    return Err(EvalError {
                        message: format!("Condition exceeds the limit of {MAX_NODES} nodes."),
                    });
                }
                pending.extend(group.children.iter().rev().map(|child| (child, depth + 1)));
            }
            Node::Condition(_) => {}
        }
    }
    Ok(())
}

fn pattern_limit() -> EvalError {
    EvalError {
        message: format!("Condition exceeds the limit of {MAX_PATTERNS} compiled patterns."),
    }
}

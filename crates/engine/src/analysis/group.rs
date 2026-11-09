use std::collections::HashMap;

use serde_json::Value;

/// One row's grouping path: the label text and representative value per level.
#[derive(Debug, Default)]
pub struct GroupTreeRow {
    pub labels: Vec<String>,
    pub values: Vec<Value>,
}

#[derive(Debug, Default)]
pub struct GroupNodePartial {
    pub count: usize,
    /// Representative value per level, recorded on the first row that reaches it.
    pub values: Vec<Value>,
    pub children: HashMap<String, GroupNodePartial>,
}

/// A normalized node: children sorted by label, value taken from the first row.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupNode {
    pub value: Value,
    pub label: String,
    pub count: usize,
    pub children: Vec<GroupNode>,
}

/// Counts one row along its label path.
pub fn add_row(root: &mut GroupNodePartial, row: &GroupTreeRow) {
    if row.labels.len() != row.values.len() {
        return;
    }
    let mut node = root;
    for (level, label) in row.labels.iter().enumerate() {
        let child = node.children.entry(label.clone()).or_default();
        child.count += 1;
        if child.values.len() == level {
            child.values.push(row.values[level].clone());
        }
        node = child;
    }
}

/// Sorts every level by label and returns the level-1 nodes.
pub fn finalize(root: &GroupNodePartial) -> Vec<GroupNode> {
    finalize_children(root, 0)
}

fn finalize_children(parent: &GroupNodePartial, level: usize) -> Vec<GroupNode> {
    let mut labels: Vec<&String> = parent.children.keys().collect();
    labels.sort();
    labels
        .into_iter()
        .map(|label| {
            let child = &parent.children[label];
            GroupNode {
                value: child.values.get(level).cloned().unwrap_or(Value::Null),
                label: label.clone(),
                count: child.count,
                children: finalize_children(child, level + 1),
            }
        })
        .collect()
}

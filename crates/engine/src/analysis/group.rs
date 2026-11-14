use std::collections::HashMap;

use serde_json::Value;

use super::group_time::GroupTimeRange;

pub const MAX_GROUP_LEVELS: usize = 8;
pub const MAX_GROUP_NODES: usize = 10_000;
const MAX_LABEL_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Default)]
struct PartialNode {
    count: usize,
    value: Value,
    range: Option<GroupTimeRange>,
    children: HashMap<(String, bool), PartialNode>,
}

/// A normalized node. Text is exact, including whitespace; null is not empty.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupNode {
    pub value: Value,
    pub label: String,
    pub count: usize,
    pub children: Vec<GroupNode>,
    pub range: Option<GroupTimeRange>,
}

/// Bounded aggregation. Exceeding a limit rejects the entire result rather
/// than returning plausible-looking incomplete counts.
#[derive(Debug, Default)]
pub struct GroupTreeBuilder {
    root: PartialNode,
    nodes: usize,
    label_bytes: usize,
}

impl GroupTreeBuilder {
    pub fn add_row(&mut self, labels: &[String], values: &[Value]) -> Result<(), &'static str> {
        self.add_with_limits(labels, values, MAX_GROUP_NODES, MAX_LABEL_BYTES)
    }

    pub fn add_row_with_ranges(
        &mut self,
        labels: &[String],
        values: &[Value],
        ranges: &[Option<GroupTimeRange>],
    ) -> Result<(), &'static str> {
        if ranges.len() != labels.len() {
            return Err("Each group level must have a matching time range.");
        }
        self.add_with_range_limits(labels, values, ranges, MAX_GROUP_NODES, MAX_LABEL_BYTES)
    }

    fn add_with_limits(
        &mut self,
        labels: &[String],
        values: &[Value],
        max_nodes: usize,
        max_bytes: usize,
    ) -> Result<(), &'static str> {
        self.add_with_range_limits(labels, values, &[], max_nodes, max_bytes)
    }

    fn add_with_range_limits(
        &mut self,
        labels: &[String],
        values: &[Value],
        ranges: &[Option<GroupTimeRange>],
        max_nodes: usize,
        max_bytes: usize,
    ) -> Result<(), &'static str> {
        if labels.len() != values.len() || labels.len() > MAX_GROUP_LEVELS {
            return Err("Grouping supports at most 8 matching column levels.");
        }
        let mut node = &mut self.root;
        for (index, (label, value)) in labels.iter().zip(values).enumerate() {
            let key = (label.clone(), value.is_null() && label.is_empty());
            if !node.children.contains_key(&key) {
                if self.nodes >= max_nodes
                    || label.len() > max_bytes.saturating_sub(self.label_bytes)
                {
                    return Err("Too many group values (limit: 10,000 groups / 32 MiB labels). Add a filter or remove a grouping level.");
                }
                self.nodes += 1;
                self.label_bytes += label.len();
            }
            let child = node.children.entry(key).or_insert_with(|| PartialNode {
                value: value.clone(),
                range: ranges.get(index).cloned().flatten(),
                ..PartialNode::default()
            });
            child.count += 1;
            node = child;
        }
        Ok(())
    }

    pub fn finish(self) -> Vec<GroupNode> {
        finalize(self.root.children)
    }
}

fn finalize(children: HashMap<(String, bool), PartialNode>) -> Vec<GroupNode> {
    let mut children: Vec<_> = children.into_iter().collect();
    children.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    children
        .into_iter()
        .map(|((label, _), child)| GroupNode {
            value: child.value,
            label,
            count: child.count,
            children: finalize(child.children),
            range: child.range,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exact_group_identity_and_nested_values_survive() {
        let mut tree = GroupTreeBuilder::default();
        for value in [Value::Null, json!(""), json!(" "), json!("a"), json!(" a ")] {
            let label = value.as_str().unwrap_or_default().to_string();
            tree.add_row(&["parent".into(), label], &[json!("parent"), value])
                .unwrap();
        }
        let nodes = tree.finish();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].count, 5);
        let children = &nodes[0].children;
        assert_eq!(children.len(), 5);
        assert_eq!(children.iter().map(|child| child.count).sum::<usize>(), 5);
        assert!(children
            .iter()
            .any(|child| child.value == Value::Null && child.label.is_empty()));
        for value in ["", " ", "a", " a "] {
            assert!(children
                .iter()
                .any(|child| child.value == json!(value) && child.label == value));
        }
    }

    #[test]
    fn repeated_groups_fit_but_new_nodes_and_large_labels_are_rejected() {
        let mut tree = GroupTreeBuilder::default();
        for _ in 0..3 {
            tree.add_with_limits(&["a".into()], &[json!("a")], 1, 1)
                .unwrap();
        }
        assert_eq!(tree.finish()[0].count, 3);
        let mut tree = GroupTreeBuilder::default();
        tree.add_with_limits(&["a".into()], &[json!("a")], 1, 100)
            .unwrap();
        assert!(tree
            .add_with_limits(&["b".into()], &[json!("b")], 1, 100)
            .is_err());
        assert!(GroupTreeBuilder::default()
            .add_with_limits(&["long".into()], &[json!("long")], 10, 3)
            .is_err());
        assert!(GroupTreeBuilder::default()
            .add_row(&vec!["a".into(); 9], &vec![json!("a"); 9])
            .is_err());
    }
}

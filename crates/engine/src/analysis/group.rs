use std::collections::{hash_map::Entry, HashMap};

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
            let child = match node.children.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    if self.nodes >= max_nodes
                        || label.len() > max_bytes.saturating_sub(self.label_bytes)
                    {
                        return Err("Too many group values (limit: 10,000 groups / 32 MiB labels). Add a filter or remove a grouping level.");
                    }
                    self.nodes += 1;
                    self.label_bytes += label.len();
                    entry.insert(PartialNode {
                        value: value.clone(),
                        range: ranges.get(index).cloned().flatten(),
                        ..PartialNode::default()
                    })
                }
            };
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

    #[test]
    fn occupied_nodes_retain_the_first_value_and_range_at_exact_limits() {
        let mut tree = GroupTreeBuilder::default();
        let first_range = GroupTimeRange {
            start: "2026-11-14T00:00:00Z".into(),
            end: "2026-11-15T00:00:00Z".into(),
        };
        let later_range = GroupTimeRange {
            start: "2026-11-15T00:00:00Z".into(),
            end: "2026-11-16T00:00:00Z".into(),
        };
        for (value, range) in [
            (json!("first"), first_range.clone()),
            (json!("later"), later_range),
        ] {
            tree.add_with_range_limits(&["slot".into()], &[value], &[Some(range)], 1, 4)
                .unwrap();
        }
        assert_eq!((tree.nodes, tree.label_bytes), (1, 4));
        let nodes = tree.finish();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].count, 2);
        assert_eq!(nodes[0].value, json!("first"));
        assert_eq!(nodes[0].range, Some(first_range));
    }

    #[test]
    fn null_and_empty_nodes_remain_distinct_when_zero_label_bytes_are_available() {
        let mut tree = GroupTreeBuilder::default();
        for _ in 0..2 {
            for value in [Value::Null, json!("")] {
                tree.add_with_limits(&[String::new()], &[value], 2, 0)
                    .unwrap();
            }
        }
        assert_eq!((tree.nodes, tree.label_bytes), (2, 0));
        let nodes = tree.finish();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().all(|node| node.label.is_empty() && node.count == 2));
        assert!(nodes.iter().any(|node| node.value == Value::Null));
        assert!(nodes.iter().any(|node| node.value == json!("")));
    }

    #[test]
    fn rejected_vacant_nodes_leave_existing_counts_and_budget_totals_intact() {
        for (max_nodes, max_bytes) in [(1, 8), (8, 1)] {
            let mut tree = GroupTreeBuilder::default();
            tree.add_with_limits(&["a".into()], &[json!("a")], max_nodes, max_bytes)
                .unwrap();
            assert!(tree
                .add_with_limits(&["b".into()], &[json!("b")], max_nodes, max_bytes)
                .is_err());
            assert_eq!((tree.nodes, tree.label_bytes), (1, 1));
            let nodes = tree.finish();
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].label, "a");
            assert_eq!(nodes[0].value, json!("a"));
            assert_eq!(nodes[0].count, 1);
        }
    }
}

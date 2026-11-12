use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum Op {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    Contains,
    StartsWith,
    EndsWith,
    Regex,
    In,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum GroupKind {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Condition {
    pub column: String,
    pub op: Op,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub negated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Group {
    pub kind: GroupKind,
    #[serde(default)]
    pub children: Vec<Node>,
}

/// A condition tree. JSON is untagged: a condition object has `column`,
/// a group object has `kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[serde(untagged)]
pub enum Node {
    Condition(Condition),
    Group(Group),
}

impl Node {
    /// Visits every column referenced by this tree in traversal order.
    pub fn visit_columns<'a>(&'a self, visit: &mut impl FnMut(&'a str)) {
        match self {
            Self::Condition(condition) => visit(condition.column.as_str()),
            Self::Group(group) => {
                for child in &group.children {
                    child.visit_columns(visit);
                }
            }
        }
    }

    /// Column names referenced by this tree, in traversal order.
    pub fn columns(&self) -> Vec<&str> {
        let mut columns = Vec::new();
        self.visit_columns(&mut |column| columns.push(column));
        columns
    }
}

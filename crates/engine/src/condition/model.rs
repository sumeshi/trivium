use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GroupKind {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Condition {
    pub column: String,
    pub op: Op,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub negated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Group {
    pub kind: GroupKind,
    #[serde(default)]
    pub children: Vec<Node>,
}

/// A condition tree. JSON is untagged: a condition object has `column`,
/// a group object has `kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Node {
    Condition(Condition),
    Group(Group),
}

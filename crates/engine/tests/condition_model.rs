use trivium_engine::condition::{Condition, Group, GroupKind, Node, Op};

fn cond(column: &str, op: Op, values: &[&str], negated: bool) -> Node {
    Node::Condition(Condition {
        column: column.to_string(),
        op,
        values: values.iter().map(|v| (*v).to_string()).collect(),
        negated,
    })
}

fn group(kind: GroupKind, children: Vec<Node>) -> Node {
    Node::Group(Group { kind, children })
}

fn assert_roundtrip(node: &Node) {
    let json = serde_json::to_string(node).expect("serialize");
    let back: Node = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(&back, node, "roundtrip mismatch for {json}");
}

#[test]
fn every_op_roundtrips() {
    let ops = [
        Op::Eq,
        Op::Ne,
        Op::Gt,
        Op::Lt,
        Op::Ge,
        Op::Le,
        Op::Contains,
        Op::StartsWith,
        Op::EndsWith,
        Op::Regex,
        Op::In,
    ];
    assert!(ops.len() >= 11);
    for op in ops {
        assert_roundtrip(&cond("EventID", op, &["4624"], false));
        assert_roundtrip(&cond("msg", op, &["a", "b"], true));
    }
}

#[test]
fn nested_groups_and_edge_values_roundtrip() {
    let cases = vec![
        cond("host", Op::Eq, &["WS01"], false),
        cond("user", Op::Ne, &[], false),
        cond("", Op::Contains, &[""], false),
        cond("path", Op::StartsWith, &["C:\\Temp"], false),
        cond("msg", Op::Contains, &["quote\"comma,", "線"], true),
        cond("n", Op::Gt, &["1700000000123456789"], false),
        cond("re", Op::Regex, &["a.*b"], false),
        cond("host", Op::In, &["WS01", "WS02", "WS03"], false),
        group(GroupKind::And, vec![]),
        group(GroupKind::Or, vec![]),
        group(
            GroupKind::And,
            vec![
                cond("user", Op::Eq, &["alice"], false),
                cond("EventID", Op::Eq, &["4624"], false),
            ],
        ),
        group(
            GroupKind::Or,
            vec![
                cond("host", Op::Contains, &["WS"], false),
                group(
                    GroupKind::And,
                    vec![
                        cond("user", Op::Eq, &["bob"], true),
                        cond("proc", Op::EndsWith, &[".exe"], false),
                    ],
                ),
            ],
        ),
        group(
            GroupKind::And,
            vec![group(
                GroupKind::Or,
                vec![group(
                    GroupKind::And,
                    vec![cond("msg", Op::Contains, &["beacon"], false)],
                )],
            )],
        ),
    ];
    assert!(cases.len() >= 8);
    for node in &cases {
        assert_roundtrip(node);
    }
}

#[test]
fn omitted_negated_and_values_default_to_empty() {
    let parsed: Node = serde_json::from_str(r#"{"column":"host","op":"eq"}"#).unwrap();
    assert_eq!(parsed, cond("host", Op::Eq, &[], false));
}

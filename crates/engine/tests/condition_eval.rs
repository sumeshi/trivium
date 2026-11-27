use std::collections::HashMap;

use serde_json::{Number, Value};

use trivium_engine::condition::{eval, search_to_node, Condition, Group, GroupKind, Node, Op};

fn condition(column: &str, op: Op, values: &[&str], negated: bool) -> Node {
    Node::Condition(Condition {
        column: column.to_string(),
        op,
        values: values.iter().map(|value| (*value).to_string()).collect(),
        negated,
    })
}

fn and(children: Vec<Node>) -> Node {
    Node::Group(Group {
        kind: GroupKind::And,
        children,
    })
}

fn or(children: Vec<Node>) -> Node {
    Node::Group(Group {
        kind: GroupKind::Or,
        children,
    })
}

fn contains(column: &str, text: &str) -> Node {
    condition(column, Op::Contains, &[text], false)
}

fn row(cells: &[(&str, &str)]) -> HashMap<String, Value> {
    cells
        .iter()
        .map(|(column, value)| ((*column).to_string(), Value::String((*value).to_string())))
        .collect()
}

#[test]
fn contains_and_or_not_match_expected_rows() {
    let rows = [
        row(&[
            ("eventid", "4624"),
            ("user", "Administrator"),
            ("command", "powershell -enc AAA"),
            ("msg", "malware beacon"),
            ("host", "WS01"),
        ]),
        row(&[
            ("eventid", "4625"),
            ("user", "guest"),
            ("command", "cmd.exe"),
            ("msg", "test beacon"),
            ("host", "WS02"),
        ]),
        row(&[
            ("eventid", "4624"),
            ("user", "administrator"),
            ("command", "cmd.exe"),
            ("msg", "error while loading"),
            ("host", "WS09"),
        ]),
        row(&[
            ("eventid", "1"),
            ("user", "alice"),
            ("command", "cmd.exe"),
            ("msg", "warn"),
            ("host", "DB01"),
        ]),
    ];

    let cases = [
        (
            "malware beacon",
            and(vec![contains("", "malware"), contains("", "beacon")]),
            vec![true, false, false, false],
        ),
        (
            "error|warn",
            or(vec![contains("", "error"), contains("", "warn")]),
            vec![false, false, true, true],
        ),
        (
            "beacon -test",
            and(vec![
                contains("", "beacon"),
                condition("", Op::Contains, &["test"], true),
            ]),
            vec![true, false, false, false],
        ),
        (
            "eventid:4624 user:administrator",
            and(vec![
                contains("eventid", "4624"),
                contains("user", "administrator"),
            ]),
            vec![true, false, true, false],
        ),
        (
            "command:\"powershell -enc\"",
            contains("command", "powershell -enc"),
            vec![true, false, false, false],
        ),
        (
            "host:WS01|WS02",
            or(vec![contains("host", "ws01"), contains("host", "ws02")]),
            vec![true, true, false, false],
        ),
    ];

    for (query, node, expected) in cases {
        let evaluated: Vec<bool> = rows
            .iter()
            .map(|cells| eval(&node, cells).expect("eval"))
            .collect();
        assert_eq!(evaluated, expected, "condition failed for {query}");
        let converted = search_to_node(query)
            .unwrap()
            .unwrap_or_else(|| panic!("no node for {query}"));
        let from_syntax: Vec<bool> = rows
            .iter()
            .map(|cells| eval(&converted, cells).expect("eval"))
            .collect();
        assert_eq!(from_syntax, expected, "search syntax failed for {query}");
    }
}

#[test]
fn unscoped_contains_reads_source_cells_and_never_the_joined_entry() {
    let cells = HashMap::from([
        ("left".into(), Value::String("Alpha".into())),
        ("right".into(), Value::String("Beta".into())),
        (String::new(), Value::String("Alpha Beta".into())),
    ]);
    assert!(eval(&contains("", "alpha"), &cells).unwrap());
    assert!(!eval(&contains("", "alpha beta"), &cells).unwrap());
    assert!(eval(&condition("", Op::Eq, &["Alpha Beta"], false), &cells).unwrap());
    assert!(eval(&condition("", Op::Regex, &["^Alpha Beta$"], false), &cells).unwrap());

    let no_source_cells = HashMap::from([(String::new(), Value::String("Alpha Beta".into()))]);
    assert!(!eval(&contains("", "alpha"), &no_source_cells).unwrap());
    assert!(eval(
        &condition("", Op::Contains, &["alpha"], true),
        &no_source_cells
    )
    .unwrap());
}

#[test]
fn numeric_compare_keeps_integer_and_decimal_precision() {
    // Integers compare exactly through i128; adjacent long integers never
    // collapse into the same value.
    let exact = "1700000000123456789";
    let mut cells = HashMap::new();
    cells.insert(
        "n".to_string(),
        Value::Number(Number::from(1_700_000_000_123_456_789_i64)),
    );
    assert!(eval(&condition("n", Op::Eq, &[exact], false), &cells).expect("eval"));
    assert!(eval(
        &condition("n", Op::Gt, &["1700000000123456788"], false),
        &cells
    )
    .expect("eval"));
    assert!(!eval(&condition("n", Op::Gt, &[exact], false), &cells).expect("eval"));
    assert!(eval(&condition("n", Op::Ge, &[exact], false), &cells).expect("eval"));

    let mut text_cells = HashMap::new();
    text_cells.insert("n".to_string(), Value::String(exact.to_string()));
    assert!(eval(
        &condition("n", Op::Gt, &["1700000000123456788"], false),
        &text_cells
    )
    .expect("eval"));

    // u64 values above i64::MAX stay distinct.
    let mut u64_cells = HashMap::new();
    u64_cells.insert(
        "n".to_string(),
        Value::String("18446744073709551615".to_string()),
    );
    assert!(eval(
        &condition("n", Op::Gt, &["18446744073709551614"], false),
        &u64_cells
    )
    .expect("eval"));
    assert!(!eval(
        &condition("n", Op::Gt, &["18446744073709551615"], false),
        &u64_cells
    )
    .expect("eval"));

    // String equality keeps leading zeros meaningful.
    let mut id = HashMap::new();
    id.insert("id".to_string(), Value::String("001".to_string()));
    assert!(!eval(&condition("id", Op::Eq, &["1"], false), &id).expect("eval"));

    // Decimals and integers beyond i128 compare their exact digits too.
    let mut fraction = HashMap::new();
    fraction.insert("n".to_string(), Value::String("1.5".to_string()));
    assert!(eval(&condition("n", Op::Gt, &["1.25"], false), &fraction).unwrap());
    assert!(eval(&condition("n", Op::Lt, &["2"], false), &fraction).unwrap());

    let mut huge = HashMap::new();
    huge.insert(
        "n".to_string(),
        Value::String("170141183460469231731687303715884105728".to_string()),
    );
    assert!(eval(&condition("n", Op::Gt, &["0"], false), &huge).unwrap());
    assert!(!eval(
        &condition(
            "n",
            Op::Gt,
            &["170141183460469231731687303715884105728"],
            false
        ),
        &huge
    )
    .unwrap());

    // Unsupported source values do not match; malformed user bounds are errors.
    for invalid in ["", " ", "NaN", "Infinity", "not numeric", "1.2.3", "1e"] {
        let invalid_cell = row(&[("n", invalid)]);
        assert!(!eval(&condition("n", Op::Gt, &["0"], false), &invalid_cell).unwrap());
        assert!(eval(&condition("n", Op::Lt, &[invalid], false), &fraction).is_err());
    }
}

#[test]
fn regex_starts_with_and_group_edges() {
    let cells = row(&[("msg", "Alpha-Beacon")]);
    assert!(eval(&condition("msg", Op::StartsWith, &["alpha"], false), &cells).expect("eval"));
    assert!(eval(&condition("msg", Op::EndsWith, &["beacon"], false), &cells).expect("eval"));
    assert!(eval(
        &condition("msg", Op::Regex, &["(?i)alpha-.*"], false),
        &cells
    )
    .expect("eval"));
    assert!(eval(&condition("msg", Op::Regex, &["["], false), &cells).is_err());
    assert!(eval(
        &Node::Group(Group {
            kind: GroupKind::And,
            children: vec![],
        }),
        &cells
    )
    .expect("eval"));
    assert!(!eval(
        &Node::Group(Group {
            kind: GroupKind::Or,
            children: vec![],
        }),
        &cells
    )
    .expect("eval"));
}

#[test]
fn datetime_ordering_compares_parsed_timestamps() {
    let cells = row(&[("ts", "2024-01-01T00:00:02")]);
    assert!(eval(
        &condition("ts", Op::Ge, &["2024-01-01T00:00:01"], false),
        &cells
    )
    .expect("eval"));
    assert!(eval(
        &condition("ts", Op::Le, &["2024-01-01T00:00:02"], false),
        &cells
    )
    .expect("eval"));
    assert!(!eval(
        &condition("ts", Op::Gt, &["2024-01-01T00:00:02"], false),
        &cells
    )
    .expect("eval"));
    assert!(eval(&condition("ts", Op::Lt, &["2024-01-02"], false), &cells).expect("eval"));
}

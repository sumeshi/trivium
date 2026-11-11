use std::collections::HashMap;

use serde_json::{Number, Value};

use trivium_engine::condition::{eval, search_to_node, Condition, Group, GroupKind, Node, Op};
use trivium_engine::search::{
    build_search_mask_boolean, to_rpn, tokenize_search_query, SearchToken,
};

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
    let mut out = HashMap::new();
    let mut joined = String::new();
    for (column, value) in cells {
        if !value.is_empty() {
            if !joined.is_empty() {
                joined.push(' ');
            }
            joined.push_str(&value.to_lowercase());
        }
        out.insert((*column).to_string(), Value::String((*value).to_string()));
    }
    out.insert(String::new(), Value::String(joined));
    out
}

fn search_mask(rows: &[HashMap<String, Value>], query: &str) -> Vec<bool> {
    let tokens = tokenize_search_query(query);
    let mut terms: Vec<(Option<String>, String)> = Vec::new();
    for token in &tokens {
        if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = token {
            if !text.is_empty() {
                let key = (col.clone(), text.clone());
                if !terms.contains(&key) {
                    terms.push(key);
                }
            }
        }
    }
    let joined: Vec<String> = rows
        .iter()
        .map(|cells| {
            cells
                .get("")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    let mut per_column: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(first) = rows.first() {
        for column in first.keys() {
            if column.is_empty() {
                continue;
            }
            per_column.insert(
                column.to_lowercase(),
                rows.iter()
                    .map(|cells| {
                        cells
                            .get(column)
                            .and_then(|value| value.as_str())
                            .unwrap_or("")
                            .to_lowercase()
                    })
                    .collect(),
            );
        }
    }
    let rpn = to_rpn(&tokens);
    build_search_mask_boolean(&rpn, &terms, &joined, Some(&per_column))
}

#[test]
fn contains_and_or_not_match_search_mask() {
    let rows = vec![
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
        ),
        (
            "error|warn",
            or(vec![contains("", "error"), contains("", "warn")]),
        ),
        (
            "beacon -test",
            and(vec![
                contains("", "beacon"),
                condition("", Op::Contains, &["test"], true),
            ]),
        ),
        (
            "eventid:4624 user:administrator",
            and(vec![
                contains("eventid", "4624"),
                contains("user", "administrator"),
            ]),
        ),
        (
            "command:\"powershell -enc\"",
            contains("command", "powershell -enc"),
        ),
        (
            "host:WS01|WS02",
            or(vec![contains("host", "ws01"), contains("host", "ws02")]),
        ),
    ];

    for (query, node) in cases {
        let mask = search_mask(&rows, query);
        let evaluated: Vec<bool> = rows
            .iter()
            .map(|cells| eval(&node, cells).expect("eval"))
            .collect();
        assert_eq!(evaluated, mask, "parity failed for {query}");
        let converted = search_to_node(query).unwrap_or_else(|| panic!("no node for {query}"));
        let from_syntax: Vec<bool> = rows
            .iter()
            .map(|cells| eval(&converted, cells).expect("eval"))
            .collect();
        assert_eq!(from_syntax, mask, "search syntax diverged for {query}");
    }
}

#[test]
fn numeric_compare_uses_i128_and_rejects_decimals() {
    // Integers compare exactly through i128; adjacent long integers never
    // collapse into the same value.
    let exact = "1700000000123456789";
    let mut cells = HashMap::new();
    cells.insert(
        "n".to_string(),
        Value::Number(Number::from(1_700_000_000_123_456_789_i64)),
    );
    assert!(eval(&condition("n", Op::Eq, &[exact], false), &cells).expect("eval"));
    assert!(eval(&condition("n", Op::Gt, &["1700000000123456788"], false), &cells).expect("eval"));
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

    // Decimal spellings and values outside i128 are explicit errors, never a
    // silent f64 comparison.
    let mut fraction = HashMap::new();
    fraction.insert("n".to_string(), Value::String("1.5".to_string()));
    assert!(eval(&condition("n", Op::Gt, &["1.25"], false), &fraction).is_err());
    assert!(eval(&condition("n", Op::Lt, &["2"], false), &fraction).is_err());

    let mut huge = HashMap::new();
    huge.insert(
        "n".to_string(),
        Value::String("170141183460469231731687303715884105728".to_string()),
    );
    assert!(eval(&condition("n", Op::Gt, &["0"], false), &huge).is_err());
}

#[test]
fn regex_starts_with_and_group_edges() {
    let cells = row(&[("msg", "Alpha-Beacon")]);
    assert!(eval(&condition("msg", Op::StartsWith, &["alpha"], false), &cells).expect("eval"));
    assert!(eval(&condition("msg", Op::EndsWith, &["beacon"], false), &cells).expect("eval"));
    assert!(eval(&condition("msg", Op::Regex, &["(?i)alpha-.*"], false), &cells).expect("eval"));
    assert!(!eval(&condition("msg", Op::Regex, &["["], false), &cells).expect("eval"));
    assert!(eval(
        &Node::Group(Group {
            kind: GroupKind::And,
            children: vec![],
        }),
        &cells
    ).expect("eval"));
    assert!(!eval(
        &Node::Group(Group {
            kind: GroupKind::Or,
            children: vec![],
        }),
        &cells
    ).expect("eval"));
}

#[test]
fn datetime_ordering_compares_parsed_timestamps() {
    let cells = row(&[("ts", "2024-01-01T00:00:02")]);
    assert!(eval(&condition("ts", Op::Ge, &["2024-01-01T00:00:01"], false), &cells).expect("eval"));
    assert!(eval(&condition("ts", Op::Le, &["2024-01-01T00:00:02"], false), &cells).expect("eval"));
    assert!(!eval(&condition("ts", Op::Gt, &["2024-01-01T00:00:02"], false), &cells).expect("eval"));
    assert!(eval(&condition("ts", Op::Lt, &["2024-01-02"], false), &cells).expect("eval"));
}

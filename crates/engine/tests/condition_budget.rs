use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::json;
use trivium_engine::condition::eval::{regex_compile_count, reset_regex_compile_count};
use trivium_engine::condition::{
    search_to_node, search_tokens_to_node, Condition, Group, GroupKind, Node, Op, PreparedNode,
};
use trivium_engine::ops::value_validation::{
    validate_condition_values, ValidateConditionValuesPayload,
};
use trivium_engine::search::{tokenize_search_query, validate_search_query, SearchToken};

// Every test in this integration binary shares the process-wide compile hook.
static HOOK_LOCK: Mutex<()> = Mutex::new(());

fn regex(values: Vec<String>) -> Node {
    Node::Condition(Condition {
        column: "value".into(),
        op: Op::Regex,
        values,
        negated: false,
    })
}

#[test]
fn tree_budget_rejects_many_compact_patterns_before_any_compile() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    reset_regex_compile_count();
    let node = regex(vec![r"\w{100}".into(); 33]);
    let error = PreparedNode::prepare(&node).unwrap_err().to_string();
    assert!(error.contains("32 compiled patterns"), "{error}");
    assert_eq!(regex_compile_count(), 0);

    // The budget applies across sibling conditions and includes formats.
    let format: Node = serde_json::from_value(json!({
        "columns": ["value"],
        "format": { "schemaVersion": 1, "id": "budget", "name": "Budget", "pattern": "[" }
    }))
    .unwrap();
    let tree = Node::Group(Group {
        kind: GroupKind::And,
        children: vec![regex(vec!["[".into(); 32]), format],
    });
    let error = PreparedNode::prepare(&tree).unwrap_err().to_string();
    assert!(error.contains("32 compiled patterns"), "{error}");
    assert_eq!(regex_compile_count(), 0);
}

#[test]
fn wide_and_deep_trees_fail_before_recursive_preparation() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let leaf = Node::Condition(Condition {
        column: "value".into(),
        op: Op::Eq,
        values: vec!["keep".into()],
        negated: false,
    });
    let wide = Node::Group(Group {
        kind: GroupKind::Or,
        children: vec![leaf.clone(); 1024],
    });
    let error = PreparedNode::prepare(&wide).unwrap_err().to_string();
    assert!(error.contains("1024 nodes"), "{error}");

    let mut deep = leaf;
    for _ in 0..64 {
        deep = Node::Group(Group {
            kind: GroupKind::And,
            children: vec![deep],
        });
    }
    let error = PreparedNode::prepare(&deep).unwrap_err().to_string();
    assert!(error.contains("64 nested levels"), "{error}");
}

#[test]
fn expanded_regex_limits_match_the_advisory_checker() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    // A short Unicode repetition expands beyond the bounded compiled size.
    let pattern = r"\w{100}".to_string();
    let error = PreparedNode::prepare(&regex(vec![pattern.clone()]))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Compiled regex exceeds size limit of 2097152 bytes"),
        "{error}"
    );
    let checked = validate_condition_values(ValidateConditionValuesPayload {
        kind: "string".into(),
        op: Op::Regex,
        values: vec![pattern],
    })
    .unwrap();
    assert!(!checked.valid);
    assert_eq!(checked.values[0].error.as_deref(), Some(error.as_str()));
}

#[test]
fn normal_complex_rules_keep_their_membership_and_source_patterns() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let patterns: Vec<String> = (0..32).map(|index| format!("^keep-{index}$")).collect();
    let node = Node::Group(Group {
        kind: GroupKind::And,
        children: vec![
            regex(patterns.clone()),
            Node::Condition(Condition {
                column: "cohort".into(),
                op: Op::Eq,
                values: vec!["keep".into()],
                negated: false,
            }),
        ],
    });
    let original = serde_json::to_value(&node).unwrap();
    let prepared = PreparedNode::prepare(&node).unwrap();
    let mut row = HashMap::from([
        ("value".into(), json!("keep-31")),
        ("cohort".into(), json!("keep")),
    ]);
    assert!(prepared.eval(&row).unwrap());
    row.insert("cohort".into(), json!("drop"));
    assert!(!prepared.eval(&row).unwrap());
    assert_eq!(serde_json::to_value(&node).unwrap(), original);
}

#[test]
fn search_limits_apply_before_ast_construction_and_keep_boundary_queries() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let too_many = vec!["keep"; 65].join(" ");
    let error = validate_search_query(&too_many).unwrap_err();
    assert!(error.contains("64 terms"), "{error}");
    assert_eq!(search_to_node(&too_many).unwrap_err(), error);
    assert_eq!(
        search_tokens_to_node(&tokenize_search_query(&too_many)).unwrap_err(),
        error
    );
    let spec = trivium_engine::ops::rows::QuerySpec {
        search: Some(too_many.clone()),
        ..Default::default()
    };
    assert_eq!(spec.validate_filter().unwrap_err().to_string(), error);
    let ioc = trivium_engine::models::IocEntry {
        flag: "safe".into(),
        tag: "review".into(),
        query: too_many,
    };
    assert!(trivium_engine::ioc::prepare_ioc_entries(vec![ioc])
        .unwrap_err()
        .to_string()
        .contains(&error));

    let too_long = "x".repeat(64 * 1024 + 1);
    let error = validate_search_query(&too_long).unwrap_err();
    assert!(error.contains("64 KiB"), "{error}");
    assert_eq!(search_to_node(&too_long).unwrap_err(), error);
    let mut tokens = vec![SearchToken::Not; 193];
    tokens.push(SearchToken::Term {
        col: None,
        text: "keep".into(),
    });
    assert!(search_tokens_to_node(&tokens)
        .unwrap_err()
        .contains("192 tokens"));
    assert!(search_to_node("|").is_err());
    assert!(search_to_node("\"\"").is_err());
    assert!(search_to_node("@tag:%GG").is_err());
    assert!(search_tokens_to_node(&tokenize_search_query("@tag:%GG")).is_err());
    assert!(search_to_node(" \t ").unwrap().is_none());

    for query in [vec!["keep"; 64].join(" "), "x".repeat(64 * 1024)] {
        validate_search_query(&query).unwrap();
        let node = search_to_node(&query).unwrap().expect("bounded condition");
        let prepared = PreparedNode::prepare(&node).unwrap();
        let row = HashMap::from([("".into(), json!(query))]);
        assert!(prepared.eval(&row).unwrap());
    }
}

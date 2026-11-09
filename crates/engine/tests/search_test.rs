use std::collections::HashMap;

use trivium_engine::search::{
    build_search_mask_boolean, to_rpn, tokenize_search_query, SearchToken,
};

fn term(text: &str) -> SearchToken {
    SearchToken::Term {
        col: None,
        text: text.to_string(),
    }
}

fn term_col(col: &str, text: &str) -> SearchToken {
    SearchToken::Term {
        col: Some(col.to_string()),
        text: text.to_string(),
    }
}

fn quoted(text: &str) -> SearchToken {
    SearchToken::QuotedTerm {
        col: None,
        text: text.to_string(),
    }
}

fn quoted_col(col: &str, text: &str) -> SearchToken {
    SearchToken::QuotedTerm {
        col: Some(col.to_string()),
        text: text.to_string(),
    }
}

#[test]
fn tokenize_readme_examples_and_implementation_behaviour() {
    let cases: Vec<(&str, Vec<SearchToken>)> = vec![
        // README examples
        (
            "malware beacon",
            vec![term("malware"), SearchToken::And, term("beacon")],
        ),
        (
            "error|warn",
            vec![term("error"), SearchToken::Or, term("warn")],
        ),
        (
            "beacon -test",
            vec![term("beacon"), SearchToken::And, SearchToken::Not, term("test")],
        ),
        (
            "eventid:4624 user:administrator",
            vec![
                term_col("eventid", "4624"),
                SearchToken::And,
                term_col("user", "administrator"),
            ],
        ),
        (
            "command:\"powershell -enc\"",
            vec![quoted_col("command", "powershell -enc")],
        ),
        ("\"-keyword\"", vec![quoted("-keyword")]),
        (
            "com:WS01|WS02",
            vec![term_col("com", "ws01"), SearchToken::Or, term_col("com", "ws02")],
        ),
        // README special-character examples
        ("\"hoge:fuga\"", vec![quoted("hoge:fuga")]),
        (
            "data:\"hoge:fuga\"",
            vec![quoted_col("data", "hoge:fuga")],
        ),
        // Consecutive pipes are collapsed by the tokenizer
        (
            "a|||b",
            vec![term("a"), SearchToken::Or, term("b")],
        ),
        // col:"phrase with spaces" is merged into one quoted term
        (
            "data:\"two words\"",
            vec![quoted_col("data", "two words")],
        ),
        // Inside quotes, '|' and '-' are literal characters
        ("\"a|b -c\"", vec![quoted("a|b -c")]),
        // Terms (including column names) are lowercased
        (
            "MALWARE EventID:4624",
            vec![
                term("malware"),
                SearchToken::And,
                term_col("eventid", "4624"),
            ],
        ),
        // Leading '-' works with a column scope as well
        (
            "-EventID:4624",
            vec![SearchToken::Not, term_col("eventid", "4624")],
        ),
        // Implicit AND is inserted between an operand and a quoted term
        (
            "foo \"bar baz\"",
            vec![term("foo"), SearchToken::And, quoted("bar baz")],
        ),
        // Empty and whitespace-only inputs produce no tokens
        ("", vec![]),
        ("   ", vec![]),
    ];

    for (input, expected) in cases {
        assert_eq!(
            tokenize_search_query(input),
            expected,
            "input: {input:?}"
        );
    }
}

#[test]
fn rpn_operator_precedence_is_not_then_and_then_or() {
    let cases: Vec<(&str, Vec<SearchToken>)> = vec![
        // AND has higher precedence than OR
        (
            "a b|c",
            vec![term("a"), term("b"), SearchToken::And, term("c"), SearchToken::Or],
        ),
        // Same expression with the OR written first
        (
            "a|b c",
            vec![term("a"), term("b"), term("c"), SearchToken::And, SearchToken::Or],
        ),
        // NOT applies to the following operand before AND
        (
            "-a b",
            vec![term("a"), SearchToken::Not, term("b"), SearchToken::And],
        ),
        (
            "a -b",
            vec![term("a"), term("b"), SearchToken::Not, SearchToken::And],
        ),
        // NOT has higher precedence than OR
        (
            "-a|b",
            vec![term("a"), SearchToken::Not, term("b"), SearchToken::Or],
        ),
        // Left-associative OR
        (
            "a|b|c",
            vec![term("a"), term("b"), SearchToken::Or, term("c"), SearchToken::Or],
        ),
        ("", vec![]),
    ];

    for (input, expected) in cases {
        let rpn = to_rpn(&tokenize_search_query(input));
        assert_eq!(rpn, expected, "input: {input:?}");
    }
}

fn operand_terms(tokens: &[SearchToken]) -> Vec<(Option<String>, String)> {
    tokens
        .iter()
        .filter_map(|token| match token {
            SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } => {
                Some((col.clone(), text.clone()))
            }
            _ => None,
        })
        .collect()
}

fn evaluate(
    query: &str,
    searchable_text: &[String],
    per_column: &HashMap<String, Vec<String>>,
) -> Vec<bool> {
    let rpn = to_rpn(&tokenize_search_query(query));
    let terms = operand_terms(&rpn);
    build_search_mask_boolean(&rpn, &terms, searchable_text, Some(per_column))
}

#[test]
fn build_search_mask_boolean_evaluates_operators_and_column_scopes() {
    let searchable_text = vec![
        "administrator malware beacon ws01".to_string(),
        "guest error test ws02".to_string(),
        "administrator warn ws01".to_string(),
    ];

    let mut per_column: HashMap<String, Vec<String>> = HashMap::new();
    per_column.insert(
        "user".to_string(),
        vec![
            "administrator".to_string(),
            "guest".to_string(),
            "administrator".to_string(),
        ],
    );
    per_column.insert(
        "command".to_string(),
        vec![
            "malware beacon".to_string(),
            "error test".to_string(),
            "warn".to_string(),
        ],
    );
    per_column.insert(
        "host".to_string(),
        vec!["ws01".to_string(), "ws02".to_string(), "ws01".to_string()],
    );

    let cases: Vec<(&str, Vec<bool>)> = vec![
        // Implicit AND
        ("malware beacon", vec![true, false, false]),
        // OR
        ("error|warn", vec![false, true, true]),
        // AND NOT
        ("administrator -guest", vec![true, false, true]),
        // Column scope
        ("user:administrator", vec![true, false, true]),
        // Column carry-over across OR: both terms search the host column
        ("host:WS01|WS02", vec![true, true, true]),
        // Column scope combined with NOT
        ("host:ws01 -command:warn", vec![true, false, false]),
        // AND across two column scopes
        (
            "user:administrator command:malware",
            vec![true, false, false],
        ),
        // OR across two different column scopes
        ("user:guest|command:warn", vec![false, true, true]),
    ];

    for (query, expected) in cases {
        let mask = evaluate(query, &searchable_text, &per_column);
        assert_eq!(mask, expected, "query: {query:?}");
    }
}

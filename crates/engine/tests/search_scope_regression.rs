use std::collections::HashMap;

use polars::prelude::*;
use trivium_engine::{
    condition::search_to_node,
    jobs::JobContext,
    ops::rows::{resolve_target, QuerySpec},
    search::{tokenize_search_query, SearchToken},
};

#[test]
fn or_column_scope_is_consumed_by_the_next_operand() {
    for (query, expected) in [
        (
            "host:ws01 | user:bob powershell",
            vec![Some("host"), Some("user"), None],
        ),
        (
            "host:ws01 | user:\"bob smith\" \"powershell -enc\"",
            vec![Some("host"), Some("user"), None],
        ),
        (
            "host:ws01 | -user:bob -powershell",
            vec![Some("host"), Some("user"), None],
        ),
        (
            "host:ws01 | -user:\"bob smith\" powershell",
            vec![Some("host"), Some("user"), None],
        ),
        (
            "host:ws01|ws02 powershell",
            vec![Some("host"), Some("host"), None],
        ),
        (
            "host:\"ws01\"|\"ws02\" powershell",
            vec![Some("host"), Some("host"), None],
        ),
        (
            "host:ws01 | user:bob|carol powershell",
            vec![Some("host"), Some("user"), Some("user"), None],
        ),
        (
            "host:ws01 | user:bob powershell | notepad",
            vec![Some("host"), Some("user"), None, None],
        ),
    ] {
        let tokens = tokenize_search_query(query);
        let scopes: Vec<_> = tokens
            .iter()
            .filter_map(|token| match token {
                SearchToken::Term { col, .. } | SearchToken::QuotedTerm { col, .. } => {
                    Some(col.as_deref())
                }
                _ => None,
            })
            .collect();
        assert_eq!(scopes, expected, "query: {query}");
    }
}

#[test]
fn mixed_or_scopes_find_expected_rows_in_search_and_converted_conditions() {
    let frame = DataFrame::new(vec![
        Series::new(
            "host",
            ["ws01", "ws02", "ws02", "ws02", "ws03", "ws03", "ws04"],
        ),
        Series::new(
            "user",
            [
                "alice",
                "bob",
                "bob",
                "carol",
                "bob smith",
                "bob smith",
                "alice",
            ],
        ),
        Series::new(
            "command",
            [
                "notepad",
                "powershell",
                "notepad",
                "powershell",
                "powershell -enc",
                "notepad",
                "powershell -enc",
            ],
        ),
    ])
    .unwrap();
    let columns = vec!["host".into(), "user".into(), "command".into()];
    for (query, expected) in [
        ("host:ws01 | user:bob powershell", vec![0, 1, 4]),
        ("host:ws01 | user:\"bob smith\" powershell", vec![0, 4]),
        ("host:\"ws01\" | user:bob \"powershell -enc\"", vec![0, 4]),
        (
            "host:ws01 | user:\"bob smith\" \"powershell -enc\"",
            vec![0, 4],
        ),
        ("host:ws01 | -user:bob powershell", vec![0, 3, 6]),
        (
            "host:ws01 | -user:\"bob smith\" powershell",
            vec![0, 1, 3, 6],
        ),
        ("host:ws01 | user:bob -powershell", vec![0, 2, 5]),
        ("host:ws01|ws02 powershell", vec![0, 1, 3]),
        ("host:\"ws01\"|\"ws02\" powershell", vec![0, 1, 3]),
        ("host:ws01|-ws02 powershell", vec![0, 4, 6]),
        ("host:ws01|ws02|ws03 powershell", vec![0, 1, 2, 3, 4]),
        (
            "host:ws01 | user:bob|carol powershell",
            vec![0, 1, 2, 3, 4, 5],
        ),
        (
            "host:ws01 | user:bob powershell | notepad",
            vec![0, 1, 2, 4, 5],
        ),
    ] {
        for spec in [
            QuerySpec {
                search: Some(query.into()),
                ..Default::default()
            },
            QuerySpec {
                condition: Some(search_to_node(query).expect("condition tree")),
                ..Default::default()
            },
        ] {
            let actual = resolve_target(
                &frame,
                &spec,
                &columns,
                &HashMap::new(),
                &JobContext::none(),
            )
            .unwrap();
            assert_eq!(actual.filtered, expected, "query: {query}; spec: {spec:?}");
        }
    }
}

#[test]
fn unscoped_conditions_use_the_selected_search_columns() {
    let frame = DataFrame::new(vec![
        Series::new("host", ["ws01", "powershell", "ws02"]),
        Series::new("user", ["alice", "bob", "bob"]),
        Series::new("command", ["notepad", "notepad", "powershell"]),
    ])
    .unwrap();
    let columns = vec!["host".into(), "user".into(), "command".into()];
    let query = "host:ws01 | user:bob powershell";

    for (scope, expected) in [
        (None, vec![0, 1, 2]),
        (Some(vec![]), vec![0, 1, 2]),
        (Some(vec!["command".to_owned()]), vec![0, 2]),
        (Some(vec!["COMMAND".to_owned()]), vec![0, 2]),
        (Some(vec!["host".to_owned()]), vec![0, 1]),
    ] {
        for converted in [false, true] {
            let spec = QuerySpec {
                search: (!converted).then(|| query.into()),
                search_columns: scope.clone(),
                condition: converted.then(|| search_to_node(query).expect("condition tree")),
                ..Default::default()
            };
            let actual = resolve_target(
                &frame,
                &spec,
                &columns,
                &HashMap::new(),
                &JobContext::none(),
            )
            .unwrap();
            assert_eq!(actual.filtered, expected, "spec: {spec:?}");
        }
    }

    // A scoped search must not prevent an unscoped structured condition from
    // requesting the same joined text as an unscoped search-bar term.
    let spec = QuerySpec {
        search: Some("user:bob".into()),
        search_columns: Some(vec!["command".into()]),
        condition: search_to_node("powershell"),
        ..Default::default()
    };
    let actual = resolve_target(
        &frame,
        &spec,
        &columns,
        &HashMap::new(),
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(actual.filtered, vec![2]);
}

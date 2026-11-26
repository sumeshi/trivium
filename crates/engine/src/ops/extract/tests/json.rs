use super::*;

fn all_columns(case: &Case, pattern: Option<&str>) -> ExtractPayload {
    ExtractPayload {
        project_id: case.id,
        columns: case.columns.clone(),
        format: pattern.map_or_else(ipv4, |pattern| FormatDefinition {
            schema_version: 1,
            id: "source-values".into(),
            name: "Source values".into(),
            pattern: pattern.into(),
            value_group: None,
            validation: None,
            examples: None,
        }),
        scope: ExtractScope::All,
        spec: QuerySpec::default(),
    }
}

fn source_column(case: &Case, source: &str) -> String {
    case.store
        .try_find(&case.id)
        .unwrap()
        .unwrap()
        .columns
        .into_iter()
        .find(|column| column.source == source)
        .unwrap()
        .id
}

#[test]
fn mixed_json_parents_do_not_duplicate_a_source_url_in_json_or_jsonl() {
    for (name, source) in [
        (
            "mixed.json",
            r#"[{"payload":"legacy"},{"payload":{"url":"https://example.test"}}]"#,
        ),
        (
            "mixed.jsonl",
            "{\"payload\":\"legacy\"}\n{\"payload\":{\"url\":\"https://example.test\"}}\n",
        ),
        (
            "mixed.ndjson",
            "{\"payload\":\"legacy\"}\n{\"payload\":{\"url\":\"https://example.test\"}}\n",
        ),
    ] {
        let case = Case::new(name, source);
        assert_eq!(
            case.columns.len(),
            2,
            "parent and child really are materialized"
        );
        let payload = all_columns(&case, Some(r"https://[a-z.]+"));
        let result = extract_values(&case.store, payload, &JobContext::none()).unwrap();
        assert_eq!(
            case.values(&result),
            vec![record("https://example.test", "1", "1")]
        );
        case.assert_no_generation_files();
    }
}

#[test]
fn json_column_subtrees_form_a_union_without_deduplicating_real_occurrences() {
    let case = Case::new(
        "overlap.json",
        r#"[
        {"payload":"10.0.0.1 10.0.0.1"},
        {"payload":{"url":"10.0.0.1 10.0.0.1","other":"10.0.0.1"}},
        {"payload":{"url":{"host":"10.0.0.1"},"other":"10.0.0.1"}},
        {"payload":null,"other":"10.0.0.1"}
    ]"#,
    );
    let mut payload = all_columns(&case, None);
    let result = extract_values(&case.store, payload.clone(), &JobContext::none()).unwrap();
    assert_eq!(case.values(&result), vec![record("10.0.0.1", "8", "4")]);

    payload.columns = vec![source_column(&case, "/payload")];
    let result = extract_values(&case.store, payload.clone(), &JobContext::none()).unwrap();
    assert_eq!(case.values(&result), vec![record("10.0.0.1", "7", "3")]);

    payload.columns.push(source_column(&case, "/payload/url"));
    payload
        .columns
        .push(source_column(&case, "/payload/url/host"));
    payload.columns.reverse();
    let result = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!(case.values(&result), vec![record("10.0.0.1", "7", "3")]);

    let mut payload = all_columns(&case, Some(r"^10\.0\.0\.1$"));
    let result = extract_values(&case.store, payload.clone(), &JobContext::none()).unwrap();
    assert_eq!(case.values(&result), vec![record("10.0.0.1", "4", "3")]);
    payload.scope = ExtractScope::Filtered;
    payload.spec.condition = Some(Node::Condition(Condition {
        column: source_column(&case, "/other"),
        op: Op::Eq,
        values: vec!["10.0.0.1".into()],
        negated: false,
    }));
    let result = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!(case.values(&result), vec![record("10.0.0.1", "1", "1")]);
}

#[test]
fn json_arrays_decoded_strings_and_escaped_paths_use_source_values_not_keys() {
    let case = Case::new(
        "values.json",
        r#"[
        {"payload":"legacy","a/b":{"~":"10.0.0.7"}},
        {"payload":{
            "values":["10.0.0.1","10.0.0.1",{"deep":"10.0.0.2"}],
            "10.9.9.9":"10.0.0.3",
            "literal":"{\"embedded\":\"10.0.0.4 10.0.0.4\"}",
            "escaped":"10.0.0.\u0035"
        }}
    ]"#,
    );
    let result =
        extract_values(&case.store, all_columns(&case, None), &JobContext::none()).unwrap();
    assert_eq!(
        case.values(&result),
        vec![
            record("10.0.0.1", "2", "1"),
            record("10.0.0.4", "2", "1"),
            record("10.0.0.2", "1", "1"),
            record("10.0.0.3", "1", "1"),
            record("10.0.0.5", "1", "1"),
            record("10.0.0.7", "1", "1")
        ]
    );

    let mut payload = all_columns(&case, Some(r#"^\{"embedded":"10\.0\.0\.4 10\.0\.0\.4"\}$"#));
    let result = extract_values(&case.store, payload.clone(), &JobContext::none()).unwrap();
    assert_eq!(
        case.values(&result),
        vec![record(r#"{"embedded":"10.0.0.4 10.0.0.4"}"#, "1", "1")]
    );

    payload.format = ipv4();
    payload.columns = vec![source_column(&case, "/a~1b/~0")];
    let result = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!(case.values(&result), vec![record("10.0.0.7", "1", "1")]);
}

#[test]
fn json_number_precision_and_a_literal_number_marker_key_are_preserved() {
    let case = Case::new(
        "numbers.json",
        r#"{
        "n":170141183460469231731687303715884105728,
        "fraction":1.2345678901234567890123456789,
        "exponent":1e+400,"zero":-0,"boolean":true,"null":null,
        "tricky":{"$serde_json::private::Number":"not-a-number"}
    }"#,
    );
    let payload = all_columns(&case, Some(r"(?s)^.+$"));
    let result = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!(
        case.values(&result),
        vec![
            record("-0", "1", "1"),
            record("1.2345678901234567890123456789", "1", "1"),
            record("170141183460469231731687303715884105728", "1", "1"),
            record("1e+400", "1", "1"),
            record("not-a-number", "1", "1"),
            record("true", "1", "1"),
        ]
    );
}

#[test]
fn csv_headers_that_look_like_json_paths_are_still_independent_cells() {
    let case = Case::new("paths.csv", "/payload,/payload/url\n10.0.0.1,10.0.0.1\n");
    let result =
        extract_values(&case.store, all_columns(&case, None), &JobContext::none()).unwrap();
    assert_eq!(case.values(&result), vec![record("10.0.0.1", "2", "1")]);
}

#[test]
fn missing_or_unsupported_source_formats_reject_extraction_without_creating_a_case() {
    for (name, source) in [
        (
            "source.json",
            r#"[{"payload":"legacy"},{"payload":{"ip":"10.0.0.1"}}]"#,
        ),
        ("source.csv", "/payload,/payload/ip\n10.0.0.1,10.0.0.1\n"),
    ] {
        let case = Case::new(name, source);
        for format in [None, Some("xml")] {
            crate::storage::update_project_meta(&case.store.project_dir(&case.id), |meta| {
                meta.source_format = format.map(str::to_owned);
            })
            .unwrap();
            let error = extract_values(&case.store, all_columns(&case, None), &JobContext::none())
                .unwrap_err();
            assert!(error.to_string().contains("source format"));
            assert_eq!(case.store.try_all().unwrap().len(), 1);
            case.assert_no_generation_files();
        }
    }
}

#[test]
fn escaped_json_strings_respect_the_transient_budget_even_when_the_pattern_has_no_hits() {
    let source = serde_json::json!({"value":"\n".repeat(128)}).to_string();
    let case = Case::new("escaped.json", &source);
    let error = scan::build(
        &case.store,
        all_columns(&case, Some("not-present")),
        aggregate::Limits {
            distinct: 100,
            bytes: 128,
        },
        &JobContext::none(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("no complete result"));
    assert_eq!(case.store.try_all().unwrap().len(), 1);
    case.assert_no_generation_files();
}

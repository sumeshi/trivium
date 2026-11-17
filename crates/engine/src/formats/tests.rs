use serde_json::json;

use super::*;

fn definition(pattern: &str, capture: Option<&str>) -> FormatDefinition {
    serde_json::from_value(json!({"schemaVersion":1,"id":"test-pattern","name":"Test",
        "pattern":pattern,"valueGroup":capture}))
    .unwrap()
}

#[test]
fn captured_values_reuse_delimiters_and_retain_utf8_cell_offsets() {
    let matcher = CompiledFormat::new(&definition(
        r"(?:\A|[ ,])(?P<value>[A-Z]+)(?:[ ,]|\z)",
        Some("value"),
    ))
    .unwrap();
    let text = "日本 AA,BB,AA 日本";
    let found: Vec<_> = matcher.occurrences(text).collect();
    assert_eq!(
        found.iter().map(|value| value.value).collect::<Vec<_>>(),
        ["AA", "BB", "AA"]
    );
    assert_eq!(
        found
            .iter()
            .map(|value| (value.start, value.end))
            .collect::<Vec<_>>(),
        [(7, 9), (10, 12), (13, 15)]
    );
    for value in &found {
        assert_eq!(&text[value.start..value.end], value.value);
    }
    assert!(matcher.has_match(text, Some("AA")));
    assert!(
        !matcher.has_match(text, Some("A")),
        "reverse filtering compares entire captured values"
    );
    assert_eq!(text, "日本 AA,BB,AA 日本");
}

#[test]
fn absent_and_zero_length_captures_do_not_count_or_skip_the_next_value() {
    let optional = CompiledFormat::new(&definition(r"a|(?P<value>b)", Some("value"))).unwrap();
    assert_eq!(
        optional
            .occurrences("abb")
            .map(|value| value.value)
            .collect::<Vec<_>>(),
        ["b", "b"]
    );
    let empty = CompiledFormat::new(&definition(r"(?P<value>\b)", Some("value"))).unwrap();
    assert_eq!(empty.occurrences("日本 abc").count(), 0);
    assert!(!empty.has_match("日本 abc", None));
    let mixed = CompiledFormat::new(&definition(r"(?P<value>a*)", Some("value"))).unwrap();
    assert_eq!(
        mixed
            .occurrences("日本abbaa")
            .map(|value| value.value)
            .collect::<Vec<_>>(),
        ["a", "aa"]
    );
}

#[test]
fn checked_traversal_can_cancel_inside_one_cell_without_any_returned_match() {
    use crate::error::AppError;

    for (pattern, validation, equals, text) in [
        (
            "(?P<value>x)",
            Some(FormatValidation::Ipv4Like),
            None,
            "x".repeat(4096),
        ),
        ("x|(?P<value>z)", None, None, "x".repeat(4096)),
        ("(?P<value>)", None, None, "日本".repeat(2048)),
        ("(?P<value>x)", None, Some("not-x"), "x".repeat(4096)),
    ] {
        let mut definition = definition(pattern, Some("value"));
        definition.validation = validation;
        let matcher = CompiledFormat::new(&definition).unwrap();
        let mut checks = 0;
        let error = matcher
            .has_match_with_check(&text, equals, &mut || {
                checks += 1;
                if checks == 2 {
                    Err(AppError::Message("Cancelled.".into()))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "Cancelled.", "{pattern}");
        assert_eq!(
            checks, 2,
            "a rejected/empty capture must still reach checkpoints"
        );
    }
}

#[test]
fn checked_traversal_preserves_unchecked_values_and_utf8_offsets() {
    let matcher = CompiledFormat::new(&definition(
        r"(?:\A|[ ,])(?P<value>[A-Z]+)(?:[ ,]|\z)",
        Some("value"),
    ))
    .unwrap();
    let text = "日本 AA,BB,AA 日本";
    let expected = matcher.occurrences(text).collect::<Vec<_>>();
    let mut checked = matcher.occurrences(text);
    let mut actual = Vec::new();
    while let Some(value) = checked.next_with_check(&mut || Ok(())).unwrap() {
        actual.push(value);
    }
    assert_eq!(actual, expected);
}

#[test]
fn anchors_keep_full_cell_meaning_after_each_occurrence() {
    let anchored = CompiledFormat::new(&definition(r"\A(?P<value>[A-Z])", Some("value"))).unwrap();
    assert_eq!(
        anchored
            .occurrences("ABC")
            .map(|value| value.value)
            .collect::<Vec<_>>(),
        ["A"]
    );
    let whole = CompiledFormat::new(&definition(r"\A[A-Z]+\z", None)).unwrap();
    assert_eq!(whole.occurrences("ABC").next().unwrap().value, "ABC");
    assert!(!whole.has_match("ABC DEF", None));
}

#[test]
fn invalid_definitions_and_examples_are_rejected_before_use() {
    assert!(CompiledFormat::new(&definition(r"(?<=x)y", None)).is_err());
    assert!(CompiledFormat::new(&definition("x", Some("missing"))).is_err());
    let mut invalid = definition("x", None);
    invalid.schema_version = 2;
    assert!(CompiledFormat::new(&invalid).is_err());
    invalid.schema_version = 1;
    invalid.examples = Some(vec![FormatExample {
        text: "x".into(),
        values: vec!["y".into()],
    }]);
    assert!(CompiledFormat::new(&invalid)
        .unwrap_err()
        .to_string()
        .contains("example 1"));
}

#[test]
fn builtin_ipv4_preserves_all_addresses_and_rejects_invalid_boundaries() {
    let root = std::env::temp_dir().join(format!("trivium-format-test-{}", uuid::Uuid::new_v4()));
    let entries = list_formats(&root).unwrap();
    let ipv4 = CompiledFormat::new(
        &entries
            .iter()
            .find(|entry| entry.definition.id == "ipv4")
            .unwrap()
            .definition,
    )
    .unwrap();
    assert_eq!(
        ipv4.occurrences("src:10.0.0.1,8.8.8.8:443")
            .map(|value| value.value)
            .collect::<Vec<_>>(),
        ["10.0.0.1", "8.8.8.8:443"]
    );
    assert!(!ipv4.has_match("256.1.2.3 1.2.3.4.5 001.2.3.4", None));
    for address in [
        "10.1.2.3",
        "172.31.255.255",
        "172.32.0.1",
        "127.0.0.1",
        "100.64.0.1",
        "169.254.1.1",
        "192.0.2.1",
        "198.18.0.1",
        "224.0.0.1",
        "240.0.0.1",
        "192.0.0.9",
        "192.0.0.8",
    ] {
        assert_eq!(ipv4.occurrences(address).next().unwrap().value, address);
    }
    let _ = std::fs::remove_dir_all(root);
}

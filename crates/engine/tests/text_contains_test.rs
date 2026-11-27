use trivium_engine::condition::{Condition, Node, Op, PreparedNode};

fn prepared(values: &[&str], negated: bool) -> PreparedNode {
    PreparedNode::prepare(&Node::Condition(Condition {
        column: "value".into(),
        op: Op::Contains,
        values: values.iter().map(|value| (*value).into()).collect(),
        negated,
    }))
    .unwrap()
}

fn check(hay: Option<&str>, values: &[&str]) {
    let lower = hay.unwrap_or_default().to_lowercase();
    let matched = values
        .iter()
        .filter(|value| !value.is_empty())
        .any(|value| lower.contains(&value.to_lowercase()));
    for negated in [false, true] {
        let predicate = prepared(values, negated);
        let bound = predicate.bind_columns(&|_| 0);
        assert_eq!(
            bound.eval(&[hay.map(Into::into)], None).unwrap(),
            negated ^ matched,
            "hay={hay:?}, values={values:?}, negated={negated}"
        );
    }
}

#[test]
fn contains_keeps_literal_case_insensitive_and_null_semantics() {
    for hay in [
        None,
        Some(""),
        Some("A"),
        Some("AAAb"),
        Some("xAAAB"),
        Some("aaaa"),
        Some("a\0B"),
        Some("[] a+b .*"),
        Some("prefix needle"),
        Some("PoWeRsHeLl.exe -enc payload"),
    ] {
        for values in [
            vec![],
            vec![""],
            vec!["a"],
            vec!["AAAB"],
            vec!["AAAAA"],
            vec!["needle"],
            vec!["a\0b"],
            vec![".*"],
            vec!["missing", "POWERSHELL"],
            vec!["", "missing", "a+b"],
        ] {
            check(hay, &values);
        }
    }
}

#[test]
fn contains_preserves_contextual_unicode_lowercase() {
    for hay in [
        "ΟΣ",
        "ΟΣ Σ",
        "İstanbul",
        "ıstanbul",
        "Straße",
        "Kelvin",
        "調査 POWERSHELL",
        "a\u{307}",
    ] {
        for value in [
            "Σ",
            "σ",
            "ς",
            "İ",
            "i",
            "ı",
            "SS",
            "ß",
            "K",
            "K",
            "powershell",
            "調査",
        ] {
            check(Some(hay), &[value]);
            check(Some(hay), &["missing", value]);
        }
    }
}

#[test]
fn contains_matches_reference_across_offsets_and_repeated_prefixes() {
    let mut state = 0x1248_abcd_u64;
    let alphabet = b"abAB\0.$[]09xyzXYZ";
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        (state >> 32) as usize
    };
    for iteration in 0..4096 {
        let mut hay: Vec<u8> = (0..next() % 256)
            .map(|_| alphabet[next() % alphabet.len()])
            .collect();
        let needle: Vec<u8> = (0..next() % 32)
            .map(|_| alphabet[next() % alphabet.len()])
            .collect();
        if iteration % 3 == 0 {
            hay.extend(needle.iter().map(u8::to_ascii_uppercase));
        }
        let hay = String::from_utf8(hay).unwrap();
        let needle = String::from_utf8(needle).unwrap();
        check(Some(&hay), &[&needle]);
    }
    for prefix in [1, 2, 8, 128, 4096] {
        let hay = format!("{}B", "a".repeat(8192));
        let needle = format!("{}b", "a".repeat(prefix));
        check(Some(&hay), &[&needle]);
        check(Some(&"a".repeat(8192)), &[&needle]);
    }
}

#[test]
fn column_scan_matches_row_evaluation_across_chunks_scopes_and_negation() {
    use polars::prelude::*;
    use std::collections::HashMap;
    use trivium_engine::{
        condition::{Group, GroupKind},
        jobs::JobContext,
        ops::rows::{resolve_target, QuerySpec},
    };

    let mut first = Series::new("first", [Some("failed"), None, Some("failed login")]);
    first
        .append(&Series::new("first", [Some("ΟΣ"), Some(""), Some("aaaab")]))
        .unwrap();
    let mut second = Series::new("second", [Some("login"), Some("FAILED LOGIN"), None]);
    second
        .append(&Series::new("second", [Some("İstanbul"), None, Some("x")]))
        .unwrap();
    let frame = DataFrame::new(vec![first, second]).unwrap();
    let columns = vec!["first".into(), "second".into()];
    for column in ["", "first", "SECOND"] {
        for values in [
            vec![],
            vec![""],
            vec!["failed login"],
            vec!["σ", "ς", "i"],
            vec!["missing", "AAAAB"],
        ] {
            for negated in [false, true] {
                let node = Node::Condition(Condition {
                    column: column.into(),
                    op: Op::Contains,
                    values: values.iter().map(|value| (*value).into()).collect(),
                    negated,
                });
                for scope in [
                    None,
                    Some(vec![]),
                    Some(vec!["first".into()]),
                    Some(vec!["second".into()]),
                ] {
                    let query = |condition| {
                        resolve_target(
                            &frame,
                            &QuerySpec {
                                condition: Some(condition),
                                search_columns: scope.clone(),
                                ..Default::default()
                            },
                            &columns,
                            &HashMap::new(),
                            &JobContext::none(),
                        )
                        .unwrap()
                    };
                    // A one-child group uses the general row evaluator.
                    let reference = query(Node::Group(Group {
                        kind: GroupKind::And,
                        children: vec![node.clone()],
                    }));
                    assert_eq!(query(node.clone()), reference, "{node:?}, scope={scope:?}");
                }
            }
        }
    }
}

//! Missing or unparseable source values are not malformed query bounds.
use std::{borrow::Cow, collections::HashMap, fs, path::PathBuf};

use serde_json::{json, Value};
use trivium_engine::{
    condition::{eval, Condition, Group, GroupKind, Node, Op, PreparedNode},
    ops,
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

fn predicate(op: Op, values: &[&str], negated: bool) -> Node {
    Node::Condition(Condition {
        column: "value".into(),
        op,
        values: values.iter().map(|value| (*value).into()).collect(),
        negated,
    })
}

#[test]
fn ordered_source_gaps_and_invalid_values_do_not_match_in_both_evaluators() {
    let missing = HashMap::new();
    let mut rows = vec![missing];
    for value in [
        Value::Null,
        json!(""),
        json!(" \t\u{3000}"),
        json!("N/A"),
        json!("NaN"),
        json!("Infinity"),
        json!("not a date or number"),
        json!("2026-13-14"),
        json!("1e"),
    ] {
        rows.push(HashMap::from([("value".into(), value)]));
    }
    for bound in [
        "0",
        "1.000000000000000000001",
        "2026-11-14T00:00:00.000000001Z",
    ] {
        for op in [Op::Gt, Op::Ge, Op::Lt, Op::Le] {
            for negated in [false, true] {
                let node = predicate(op, &[bound], negated);
                let prepared = PreparedNode::prepare(&node).unwrap();
                let borrowed = prepared.bind_columns(&|_| 0);
                for row in &rows {
                    assert_eq!(eval(&node, row).unwrap(), negated, "{row:?} {node:?}");
                    let cell = row.get("value").and_then(Value::as_str).map(Cow::Borrowed);
                    assert_eq!(borrowed.eval(&[cell], None).unwrap(), negated);
                }
            }
        }
    }
    // The bound selects the interpretation, even when the source looks valid
    // in another domain. A datetime is not compared to a number as source text.
    for (source, bound) in [("42", "2026-11-14"), ("2026-11-14", "42")] {
        let row = HashMap::from([("value".into(), json!(source))]);
        assert!(!eval(&predicate(Op::Ge, &[bound], false), &row).unwrap());
    }
}

#[test]
fn invalid_ordered_bounds_fail_preparation_before_data_or_short_circuiting() {
    for bound in [
        "",
        " \t",
        "N/A",
        "NaN",
        "Infinity",
        "1.2.3",
        "1e",
        "2026-13-14",
    ] {
        for op in [Op::Gt, Op::Ge, Op::Lt, Op::Le] {
            let bad = predicate(op, &["0", bound], false);
            for node in [
                bad.clone(),
                Node::Group(Group {
                    kind: GroupKind::And,
                    children: vec![predicate(Op::Eq, &["never matches"], false), bad],
                }),
            ] {
                let error = PreparedNode::prepare(&node).unwrap_err().to_string();
                assert!(error.contains("Ordered comparison requires"), "{error}");
                assert!(eval(&node, &HashMap::new()).is_err());
            }
        }
    }
}

#[test]
fn prepared_bounds_keep_decimal_digits_offsets_and_nanoseconds_exact() {
    for (source, bound) in [
        (
            "9007199254740993.000000000000000001",
            "9007199254740993.000000000000000002",
        ),
        (
            "170141183460469231731687303715884105728",
            "170141183460469231731687303715884105729",
        ),
        (
            "2026-11-14T09:00:00.000000001+09:00",
            "2026-11-14T00:00:00.000000002Z",
        ),
    ] {
        let row = HashMap::from([("value".into(), json!(source))]);
        for op in [Op::Lt, Op::Le] {
            assert!(eval(&predicate(op, &[bound], false), &row).unwrap());
        }
        for op in [Op::Gt, Op::Ge] {
            assert!(!eval(&predicate(op, &[bound], false), &row).unwrap());
        }
    }
    let row = HashMap::from([("value".into(), json!("2026-11-14T09:00:00.000000001+09:00"))]);
    let utc = "2026-11-14T00:00:00.000000001Z";
    assert!(eval(&predicate(Op::Ge, &[utc], false), &row).unwrap());
    assert!(eval(&predicate(Op::Le, &[utc], false), &row).unwrap());
    assert!(!eval(&predicate(Op::Gt, &[utc], false), &row).unwrap());
    assert!(!eval(&predicate(Op::Lt, &[utc], false), &row).unwrap());
}

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn saved_csv_and_jsonl_ranges_skip_gaps_without_changing_source_values() {
    const FIRST: &str = "2026-11-14T09:00:00.000000001+09:00";
    const LAST: &str = "2026-11-14T00:00:00.000000002Z";
    for (extension, bytes, last_row) in [
        ("csv", format!("Timestamp,Number,Event\n{FIRST},9007199254740993.000000000000000001,start\n,,empty\n\" \t \",\" \t \",space\nN/A,N/A,invalid\n42,2026-11-14,cross-domain\n{LAST},9007199254740993.000000000000000002,finish\n"), 5),
        ("jsonl", [
            json!({"Timestamp":FIRST,"Number":"9007199254740993.000000000000000001","Event":"start"}),
            json!({"Timestamp":null,"Number":null,"Event":"null"}),
            json!({"Event":"missing"}),
            json!({"Timestamp":"","Number":"","Event":"empty"}),
            json!({"Timestamp":" \t ","Number":" \t ","Event":"space"}),
            json!({"Timestamp":"N/A","Number":"N/A","Event":"invalid"}),
            json!({"Timestamp":42,"Number":"2026-11-14","Event":"cross-domain"}),
            json!({"Timestamp":LAST,"Number":"9007199254740993.000000000000000002","Event":"finish"}),
        ].map(|row| row.to_string()).join("\n"), 7),
    ] {
        let root = Fixture(std::env::temp_dir().join(format!("trivium-ordered-gaps-{}", Uuid::new_v4())));
        fs::create_dir_all(&root.0).unwrap();
        let source = root.0.join(format!("events.{extension}"));
        fs::write(&source, &bytes).unwrap();
        let id = {
            let store = ProjectsStore::new(root.0.join("workspace")).unwrap();
            ops::projects::create_project(&store, serde_json::from_value(json!({"path":source})).unwrap())
                .unwrap().meta.id
        };
        // Reopen persisted storage rather than passing a complete DataFrame.
        let store = ProjectsStore::new(root.0.join("workspace")).unwrap();
        let ids = common::column_ids(&store, id);
        let timestamp = common::id(&ids, if extension == "csv" {"Timestamp"} else {"/Timestamp"});
        let number = common::id(&ids, if extension == "csv" {"Number"} else {"/Number"});
        let range = json!({"kind":"and","children":[
            {"column":timestamp,"op":"ge","values":["2026-11-14T00:00:00.000000001Z"]},
            {"column":timestamp,"op":"le","values":[LAST]},
        ]});
        let result = ops::rows::query_project_rows(&store, serde_json::from_value(json!({
            "projectId":id,"condition":range,
        })).unwrap()).unwrap();
        assert_eq!(result.rows.iter().map(|row| row.row_index).collect::<Vec<_>>(), [0, last_row]);
        assert_eq!(common::cell_text(&result.rows[0], &timestamp), Some(FIRST));
        assert_eq!(common::cell_text(&result.rows[1], &timestamp), Some(LAST));

        let destination = root.0.join("range.csv");
        let exported = ops::export::export_project(&store, serde_json::from_value(json!({
            "projectId":id,"destination":destination,"condition":range,"includeAnnotations":false,
        })).unwrap()).unwrap();
        assert_eq!(exported.rows, 2);
        let preview = ops::bulk::bulk_preview(&store, serde_json::from_value(json!({
            "projectId":id,"target":"filtered","condition":range,"flag":"critical",
        })).unwrap()).unwrap();
        assert_eq!(preview.count, 2);
        let numeric = ops::rows::query_project_rows(&store, serde_json::from_value(json!({
            "projectId":id,"condition":{"column":number,"op":"gt","values":["9007199254740993.000000000000000001"]},
        })).unwrap()).unwrap();
        assert_eq!(numeric.rows.iter().map(|row| row.row_index).collect::<Vec<_>>(), [last_row]);

        // Even an empty result cannot hide an invalid user bound.
        let invalid = json!({"kind":"and","children":[
            {"column":timestamp,"op":"eq","values":["no such value"]},
            {"column":timestamp,"op":"ge","values":["not-a-timestamp"]},
        ]});
        assert!(ops::rows::query_project_rows(&store, serde_json::from_value(json!({
            "projectId":id,"condition":invalid,
        })).unwrap()).is_err());
        assert_eq!(fs::read_to_string(&source).unwrap(), bytes);
    }
}

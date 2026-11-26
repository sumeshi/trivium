//! JSON source types and exact stored text stay consistent across cell DTOs.

use polars::prelude::{AnyValue, DataFrame, NamedFrom, Series};
use serde_json::{json, Value};
use trivium_engine::{
    models::ColumnMeta,
    value_utils::{build_cell, build_full_cell, build_row_cells, CellColumns, CELL_PREVIEW_CHARS},
};

#[test]
fn stored_object_values_keep_distinct_null_missing_and_scalar_cells() {
    let records = [
        json!({"context": {"host": "foo"}}),
        json!({"context": "bar"}),
        json!({"context": null}),
        json!({}),
    ];
    let originals: Vec<_> = records.iter().map(Value::to_string).collect();
    let df = DataFrame::new(vec![
        Series::new(
            "c000001",
            &[Some("{\"host\":\"foo\"}"), Some("bar"), None, None],
        ),
        Series::new("__original", originals),
    ])
    .unwrap();
    let columns = [ColumnMeta {
        id: "c000001".into(),
        label: "context".into(),
        source: "/context".into(),
        order: 0,
    }];
    let builder = CellColumns::new(&columns);
    for (index, (text, kind)) in [
        (Some("{\"host\":\"foo\"}"), "object"),
        (Some("bar"), "string"),
        (None, "null"),
        (None, "missing"),
    ]
    .into_iter()
    .enumerate()
    {
        let page = build_row_cells(&df, index, &columns, &builder);
        let full = builder.build_full(
            "c000001",
            &df.column("c000001").unwrap().get(index).unwrap(),
            Some(&records[index]),
        );
        assert_eq!(page["c000001"], full);
        assert_eq!(full.text.as_deref(), text);
        assert_eq!(full.kind, kind);
    }
}

#[test]
fn stored_json_cells_keep_exact_text_types_and_unicode_preview_lengths() {
    let long = "調査".repeat(CELL_PREVIEW_CHARS);
    let original = json!({
        "text": "<a@example.org>\n\"quoted\"",
        "empty": "",
        "flag": false,
        "number": 9007199254740993_u64,
        "array": [null, "", 1],
        "object": {"message": long},
    });
    for (source, text, kind) in [
        ("/text", "<a@example.org>\n\"quoted\"", "string"),
        ("/empty", "", "string"),
        ("/flag", "false", "bool"),
        ("/number", "9007199254740993", "int"),
        ("/array", "[null,\"\",1]", "array"),
    ] {
        let cell = build_full_cell(&AnyValue::String(text), Some(&original), source);
        assert_eq!(cell.text.as_deref(), Some(text));
        assert_eq!(cell.kind, kind);
        assert_eq!(cell.length, Some(text.chars().count()));
        assert!(!cell.truncated);
    }
    let expected = original["object"].to_string();
    let full = build_full_cell(&AnyValue::String(&expected), Some(&original), "/object");
    let preview = build_cell(&AnyValue::String(&expected), Some(&original), "/object");
    assert_eq!(full.text.as_deref(), Some(expected.as_str()));
    assert_eq!(full.kind, "object");
    assert_eq!(full.length, Some(expected.chars().count()));
    assert!(!full.truncated);
    assert_eq!(
        preview.text.as_deref(),
        Some(
            expected
                .chars()
                .take(CELL_PREVIEW_CHARS)
                .collect::<String>()
                .as_str()
        )
    );
    assert_eq!(preview.length, full.length);
    assert!(preview.truncated);
}

#[test]
fn flattened_paths_resolve_numeric_object_keys_and_escapes_but_never_array_indices() {
    let object = json!({"a/b~": {"0": "host"}});
    let array = json!({"a/b~": ["host"]});
    let field = "/a~1b~0/0";
    let object_cell = build_full_cell(&AnyValue::String("host"), Some(&object), field);
    assert_eq!(object_cell.text.as_deref(), Some("host"));
    assert_eq!(object_cell.kind, "string");
    let array_cell = build_full_cell(&AnyValue::Null, Some(&array), field);
    assert_eq!(array_cell.text, None);
    assert_eq!(array_cell.kind, "missing");
    // Even a non-null stale slot must not acquire a source type from an array
    // index: array elements are not flattened columns.
    let stale = build_full_cell(&AnyValue::String("123"), Some(&json!({"a": [123]})), "/a/0");
    assert_eq!(stale.kind, "string");
    let csv = json!(["host"]);
    let csv_null = build_full_cell(&AnyValue::Null, Some(&csv), "/0");
    assert_eq!(csv_null.text, None);
    assert_eq!(csv_null.kind, "null");
}

#[test]
fn arbitrary_precision_json_number_kinds_follow_spelling_without_rounding_text() {
    for (raw, kind) in [
        ("18446744073709551615", "int"),
        ("18446744073709551616", "int"),
        ("-9223372036854775809", "int"),
        ("170141183460469231731687303715884105728", "int"),
        ("-170141183460469231731687303715884105729", "int"),
        ("1.0", "float"),
        ("1e3", "float"),
        ("1.000000000000000000001", "float"),
        ("4.2e-400", "float"),
    ] {
        let original: Value = serde_json::from_str(&format!("{{\"n\":{raw}}}")).unwrap();
        let stored = build_full_cell(&AnyValue::String(raw), Some(&original), "/n");
        assert_eq!(stored.text.as_deref(), Some(raw));
        assert_eq!(stored.kind, kind, "{raw}");
        let string_record = json!({"n": raw});
        let string_cell = build_full_cell(&AnyValue::String(raw), Some(&string_record), "/n");
        assert_eq!(string_cell.kind, "string");
        assert_eq!(string_cell.text.as_deref(), Some(raw));
    }
}

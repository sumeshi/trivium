//! Page-local column bindings preserve frame positions and exact JSON source types.

use std::collections::HashMap;

use polars::prelude::*;
use trivium_engine::{
    models::{Cell, ColumnMeta},
    value_utils::CellColumns,
};

fn column(id: &str, source: &str) -> ColumnMeta {
    ColumnMeta {
        id: id.into(),
        label: id.into(),
        source: source.into(),
        order: 0,
    }
}

fn expect(cells: &HashMap<String, Cell>, id: &str, text: Option<&str>, kind: &str) {
    assert_eq!(cells[id].text.as_deref(), text, "{id}");
    assert_eq!(cells[id].kind, kind, "{id}");
    assert!(!cells[id].truncated, "{id}");
    assert_eq!(
        cells[id].length,
        text.map(|value| value.chars().count()),
        "{id}"
    );
}

#[test]
fn bindings_follow_each_sliced_or_reordered_frame_and_metadata_projection() {
    let names = ["zero", "one", "two"];
    let numbers = ["1", "0007", "1e100"];
    let kinds = ["int", "string", "float"];
    let frame = DataFrame::new(vec![
        Series::new("unrelated", [9_u32, 8, 7]),
        Series::new("number", numbers),
        Series::new(
            "__original",
            [
                r#"{"number":1,"name":"zero"}"#,
                r#"{"number":"0007","name":"one"}"#,
                r#"{"number":1e100,"name":"two"}"#,
            ],
        ),
        Series::new("name", names),
    ])
    .unwrap();
    // Metadata order deliberately differs from the physical frame order.
    let columns = [column("name", "/name"), column("number", "/number")];
    let order = UInt32Chunked::from_slice("order", &[2, 0, 2]);
    for (page, originals) in [
        (frame.slice(1, 2), vec![1, 2]),
        (frame.take(&order).unwrap(), vec![2, 0, 2]),
    ] {
        let cells = CellColumns::new(&page, &columns);
        for (position, source) in originals.into_iter().enumerate() {
            let row = cells.build_row(position);
            assert_eq!(row.len(), 2);
            expect(&row, "name", Some(names[source]), "string");
            expect(&row, "number", Some(numbers[source]), kinds[source]);
        }
    }
}

#[test]
fn decoded_paths_keep_empty_escaped_keys_and_do_not_descend_into_arrays() {
    let frame = DataFrame::new(vec![
        Series::new("escaped", [Some("object-zero"), None, None]),
        Series::new("empty_key", [Some("tilde-one"), Some("tilde-two"), None]),
        Series::new("parent", [r#"{"0":"object-zero"}"#, r#"["array-zero"]"#, "{}"]),
        Series::new("empty_root", [r#"{"~1":"tilde-one"}"#, r#"{"~1":"tilde-two"}"#, r#"{"~1":null}"#]),
        Series::new("nullv", [None::<&str>, None, None]),
        Series::new("number", ["18446744073709551616", "4.2e-400", "18446744073709551616"]),
        Series::new("__original", [
            r#"{"a/b~":{"0":"object-zero"},"":{"~1":"tilde-one"},"nullv":null,"number":18446744073709551616}"#,
            r#"{"a/b~":["array-zero"],"":{"~1":"tilde-two"},"nullv":null,"number":4.2e-400}"#,
            r#"{"a/b~":{},"":{"~1":null},"number":"18446744073709551616"}"#,
        ]),
    ]).unwrap();
    let columns = [
        column("escaped", "/a~1b~0/0"),
        column("empty_key", "//~01"),
        column("parent", "/a~1b~0"),
        column("empty_root", "/"),
        column("nullv", "/nullv"),
        column("number", "/number"),
    ];
    let cells = CellColumns::new(&frame, &columns);
    let first = cells.build_row(0);
    expect(&first, "escaped", Some("object-zero"), "string");
    expect(&first, "empty_key", Some("tilde-one"), "string");
    expect(&first, "parent", Some(r#"{"0":"object-zero"}"#), "object");
    expect(
        &first,
        "empty_root",
        Some(r#"{"~1":"tilde-one"}"#),
        "object",
    );
    expect(&first, "nullv", None, "null");
    expect(&first, "number", Some("18446744073709551616"), "int");

    let second = cells.build_row(1);
    expect(&second, "escaped", None, "missing");
    expect(&second, "empty_key", Some("tilde-two"), "string");
    expect(&second, "parent", Some(r#"["array-zero"]"#), "array");
    expect(&second, "nullv", None, "null");
    expect(&second, "number", Some("4.2e-400"), "float");

    let third = cells.build_row(2);
    expect(&third, "escaped", None, "missing");
    expect(&third, "empty_key", None, "null");
    expect(&third, "parent", Some("{}"), "object");
    expect(&third, "nullv", None, "missing");
    expect(&third, "number", Some("18446744073709551616"), "string");
}

#[test]
fn omitted_series_and_original_columns_keep_the_existing_null_fallbacks() {
    let columns = [
        column("real", "/present"),
        column("absent", "/present"),
        column("missing", "/missing"),
    ];
    let csv = DataFrame::new(vec![Series::new("real", [Some("0007"), None])]).unwrap();
    let cells = CellColumns::new(&csv, &columns);
    expect(&cells.build_row(0), "real", Some("0007"), "string");
    for name in ["absent", "missing"] {
        expect(&cells.build_row(0), name, None, "null");
    }
    expect(&cells.build_row(1), "real", None, "null");

    let json = DataFrame::new(vec![
        Series::new("real", ["kept"]),
        Series::new("__original", [r#"{"present":"kept"}"#]),
    ])
    .unwrap();
    let row = CellColumns::new(&json, &columns).build_row(0);
    expect(&row, "absent", None, "null");
    expect(&row, "missing", None, "missing");
}

#[test]
fn original_records_still_use_strict_duplicate_and_arbitrary_precision_parsing() {
    let frame = DataFrame::new(vec![
        Series::new("value", ["true", "true", "true"]),
        Series::new("private", [Some("42"), None, None]),
        Series::new(
            "__original",
            [
                r#"{"x":true,"$serde_json::private::Number":"42"}"#,
                r#"{"x":true,"x":false}"#,
                r#"{"x":true}"#,
            ],
        ),
    ])
    .unwrap();
    let columns = [
        column("value", "/x"),
        column("private", "/$serde_json::private::Number"),
    ];
    let cells = CellColumns::new(&frame, &columns);
    let first = cells.build_row(0);
    expect(&first, "value", Some("true"), "bool");
    expect(&first, "private", Some("42"), "string");
    // Duplicate keys discard source-type hints instead of silently using one value.
    expect(&cells.build_row(1), "value", Some("true"), "string");
    expect(&cells.build_row(2), "value", Some("true"), "bool");
}

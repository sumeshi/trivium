//! Display, filtering and analysis consume one engine interpretation of dates.
use polars::prelude::{AnyValue, TimeUnit};
use serde::Deserialize;
use serde_json::{json, Value};
use trivium_engine::{ops, store::ProjectsStore, value_utils::build_cell};

#[derive(Deserialize)]
struct Fixture {
    input: String,
    iso: Option<String>,
}

#[test]
fn source_datetime_fixtures_publish_exact_keys_without_rewriting_text() {
    let fixtures: Vec<Fixture> = serde_json::from_str(include_str!(
        "../../../tests/fixtures/datetime_formats.json"
    ))
    .unwrap();
    for fixture in fixtures {
        let expected = fixture.iso.as_deref().map(|iso| {
            chrono::DateTime::parse_from_rfc3339(iso)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
                .to_string()
        });
        // A string column can contain both timestamps and unparseable values.
        // Recognition belongs to the value, independent of a page's other rows.
        {
            let cell = build_cell(&AnyValue::String(&fixture.input), None, "value");
            let wire = serde_json::to_value(&cell).unwrap();
            assert_eq!(
                wire.get("datetime_nanos").and_then(Value::as_str),
                expected.as_deref(),
                "{:?}",
                fixture.input
            );
            assert_eq!(cell.text.as_deref(), Some(fixture.input.as_str()));
        }
    }
}

#[test]
fn typed_temporal_units_publish_exact_keys_without_guessing_numeric_epochs() {
    let timezone = None;
    for (value, expected) in [
        (AnyValue::Date(-1), Some("-86400000000000")),
        (
            AnyValue::Datetime(-1, TimeUnit::Nanoseconds, &timezone),
            Some("-1"),
        ),
        (
            AnyValue::Datetime(1, TimeUnit::Microseconds, &timezone),
            Some("1000"),
        ),
        (
            AnyValue::Datetime(1, TimeUnit::Milliseconds, &timezone),
            Some("1000000"),
        ),
        (AnyValue::Int64(1_700_000_000), None),
        (AnyValue::Time(1), None),
        (AnyValue::Null, None),
    ] {
        let cell = build_cell(&value, None, "value");
        assert_eq!(
            serde_json::to_value(cell)
                .unwrap()
                .get("datetime_nanos")
                .and_then(Value::as_str),
            expected
        );
    }
}

#[test]
fn mixed_csv_and_json_pages_and_full_cells_share_keys_and_preserve_padded_evidence() {
    let root = tempfile::tempdir().unwrap();
    let padded = format!(
        "{}2026/11/14 03:04:05,123456790 UTC{}",
        " ".repeat(300),
        "\u{2002}".repeat(90)
    );
    let values = [
        Some("2026-11-14T03:04:05.123456789Z"),
        Some(padded.as_str()),
        Some("N/A"),
        Some(" \t "),
        Some("1700000000"),
        None,
    ];
    let expected = [
        Some("1794625445123456789"),
        Some("1794625445123456790"),
        None,
        None,
        None,
        None,
    ];
    for extension in ["csv", "jsonl"] {
        let path = root.path().join(format!("source.{extension}"));
        let bytes = if extension == "csv" {
            let mut writer = csv::Writer::from_writer(Vec::new());
            writer.write_record(["Timestamp", "Label"]).unwrap();
            for (index, value) in values.iter().enumerate() {
                writer
                    .write_record([value.unwrap_or_default(), &index.to_string()])
                    .unwrap();
            }
            writer.into_inner().unwrap()
        } else {
            values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    json!({"Timestamp":value,"Label":index.to_string()}).to_string() + "\n"
                })
                .collect::<String>()
                .into_bytes()
        };
        std::fs::write(&path, &bytes).unwrap();
        let store = ProjectsStore::new(root.path().join(format!("cases-{extension}"))).unwrap();
        let meta = ops::projects::create_project(
            &store,
            serde_json::from_value(json!({"path":path})).unwrap(),
        )
        .unwrap()
        .meta;
        let column = meta
            .columns
            .iter()
            .find(|column| column.label == "Timestamp")
            .unwrap()
            .id
            .clone();
        let page = ops::rows::query_project_rows(
            &store,
            serde_json::from_value(json!({"projectId":meta.id})).unwrap(),
        )
        .unwrap();
        assert_eq!(page.rows.len(), values.len());
        for (index, row) in page.rows.iter().enumerate() {
            let cell = &row.cells[&column];
            let full = ops::cell::get_cell(
                &store,
                serde_json::from_value(
                    json!({"projectId":meta.id,"rowIndex":index,"columnId":column}),
                )
                .unwrap(),
            )
            .unwrap();
            for wire in [
                serde_json::to_value(cell).unwrap(),
                serde_json::to_value(&full).unwrap(),
            ] {
                assert_eq!(
                    wire.get("datetime_nanos").and_then(Value::as_str),
                    expected[index],
                    "{extension} row {index}"
                );
            }
            if index == 1 {
                assert!(cell.truncated);
                assert!(cell
                    .text
                    .as_deref()
                    .unwrap()
                    .chars()
                    .all(char::is_whitespace));
            }
            assert_eq!(
                full.text.as_deref(),
                if extension == "csv" {
                    Some(values[index].unwrap_or_default())
                } else {
                    values[index]
                }
            );
        }
        let analysis = ops::time::time_analysis(
            &store,
            serde_json::from_value(json!({"projectId":meta.id,"column":column,"asDatetime":true}))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(analysis.sessions[0].duration_nanos, "1");
        assert_eq!(analysis.sessions[0].start, values[0].unwrap());
        assert_eq!(analysis.sessions[0].end, padded);
        assert_eq!((analysis.invalid, analysis.missing), (2, 2));
        let selected = ops::rows::query_project_rows(&store, serde_json::from_value(json!({"projectId":meta.id,"condition":{"column":column,"op":"ge","values":[analysis.sessions[0].end_utc]}})).unwrap()).unwrap();
        assert_eq!(
            selected
                .rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>(),
            [1]
        );
        let output = root.path().join(format!("export-{extension}.csv"));
        ops::export::export_project(
            &store,
            serde_json::from_value(
                json!({"projectId":meta.id,"destination":output,"includeAnnotations":false}),
            )
            .unwrap(),
        )
        .unwrap();
        let exported = csv::Reader::from_path(output)
            .unwrap()
            .records()
            .map(|row| row.unwrap()[0].to_owned())
            .collect::<Vec<_>>();
        assert_eq!(exported, values.map(|value| value.unwrap_or_default()));
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}

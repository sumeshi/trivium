//! Frozen storage and cell DTOs produced by the Polars 0.40 engine remain readable.

use std::{fs, path::PathBuf};

use chrono::Utc;
use polars::prelude::DataFrame;
use serde_json::Value;
use trivium_engine::{
    condition::{Condition, Node, Op},
    models::{ColumnMeta, ProjectMeta},
    ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    project_io::{
        read_page_rows_dataframe, read_project_dataframe, ProjectBatchReader, INTERNAL_ORIGINAL,
        INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD,
    },
    store::ProjectsStore,
    value_utils::CellColumns,
};
use uuid::Uuid;

const SOURCE: &str = include_str!("fixtures/polars040/source.jsonl");

fn assert_rows(frame: &DataFrame, columns: &[ColumnMeta], expected: &Value, rows: &[usize]) {
    assert_eq!(frame.height(), rows.len());
    let originals: Vec<_> = SOURCE.lines().collect();
    let cells = CellColumns::new(frame, columns);
    for (local, &row) in rows.iter().enumerate() {
        assert_eq!(
            frame
                .column(INTERNAL_ROW_ID)
                .unwrap()
                .i64()
                .unwrap()
                .get(local),
            Some(row as i64)
        );
        assert_eq!(
            frame
                .column(INTERNAL_SOURCE_RECORD)
                .unwrap()
                .i64()
                .unwrap()
                .get(local),
            Some(row as i64 + 1)
        );
        assert_eq!(
            frame
                .column(INTERNAL_ORIGINAL)
                .unwrap()
                .str()
                .unwrap()
                .get(local),
            Some(originals[row])
        );
        // Check both the physical strings and their published JSON source types.
        for column in columns {
            assert_eq!(
                frame.column(&column.id).unwrap().str().unwrap().get(local),
                expected["rows"][row]["cells"][&column.id]["text"].as_str(),
                "stored text for {} at row {row}",
                column.id
            );
        }
        assert_eq!(
            serde_json::to_value(cells.build_row(local)).unwrap(),
            expected["rows"][row]["cells"],
            "cell DTOs for row {row}"
        );
    }
}

#[test]
fn polars040_projects_retain_exact_values_through_readers_and_queries() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/polars040/evidence.parquet");
    let original_bytes = fs::read(&fixture).unwrap();
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/polars040/expected.json")).unwrap();
    let columns: Vec<ColumnMeta> = serde_json::from_value(expected["columns"].clone()).unwrap();
    let mut projection: Vec<_> = columns.iter().map(|column| column.id.clone()).collect();
    projection
        .extend([INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD].map(str::to_owned));
    let frame = read_project_dataframe(&fixture).unwrap();
    assert_rows(&frame, &columns, &expected, &[0, 1]);

    // The custom bounded decoder and reordered page reads must match the full reader.
    projection.reverse();
    let mut read_rows = 0;
    for batch in ProjectBatchReader::open(&fixture, &projection).unwrap() {
        let batch = batch.unwrap();
        assert_eq!(batch.offset, read_rows);
        let rows: Vec<_> = (batch.offset..batch.offset + batch.frame.height()).collect();
        assert_rows(&batch.frame, &columns, &expected, &rows);
        read_rows += batch.frame.height();
    }
    assert_eq!(read_rows, 2);
    let order = [1, 0, 1];
    let page = read_page_rows_dataframe(&fixture, &projection, &order).unwrap();
    assert_rows(&page, &columns, &expected, &order);

    // Reopen the frozen bytes as a real project without any storage conversion.
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let id = Uuid::new_v4();
    let directory = store.project_dir(&id);
    fs::create_dir_all(&directory).unwrap();
    let data = directory.join("data.parquet");
    fs::write(&data, &original_bytes).unwrap();
    store
        .insert(ProjectMeta {
            id,
            name: "Polars 0.40 compatibility".into(),
            description: None,
            created_at: Utc::now(),
            total_records: 2,
            flagged_records: 0,
            source_sha256: None,
            source_file_name: None,
            source_format: Some("jsonl".into()),
            source_size: None,
            source_path: None,
            import_report: None,
            hidden_columns: vec![],
            storage_version: 2,
            columns,
        })
        .unwrap();
    let query = |spec| {
        query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: id,
                spec,
                offset: None,
                limit: None,
            },
        )
        .unwrap()
    };
    let all = query(QuerySpec::default());
    assert_eq!(all.total_filtered_rows, 2);
    assert_eq!(serde_json::to_value(all.rows).unwrap(), expected["rows"]);
    for (value, row) in [("18446744073709551615", 0), ("9007199254740993", 1)] {
        let filtered = query(QuerySpec {
            condition: Some(Node::Condition(Condition {
                column: "c000001".into(),
                op: Op::Eq,
                values: vec![value.into()],
                negated: false,
            })),
            ..Default::default()
        });
        assert_eq!(filtered.total_filtered_rows, 1);
        assert_eq!(
            serde_json::to_value(filtered.rows).unwrap(),
            serde_json::json!([expected["rows"][row]])
        );
    }
    assert_eq!(fs::read(&data).unwrap(), original_bytes);
    assert_eq!(fs::read(&fixture).unwrap(), original_bytes);
}

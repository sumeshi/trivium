//! JSON structure and layout must not silently remove fields or valid records.

use std::{collections::HashMap, fs, path::PathBuf};

use serde_json::{json, Value};
use trivium_engine::{
    ops::{
        cell::{get_cell, GetCellPayload},
        export::{export_project, ExportProjectPayload},
        projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    project_io::read_original_records,
    store::ProjectsStore,
};
use uuid::Uuid;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-json-edge-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }

    fn source(&self, name: &str, body: &str) -> CreateProjectPayload {
        let path = self.0.join(name);
        fs::write(&path, body).unwrap();
        CreateProjectPayload {
            path: path.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn nested_empty_objects_survive_import_query_full_cell_reopen_and_csv_export() {
    let records = [
        json!({"context": {}, "wrapper": {"inner": {}}, "array": [], "a/b~": {}}),
        json!({"context": null, "array": [{}]}),
        json!({"context": [], "wrapper": {"inner": null}}),
        json!({}),
        json!({"context": "", "wrapper": {}}),
    ];
    for extension in ["json", "jsonl", "ndjson"] {
        let fixture = Fixture::new();
        let store = ProjectsStore::new(fixture.0.clone()).unwrap();
        let body = if extension == "json" {
            serde_json::to_string(&records).unwrap()
        } else {
            records
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        };
        let payload = fixture.source(&format!("source.{extension}"), &body);
        let source = PathBuf::from(&payload.path);
        let meta = create_project(&store, payload).unwrap().meta;
        let ids: HashMap<_, _> = meta
            .columns
            .iter()
            .map(|column| (column.source.as_str(), column.id.as_str()))
            .collect();
        for source in [
            "/context",
            "/wrapper/inner",
            "/wrapper",
            "/array",
            "/a~1b~0",
        ] {
            assert!(
                ids.contains_key(source),
                "{extension}: missing source field {source}"
            );
        }
        let context = ids["/context"];
        let all = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: meta.id,
                spec: QuerySpec::default(),
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        let expected = [
            (Some("{}"), "object"),
            (None, "null"),
            (Some("[]"), "array"),
            (None, "missing"),
            (Some(""), "string"),
        ];
        for (index, (text, kind)) in expected.into_iter().enumerate() {
            let listed = &all.rows[index].cells[context];
            assert_eq!((listed.text.as_deref(), listed.kind.as_str()), (text, kind));
            let full = get_cell(
                &store,
                GetCellPayload {
                    project_id: meta.id,
                    row_index: index,
                    column_id: context.into(),
                },
            )
            .unwrap();
            assert_eq!((full.text.as_deref(), full.kind.as_str()), (text, kind));
        }
        assert_eq!(
            all.rows[0].cells[ids["/wrapper/inner"]].text.as_deref(),
            Some("{}")
        );
        assert_eq!(
            all.rows[0].cells[ids["/a~1b~0"]].text.as_deref(),
            Some("{}")
        );
        let matches = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: meta.id,
                spec: QuerySpec {
                    search: Some(format!("{context}:{{}}")),
                    ..Default::default()
                },
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        assert_eq!(
            matches
                .rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>(),
            [0]
        );
        assert_eq!(
            read_original_records(&store.project_dir(&meta.id)).unwrap(),
            records
        );

        let destination = fixture.0.join("output.csv");
        export_project(
            &store,
            ExportProjectPayload {
                allow_overwrite: false,
                project_id: meta.id,
                destination: destination.to_string_lossy().into(),
                scope: Some("all".into()),
                spec: QuerySpec::default(),
                columns: None,
                row_indices: None,
                include_annotations: Some(false),
            },
        )
        .unwrap();
        let mut csv = csv::Reader::from_path(destination).unwrap();
        let headers = csv.headers().unwrap().clone();
        let index = headers.iter().position(|label| label == "context").unwrap();
        let rows = csv.records().map(Result::unwrap).collect::<Vec<_>>();
        assert_eq!(
            rows.iter()
                .map(|row| row.get(index).unwrap())
                .collect::<Vec<_>>(),
            ["{}", "", "[]", "", ""]
        );
        let inner = headers
            .iter()
            .position(|label| label == "wrapper/inner")
            .unwrap();
        assert_eq!(rows[0].get(inner), Some("{}"));
        assert_eq!(fs::read_to_string(source).unwrap(), body);
        drop(store);
        let reopened = ProjectsStore::new(fixture.0.clone()).unwrap();
        let loaded = load_project(
            &reopened,
            ProjectRequest {
                project_id: meta.id,
            },
        )
        .unwrap();
        assert_eq!(
            loaded.initial_rows[0].cells[context].text.as_deref(),
            Some("{}")
        );
    }
}

#[test]
fn root_empty_objects_remain_records_without_a_synthetic_data_column() {
    for extension in ["json", "jsonl"] {
        let fixture = Fixture::new();
        let store = ProjectsStore::new(fixture.0.clone()).unwrap();
        let imported =
            create_project(&store, fixture.source(&format!("empty.{extension}"), "{}")).unwrap();
        assert_eq!(imported.meta.total_records, 1);
        assert!(imported.meta.columns.is_empty());
        let loaded = load_project(
            &store,
            ProjectRequest {
                project_id: imported.meta.id,
            },
        )
        .unwrap();
        assert_eq!(loaded.initial_rows.len(), 1);
        assert!(loaded.initial_rows[0].cells.is_empty());
    }
}

#[test]
fn json_layout_detection_handles_whitespace_larger_than_the_read_buffer() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.0.clone()).unwrap();
    for (index, (prefix, document, expected_rows)) in [
        (" ".repeat(9_000), r#"[{"user":"alice"}]"#, 1),
        (
            format!("\u{feff}{}", " \t\r\n".repeat(16_384)),
            r#"[{"user":"alice"},{"user":"bob"}]"#,
            2,
        ),
        ("\n".repeat(16_384), "[]", 0),
        (" ".repeat(9_000), r#"{"user":"alice"}"#, 1),
    ]
    .into_iter()
    .enumerate()
    {
        let body = format!("{prefix}{document}");
        let imported = create_project(
            &store,
            fixture.source(&format!("spaces-{index}.json"), &body),
        )
        .unwrap();
        assert_eq!(imported.meta.total_records, expected_rows);
        assert_eq!(
            read_original_records(&store.project_dir(&imported.meta.id))
                .unwrap()
                .len(),
            expected_rows
        );
    }
}

#[test]
fn malformed_json_after_long_whitespace_reports_source_lines_and_rolls_back() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.0.clone()).unwrap();
    let body = format!("{}[{{\"user\":\"alice\"}}] invalid", "\n".repeat(9_000));
    let error = create_project(&store, fixture.source("invalid.json", &body))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("trailing characters") && error.contains("line 9001"),
        "{error}"
    );
    assert!(store.try_all().unwrap().is_empty());
    assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
    assert_eq!(fs::read_dir(fixture.0.join("projects")).unwrap().count(), 0);
}

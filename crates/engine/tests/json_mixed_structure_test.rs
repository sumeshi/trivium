//! Mixed source types at one object path must keep their actual cell values.
use std::{collections::HashMap, fs, path::PathBuf};

use polars::prelude::{NamedFrom, Series};
use serde_json::json;
use trivium_engine::{
    ops::{
        cell::{get_cell, GetCellPayload},
        export::{export_project, ExportProjectPayload},
        flags::{update_flag, UpdateFlagPayload},
        projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    project_io::{read_original_records, read_project_dataframe, write_project_dataframe},
    store::ProjectsStore,
};
use uuid::Uuid;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-json-mixed-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}

#[test]
fn existing_case_reads_preserve_storage_original_records_and_saved_annotations() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.0.join("workspace")).unwrap();
    let source = fixture.0.join("old.jsonl");
    fs::write(
        &source,
        "{\"context\":{\"host\":\"foo\"}}\n{\"context\":\"bar\"}\n",
    )
    .unwrap();
    let meta = create_project(
        &store,
        CreateProjectPayload {
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap()
    .meta;
    let parent = &meta
        .columns
        .iter()
        .find(|column| column.source == "/context")
        .unwrap()
        .id;
    let path = store.project_dir(&meta.id).join("data.parquet");
    // Reproduce the previous importer representation, retaining __original.
    let mut frame = read_project_dataframe(&path).unwrap();
    frame
        .replace(parent, Series::new(parent, [None, Some("bar")]))
        .unwrap();
    write_project_dataframe(&path, &mut frame).unwrap();
    let original_bytes = fs::read(&path).unwrap();
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: meta.id,
            row_index: 0,
            flag: Some("critical".into()),
            memo: Some(Some("Saved investigation".into())),
            tags: Some(vec!["review".into()]),
            bookmarked: Some(true),
        },
    )
    .unwrap();
    let page = query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: meta.id,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .unwrap();
    // Older mixed-shape cases need reimport for corrected parent-cell search
    // and export. Merely viewing one must not migrate its stored evidence.
    get_cell(
        &store,
        GetCellPayload {
            project_id: meta.id,
            row_index: 0,
            column_id: parent.clone(),
        },
    )
    .unwrap();
    assert_eq!(page.rows[0].flag, "critical");
    assert_eq!(page.rows[0].memo.as_deref(), Some("Saved investigation"));
    assert_eq!(page.rows[0].tags, ["review"]);
    assert!(page.rows[0].bookmarked);
    assert_eq!(
        read_original_records(&store.project_dir(&meta.id)).unwrap(),
        [json!({"context":{"host":"foo"}}), json!({"context":"bar"})]
    );
    assert_eq!(
        fs::read(path).unwrap(),
        original_bytes,
        "viewing an older case must not modify saved evidence"
    );
}

#[test]
fn excessive_parent_duplication_rejects_import_atomically_without_limiting_a_large_leaf() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.0.join("workspace")).unwrap();
    let source = fixture.0.join("parent-budget.jsonl");
    let payload = "x".repeat(trivium_engine::import::jsonl::DEFAULT_BATCH_BYTES / 2 + 1024);
    let body = format!(
        "{{\"a\":0}}\n{{\"a\":{{\"b\":0}}}}\n{}\n",
        json!({"a":{"b":{"payload":payload}}})
    );
    fs::write(&source, &body).unwrap();
    let error = create_project(
        &store,
        CreateProjectPayload {
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("record 3") && error.contains("parent-object") && error.contains("8388608"),
        "{error}"
    );
    assert!(store.try_all().unwrap().is_empty());
    assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
    assert_eq!(fs::read_to_string(&source).unwrap(), body);
    // The pre-existing single-oversized-leaf behavior is not restricted by
    // the additional parent-copy budget.
    let leaf = fixture.0.join("leaf.jsonl");
    let large = "y".repeat(trivium_engine::import::jsonl::DEFAULT_BATCH_BYTES + 1);
    fs::write(&leaf, json!({"leaf":large}).to_string()).unwrap();
    let imported = create_project(
        &store,
        CreateProjectPayload {
            path: leaf.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    assert_eq!(imported.meta.total_records, 1);
}

#[test]
fn exact_large_numbers_keep_their_spelling_and_kind_through_import_page_and_full_cell() {
    let body = r#"{"wide":18446744073709551616,"negative":-170141183460469231731687303715884105729,"decimal":1.000000000000000000001,"exponent":4.2e-400,"text":"18446744073709551616","zero":-0}"#;
    for extension in ["json", "jsonl", "ndjson"] {
        let fixture = Fixture::new();
        let store = ProjectsStore::new(fixture.0.join("workspace")).unwrap();
        let source = fixture.0.join(format!("numbers.{extension}"));
        fs::write(&source, body).unwrap();
        let meta = create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta;
        let page = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: meta.id,
                spec: QuerySpec::default(),
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        for (source, text, kind) in [
            ("/wide", "18446744073709551616", "int"),
            (
                "/negative",
                "-170141183460469231731687303715884105729",
                "int",
            ),
            ("/decimal", "1.000000000000000000001", "float"),
            ("/exponent", "4.2e-400", "float"),
            ("/text", "18446744073709551616", "string"),
            ("/zero", "-0", "int"),
        ] {
            let column = &meta
                .columns
                .iter()
                .find(|column| column.source == source)
                .unwrap()
                .id;
            let cell = &page.rows[0].cells[column];
            assert_eq!(cell.text.as_deref(), Some(text), "{extension} {source}");
            assert_eq!(cell.kind, kind, "{extension} {source}");
            let full = get_cell(
                &store,
                GetCellPayload {
                    project_id: meta.id,
                    row_index: 0,
                    column_id: column.clone(),
                },
            )
            .unwrap();
            assert_eq!(full.text.as_deref(), Some(text));
            assert_eq!(full.kind, kind);
        }
        assert_eq!(fs::read_to_string(source).unwrap(), body);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn mixed_parent_objects_and_arrays_match_in_pages_cells_search_export_and_reopen() {
    let records = [
        json!({"context":{"host":"foo","child":{"value":1}},"a/b~":{"0":"object-key"}}),
        json!({"context":"bar","a/b~":["array-item"]}),
        json!({"context":null}),
        json!({}),
        json!({"context":{},"a/b~":{}}),
        json!({"context":[{"host":"array-host"}]}),
        json!({"context":{"host":"baz"}}),
    ];
    for extension in ["json", "jsonl", "ndjson"] {
        let fixture = Fixture::new();
        let store = ProjectsStore::new(fixture.0.join("workspace")).unwrap();
        let source = fixture.0.join(format!("source.{extension}"));
        let body = if extension == "json" {
            serde_json::to_string(&records).unwrap()
        } else {
            records.iter().map(|record| format!("{record}\n")).collect()
        };
        fs::write(&source, &body).unwrap();
        let meta = create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta;
        let columns: HashMap<_, _> = meta
            .columns
            .iter()
            .map(|column| (column.source.clone(), column.id.clone()))
            .collect();
        let parent = &columns["/context"];
        // Ordinary parent objects still do not introduce their own columns.
        assert!(!columns.contains_key("/context/child"));
        let query = |spec| {
            query_project_rows(
                &store,
                QueryRowsPayload {
                    project_id: meta.id,
                    spec,
                    offset: None,
                    limit: None,
                },
            )
            .unwrap()
        };
        let page = query(QuerySpec::default());
        let expected = [
            (Some(records[0]["context"].to_string()), "object"),
            (Some("bar".into()), "string"),
            (None, "null"),
            (None, "missing"),
            (Some("{}".into()), "object"),
            (Some(records[5]["context"].to_string()), "array"),
            (Some(records[6]["context"].to_string()), "object"),
        ];
        for (index, (text, kind)) in expected.iter().enumerate() {
            let cell = &page.rows[index].cells[parent];
            assert_eq!(
                (&cell.text, cell.kind.as_str()),
                (text, *kind),
                "{extension} row {index}"
            );
            let full = get_cell(
                &store,
                GetCellPayload {
                    project_id: meta.id,
                    row_index: index,
                    column_id: parent.clone(),
                },
            )
            .unwrap();
            assert_eq!((&full.text, full.kind.as_str()), (text, *kind));
        }
        let numeric_child = &columns["/a~1b~0/0"];
        assert_eq!(
            page.rows[0].cells[numeric_child].text.as_deref(),
            Some("object-key")
        );
        assert_eq!(
            page.rows[1].cells[numeric_child].kind, "missing",
            "array indexes are not flattened object keys"
        );
        assert_eq!(
            page.rows[5].cells[&columns["/context/host"]].kind,
            "missing"
        );
        for (needle, ids) in [("foo", vec![0]), ("baz", vec![6]), ("array-host", vec![5])] {
            let filtered = query(QuerySpec {
                search: Some(format!("{parent}:{needle}")),
                ..Default::default()
            });
            assert_eq!(
                filtered
                    .rows
                    .iter()
                    .map(|row| row.row_index)
                    .collect::<Vec<_>>(),
                ids
            );
        }
        let destination = fixture.0.join("output.csv");
        export_project(
            &store,
            ExportProjectPayload {
                project_id: meta.id,
                destination: destination.to_string_lossy().into(),
                allow_overwrite: false,
                scope: Some("all".into()),
                spec: QuerySpec::default(),
                columns: Some(vec![parent.clone(), numeric_child.clone()]),
                row_indices: None,
                include_annotations: Some(false),
            },
        )
        .unwrap();
        let mut reader = csv::Reader::from_path(&destination).unwrap();
        let exported: Vec<_> = reader.records().map(Result::unwrap).collect();
        assert_eq!(
            exported
                .iter()
                .map(|row| row.get(0).unwrap())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(text, _)| text.as_deref().unwrap_or(""))
                .collect::<Vec<_>>()
        );
        assert_eq!(exported[0].get(1), Some("object-key"));
        assert_eq!(exported[1].get(1), Some(""));
        assert_eq!(
            read_original_records(&store.project_dir(&meta.id)).unwrap(),
            records
        );
        assert_eq!(fs::read_to_string(&source).unwrap(), body);
        drop(store);
        let reopened = ProjectsStore::new(fixture.0.join("workspace")).unwrap();
        let loaded = load_project(
            &reopened,
            ProjectRequest {
                project_id: meta.id,
            },
        )
        .unwrap();
        assert_eq!(
            loaded.initial_rows[0].cells[parent].text.as_deref(),
            expected[0].0.as_deref()
        );
        assert_eq!(loaded.initial_rows[0].cells[parent].kind, "object");
    }
}

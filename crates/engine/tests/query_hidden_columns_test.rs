//! Hiding every source column must retain row identities and annotations.
use std::fs::{self, File};

use polars::prelude::*;
use serde_json::json;
use trivium_engine::{
    models::ProjectMeta,
    ops::{
        flags::{set_hidden_columns, update_flag, HiddenColumnsPayload, UpdateFlagPayload},
        projects::{create_project, CreateProjectPayload},
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    store::ProjectsStore,
};
use uuid::Uuid;

fn verify_all_hidden(store: &ProjectsStore, id: Uuid, message: &str, value: &str) {
    update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 1,
            flag: Some("critical".into()),
            memo: Some(Some("investigation note".into())),
            tags: Some(vec!["review".into()]),
            bookmarked: Some(true),
        },
    )
    .unwrap();
    set_hidden_columns(
        store,
        HiddenColumnsPayload {
            project_id: id,
            hidden_columns: vec![message.into(), value.into()],
        },
    )
    .unwrap();
    for (spec, expected) in [
        (QuerySpec::default(), vec![0, 1, 2]),
        (
            QuerySpec {
                search: Some("needle".into()),
                search_columns: Some(vec![message.into()]),
                ..Default::default()
            },
            vec![1],
        ),
        (
            QuerySpec {
                sort_key: Some(value.into()),
                sort_direction: Some("desc".into()),
                ..Default::default()
            },
            vec![2, 1, 0],
        ),
        (
            QuerySpec {
                flag_filter: Some("critical".into()),
                ..Default::default()
            },
            vec![1],
        ),
    ] {
        // Exercise both cold and ready-result paths for filtered/sorted queries.
        for _ in 0..2 {
            let response = query_project_rows(
                store,
                QueryRowsPayload {
                    project_id: id,
                    spec: spec.clone(),
                    offset: Some(0),
                    limit: Some(10),
                },
            )
            .unwrap();
            assert_eq!(response.total_rows, 3);
            assert_eq!(response.total_filtered_rows, expected.len());
            assert_eq!(
                response
                    .rows
                    .iter()
                    .map(|row| row.row_index)
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(response.rows.iter().all(|row| row.cells.is_empty()));
            let annotated = response.rows.iter().find(|row| row.row_index == 1).unwrap();
            assert_eq!(annotated.flag, "critical");
            assert_eq!(annotated.memo.as_deref(), Some("investigation note"));
            assert_eq!(annotated.tags, ["review"]);
            assert!(annotated.bookmarked);
        }
    }
}

#[test]
fn legacy_parquet_without_internal_anchors_can_hide_every_source_column() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let id = Uuid::new_v4();
    let directory = store.project_dir(&id);
    fs::create_dir_all(&directory).unwrap();
    let mut frame = DataFrame::new(vec![
        Series::new("message", ["first", "needle", "last"]),
        Series::new("value", [1_i64, 2, 3]),
    ])
    .unwrap();
    ParquetWriter::new(File::create(directory.join("data.parquet")).unwrap())
        .finish(&mut frame)
        .unwrap();
    let meta: ProjectMeta = serde_json::from_value(json!({
        "id": id, "name": "legacy", "description": null,
        "created_at": "2026-11-19T00:00:00Z", "total_records": 3,
        "flagged_records": 0, "ioc_applied_records": 0, "hidden_columns": [],
        "storage_version": 0, "columns": []
    }))
    .unwrap();
    store.insert(meta).unwrap();
    verify_all_hidden(&store, id, "message", "value");
}

#[test]
fn imported_cases_can_hide_every_column_without_losing_annotation_controls() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let source = root.path().join("source.csv");
    fs::write(&source, "message,value\nfirst,1\nneedle,2\nlast,3\n").unwrap();
    let imported = create_project(
        &store,
        CreateProjectPayload {
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    verify_all_hidden(&store, imported.meta.id, "c000001", "c000002");
}

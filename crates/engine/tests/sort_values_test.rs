//! Sorting changes comparison keys, never the evidence displayed or exported.

use std::fs;

use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::projects::{create_project, CreateProjectPayload};
use trivium_engine::ops::rows::{
    locate_project_row, query_project_rows, LocateRowPayload, QueryRowsPayload, QuerySpec,
};
use trivium_engine::store::ProjectsStore;
use uuid::Uuid;

mod common;

#[test]
fn internal_sort_ids_ignore_case_across_pages_location_and_export() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("values.csv");
    fs::write(&source, "value\n2\n1\n1\n").unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let project = create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: source.to_string_lossy().into_owned(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap()
    .meta
    .id;
    let column = common::id(&common::column_ids(&store, project), "value");
    let uppercase = column.to_ascii_uppercase();
    assert_ne!(uppercase, column);
    for (direction, expected, first_position) in
        [("asc", vec![1, 2, 0], 2), ("desc", vec![0, 1, 2], 0)]
    {
        let spec = QuerySpec {
            sort_key: Some(uppercase.clone()),
            sort_direction: Some(direction.into()),
            ..Default::default()
        };
        let page = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: project,
                spec: spec.clone(),
                offset: None,
                limit: Some(100),
            },
        )
        .unwrap();
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>(),
            expected,
            "{direction}: uppercase internal ID must retain the requested sort"
        );
        let middle = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: project,
                spec: spec.clone(),
                offset: Some(1),
                limit: Some(1),
            },
        )
        .unwrap();
        assert_eq!(middle.rows.len(), 1);
        assert_eq!(middle.rows[0].row_index, expected[1]);
        let location = locate_project_row(
            &store,
            LocateRowPayload {
                project_id: project,
                row_index: 0,
                spec: spec.clone(),
            },
        )
        .unwrap();
        assert_eq!(location.position, Some(first_position));
        assert_eq!(location.total_filtered_rows, 3);

        let destination = root.path().join(format!("uppercase-{direction}.csv"));
        export_project(
            &store,
            ExportProjectPayload {
                allow_overwrite: false,
                project_id: project,
                destination: destination.to_string_lossy().into_owned(),
                scope: Some("filtered".into()),
                spec,
                columns: Some(vec![column.clone()]),
                row_indices: None,
                include_annotations: Some(false),
            },
        )
        .unwrap();
        let values = ["2", "1", "1"];
        let exported = csv::Reader::from_path(destination)
            .unwrap()
            .records()
            .map(|record| record.unwrap().get(0).unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            exported,
            expected.iter().map(|&row| values[row]).collect::<Vec<_>>()
        );
    }
}

#[test]
fn exact_numeric_and_datetime_sort_survive_reopen_and_export_original_spellings() {
    let root = std::env::temp_dir().join(format!("trivium-sort-values-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    for (name, values, ascending, descending) in [
        (
            "datetime",
            vec![
                "2026-11-14T01:00:00Z",
                "2026-11-14T09:00:00+09:00",
                "2026/11/14 00:00:00.000000001",
                "2026-11-14T00:00:00.000000000Z",
            ],
            vec![1, 3, 2, 0],
            vec![0, 2, 1, 3],
        ),
        (
            "numeric",
            vec![
                "10.5",
                "2.5",
                "9007199254740993.000000000000000001",
                "9007199254740993.000000000000000000",
                "170141183460469231731687303715884105729",
                "170141183460469231731687303715884105728",
                "2.50e0",
            ],
            vec![1, 6, 0, 3, 2, 5, 4],
            vec![4, 5, 2, 3, 0, 1, 6],
        ),
    ] {
        let source = root.join(format!("{name}.csv"));
        let source_bytes = format!("value\n{}\n", values.join("\n"));
        fs::write(&source, &source_bytes).unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let project = create_project(
            &store,
            CreateProjectPayload {
                import_options: None,
                path: source.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        let column = common::id(&common::column_ids(&store, project), "value");
        drop(store);

        let store = ProjectsStore::new(root.clone()).unwrap();
        for (direction, expected) in [("asc", ascending), ("desc", descending)] {
            let spec = QuerySpec {
                sort_key: Some(column.clone()),
                sort_direction: Some(direction.into()),
                ..Default::default()
            };
            let page = query_project_rows(
                &store,
                QueryRowsPayload {
                    project_id: project,
                    spec: spec.clone(),
                    offset: None,
                    limit: Some(100),
                },
            )
            .unwrap();
            assert_eq!(
                page.rows
                    .iter()
                    .map(|row| row.row_index)
                    .collect::<Vec<_>>(),
                expected,
                "{name} {direction}"
            );
            let original_values: Vec<_> = expected.iter().map(|&index| values[index]).collect();
            assert_eq!(
                page.rows
                    .iter()
                    .map(|row| common::cell_text(row, &column).unwrap())
                    .collect::<Vec<_>>(),
                original_values
            );
            let destination = root.join(format!("{name}-{direction}-export.csv"));
            export_project(
                &store,
                ExportProjectPayload {
                    allow_overwrite: false,
                    project_id: project,
                    destination: destination.to_string_lossy().into_owned(),
                    scope: Some("filtered".into()),
                    spec,
                    columns: Some(vec![column.clone()]),
                    row_indices: None,
                    include_annotations: Some(false),
                },
            )
            .unwrap();
            let mut reader = csv::Reader::from_path(&destination).unwrap();
            assert_eq!(
                reader.headers().unwrap().iter().collect::<Vec<_>>(),
                ["value"]
            );
            let exported: Vec<_> = reader
                .records()
                .map(|record| record.unwrap().get(0).unwrap().to_owned())
                .collect();
            assert_eq!(exported, original_values);
        }
        assert_eq!(fs::read_to_string(&source).unwrap(), source_bytes);
    }
    fs::remove_dir_all(root).unwrap();
}

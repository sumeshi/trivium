//! Unscoped conditions keep the original selected text for exact and regex ops.

use std::fs;

use trivium_engine::{
    annotation::AnnotationPatch,
    condition::{Condition, Node, Op},
    ops::{
        bulk::{bulk_preview, BulkAnnotatePayload},
        export::{export_project, ExportProjectPayload},
        projects::{create_project, CreateProjectPayload},
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    store::ProjectsStore,
};

mod common;

#[test]
fn unscoped_exact_and_regex_conditions_preserve_case_across_query_export_and_bulk() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("case.csv");
    fs::write(
        &source,
        "id,value,ignored\n0,Alpha,Noise\n1,alpha,Alpha\n2,Other,Alpha\n",
    )
    .unwrap();
    let store = ProjectsStore::new(root.path().join("projects")).unwrap();
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
    let columns = common::column_ids(&store, project);
    let value = common::id(&columns, "value");
    let id = common::id(&columns, "id");

    for (index, (op, text, negated, expected)) in [
        (Op::Eq, "Alpha", false, vec![0]),
        (Op::Ne, "Alpha", false, vec![1, 2]),
        (Op::In, "Alpha", false, vec![0]),
        (Op::Eq, "Alpha", true, vec![1, 2]),
        (Op::Regex, "^Alpha$", false, vec![0]),
        (Op::Regex, "^Alpha$", true, vec![1, 2]),
        (Op::Regex, "(?i)^Alpha$", false, vec![0, 1]),
        (Op::Contains, "ALPHA", false, vec![0, 1]),
    ]
    .into_iter()
    .enumerate()
    {
        let spec = QuerySpec {
            search_columns: Some(vec![value.clone()]),
            condition: Some(Node::Condition(Condition {
                column: String::new(),
                op,
                values: vec![text.into()],
                negated,
            })),
            ..Default::default()
        };
        let query = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: project,
                spec: spec.clone(),
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        assert_eq!(
            query
                .rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>(),
            expected,
            "{op:?} {text:?}, negated={negated}: original case and selected scope"
        );
        assert_eq!(query.total_filtered_rows, expected.len());

        let destination = root.path().join(format!("case-{index}.csv"));
        let exported = export_project(
            &store,
            ExportProjectPayload {
                allow_overwrite: false,
                project_id: project,
                destination: destination.to_string_lossy().into_owned(),
                scope: Some("filtered".into()),
                spec: spec.clone(),
                columns: Some(vec![id.clone()]),
                row_indices: None,
                include_annotations: Some(false),
            },
        )
        .unwrap();
        assert_eq!(exported.rows, expected.len());
        let exported_ids = csv::Reader::from_path(destination)
            .unwrap()
            .records()
            .map(|record| record.unwrap()[0].parse::<usize>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(exported_ids, expected);

        let bulk = bulk_preview(
            &store,
            BulkAnnotatePayload {
                project_id: project,
                target: "filtered".into(),
                row_indices: None,
                preview_token: None,
                spec,
                patch: AnnotationPatch {
                    flag: Some("critical".into()),
                    ..Default::default()
                },
            },
        )
        .unwrap();
        assert_eq!(bulk.count, expected.len());
    }
}

#[test]
fn joined_unicode_text_keeps_case_and_empty_cells_keep_existing_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("unicode.csv");
    fs::write(
        &source,
        "id,left,empty,right\n0,Älpha,,Ω\n1,älpha,,ω\n2,,,Älpha Ω\n3,Other,,Älpha Ω\n",
    )
    .unwrap();
    let store = ProjectsStore::new(root.path().join("projects")).unwrap();
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
    let columns = common::column_ids(&store, project);
    let scope = ["left", "empty", "right"]
        .map(|source| common::id(&columns, source))
        .to_vec();
    for (op, text, expected) in [
        (Op::Eq, "Älpha Ω", vec![0, 2]),
        (Op::Regex, "^Älpha Ω$", vec![0, 2]),
        (Op::Regex, "(?i)^älpha ω$", vec![0, 1, 2]),
        // Contains still searches individual cells, so it does not cross the
        // left/right boundary in row 0 or row 1.
        (Op::Contains, "ÄLPHA Ω", vec![2, 3]),
        (Op::StartsWith, "ÄLPHA", vec![0, 1, 2]),
        (Op::EndsWith, "ω", vec![0, 1, 2, 3]),
    ] {
        let query = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: project,
                spec: QuerySpec {
                    search_columns: Some(scope.clone()),
                    condition: Some(Node::Condition(Condition {
                        column: String::new(),
                        op,
                        values: vec![text.into()],
                        negated: false,
                    })),
                    ..Default::default()
                },
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        assert_eq!(
            query
                .rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>(),
            expected,
            "{op:?} {text:?}"
        );
    }
}

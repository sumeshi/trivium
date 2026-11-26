//! Flexible spellings change comparison keys, never stored or exported evidence.
use std::fs;

use serde_json::json;
use trivium_engine::{
    condition::{Condition, Node, Op},
    ops::{
        self,
        projects::{CreateProjectPayload, ProjectRequest},
        rows::{QueryRowsPayload, QuerySpec},
    },
    storage::load_flags,
    store::ProjectsStore,
};

mod common;

#[test]
fn flexible_dates_share_sort_filter_analysis_bulk_and_export_instants() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flexible.jsonl");
    let values = [
        "2026/11/14 12:4:5,123456790 +0900",
        "Sat, 14 Nov 2026 3:4:5,123456789 GMT",
        "2026.11.14t3:4:5.123456789Z",
        "2026-11-14T3:4:5.123456788UTC",
        "2026-11-14 3:4 UTC",
        "2026/11/14 3:4:5,123456789Z",
    ];
    let source = values
        .iter()
        .map(|text| json!({"Timestamp":text}).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&path, &source).unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let project = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().into_owned(),
            description: None,
            delimiter: None,
            restore_annotations: None,
            import_options: None,
        },
    )
    .unwrap();
    let id = project.meta.id;
    let column = project.meta.columns[0].id.clone();
    let loaded = ops::projects::load_project(&store, ProjectRequest { project_id: id }).unwrap();
    assert_eq!(loaded.column_kinds[&column], "datetime");
    let spec = QuerySpec {
        sort_key: Some(column.clone()),
        sort_direction: Some("asc".into()),
        ..Default::default()
    };
    let page = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: spec.clone(),
            offset: None,
            limit: None,
        },
    )
    .unwrap();
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        [4, 3, 1, 2, 5, 0]
    );
    let bound = "2026-11-14T03:04:05.123456789Z";
    let condition = Node::Condition(Condition {
        column: column.clone(),
        op: Op::Ge,
        values: vec![bound.into()],
        negated: false,
    });
    let filtered = QuerySpec {
        condition: Some(condition.clone()),
        ..spec
    };
    let matched = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: filtered.clone(),
            offset: None,
            limit: None,
        },
    )
    .unwrap();
    let indices = [1, 2, 5, 0];
    assert_eq!(
        matched
            .rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        indices
    );
    assert_eq!(
        matched
            .rows
            .iter()
            .map(|row| common::cell_text(row, &column).unwrap())
            .collect::<Vec<_>>(),
        indices.map(|index| values[index])
    );

    // Equality deliberately keeps exact source-text semantics despite equal instants.
    let exact = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            offset: None,
            limit: None,
            spec: QuerySpec {
                condition: Some(Node::Condition(Condition {
                    column: column.clone(),
                    op: Op::Eq,
                    values: vec![values[1].into()],
                    negated: false,
                })),
                ..Default::default()
            },
        },
    )
    .unwrap();
    assert_eq!(exact.rows.len(), 1);
    assert_eq!(exact.rows[0].row_index, 1);

    let params = json!({"projectId":id,"column":column,"condition":condition,"buckets":3});
    let histogram =
        ops::histogram::histogram(&store, serde_json::from_value(params.clone()).unwrap()).unwrap();
    assert_eq!(
        (histogram.invalid, histogram.missing, histogram.outside),
        (0, 0, 0)
    );
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|bucket| bucket.count)
            .sum::<u64>(),
        4
    );
    let range = histogram.full_range.unwrap();
    assert_eq!(range.min, "2026-11-14T03:04:05.123456789");
    assert_eq!(range.max, "2026-11-14T03:04:05.123456790");
    let time = ops::time::time_analysis(&store, serde_json::from_value(params).unwrap()).unwrap();
    assert_eq!((time.invalid, time.missing), (0, 0));
    assert_eq!(
        time.sessions
            .iter()
            .map(|session| session.count)
            .sum::<u64>(),
        4
    );
    let grouped = ops::group::group_tree(
        &store,
        serde_json::from_value(json!({
            "projectId":id,"groupColumns":[column],"groupGranularities":["day"]
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(grouped.nodes.len(), 1);
    assert_eq!(grouped.nodes[0].count, values.len());
    let mut bulk =
        json!({"projectId":id,"target":"filtered","condition":condition,"flag":"critical"});
    let preview =
        ops::bulk::bulk_preview(&store, serde_json::from_value(bulk.clone()).unwrap()).unwrap();
    assert_eq!(preview.count, 4);
    bulk["previewToken"] = json!(preview.preview_token);
    assert_eq!(
        ops::bulk::bulk_update(&store, serde_json::from_value(bulk).unwrap())
            .unwrap()
            .updated,
        4
    );
    let mut flagged = load_flags(&store.flags_path(&id))
        .unwrap()
        .into_keys()
        .collect::<Vec<_>>();
    flagged.sort_unstable();
    assert_eq!(flagged, [0, 1, 2, 5]);
    let destination = root.path().join("filtered.csv");
    ops::export::export_project(
        &store,
        ops::export::ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().into_owned(),
            allow_overwrite: false,
            scope: Some("filtered".into()),
            spec: filtered,
            columns: Some(vec![column]),
            row_indices: None,
            include_annotations: Some(false),
        },
    )
    .unwrap();
    let mut reader = csv::Reader::from_path(&destination).unwrap();
    let exported = reader
        .records()
        .map(|row| row.unwrap()[0].to_owned())
        .collect::<Vec<_>>();
    assert_eq!(exported, indices.map(|index| values[index]));
    assert_eq!(fs::read_to_string(path).unwrap(), source);
}

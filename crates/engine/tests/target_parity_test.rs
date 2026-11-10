//! T04: the page query, filtered export, and bulk preview resolve the same
//! original row ids in the same display order.
//!
//! The fixture keeps the search term in a column that is not exported. This is
//! the regression for "evaluating the condition after narrowing output
//! columns": restricting output columns must not change the selected rows.

use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use trivium_engine::annotation::AnnotationPatch;
use trivium_engine::condition::{Condition, Node, Op};
use trivium_engine::ops;
use trivium_engine::ops::bulk::{bulk_preview, bulk_update, BulkAnnotatePayload};
use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::storage::load_flags;
use trivium_engine::store::ProjectsStore;

mod common;

fn setup() -> (PathBuf, ProjectsStore, Uuid, std::collections::HashMap<String, String>) {
    let root = std::env::temp_dir().join(format!("trivium-target-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let csv = root.join("targets.csv");
    let mut body = String::from("id,hidden,cond,rank,msg\n");
    for index in 0..20 {
        let hidden = if index % 2 == 0 { "needle" } else { "other" };
        let cond = if index % 3 == 0 { "keep" } else { "drop" };
        body.push_str(&format!(
            "r{index:02},{hidden},{cond},{index},m{index:02}\n"
        ));
    }
    fs::write(&csv, body).expect("failed to write fixture");

    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id;

    // 0 and 6 survive search + condition + priority + bookmark; 12 is
    // bookmarked but only "safe" and 18 has no flag/bookmark.
    for (row, flag, bookmarked) in [
        (0usize, "suspicious", true),
        (6, "critical", true),
        (12, "safe", true),
        (18, "", false),
    ] {
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index: row,
                flag: Some(flag.to_string()),
                memo: None,
                tags: None,
                bookmarked: Some(bookmarked),
            },
        )
        .expect("failed to annotate row");
    }

    let ids = common::column_ids(&store, id);
    (root, store, id, ids)
}

fn spec(ids: &std::collections::HashMap<String, String>) -> QuerySpec {
    QuerySpec {
        search: Some("needle".to_string()),
        // The search column is deliberately absent from `columns` below.
        search_columns: Some(vec![common::id(ids, "hidden")]),
        condition: Some(Node::Condition(Condition {
            column: common::id(ids, "cond"),
            op: Op::Eq,
            values: vec!["keep".to_string()],
            negated: false,
        })),
        flag_filter: Some("priority".to_string()),
        bookmarked_only: Some(true),
        sort_key: Some(common::id(ids, "rank")),
        sort_direction: Some("desc".to_string()),
    }
}

fn query_ids(store: &ProjectsStore, id: Uuid, spec: QuerySpec) -> Vec<usize> {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec,
            offset: None,
            limit: Some(100),
        },
    )
    .expect("query failed")
    .rows
    .iter()
    .map(|row| row.row_index)
    .collect()
}

fn export_column(
    store: &ProjectsStore,
    root: &std::path::Path,
    project_id: Uuid,
    name: &str,
    scope: &str,
    spec: QuerySpec,
    columns: Vec<String>,
) -> Vec<String> {
    let destination = root.join(format!("export-{name}.csv"));
    export_project(
        store,
        ExportProjectPayload {
            project_id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some(scope.to_string()),
            spec,
            columns: Some(columns),
        },
    )
    .expect("export failed");

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&destination)
        .expect("failed to open export");
    let headers = reader.headers().expect("missing header").clone();
    let column = headers
        .iter()
        .position(|header| header == name)
        .expect("missing requested export column");
    reader
        .records()
        .map(|record| {
            record
                .expect("failed to read exported record")
                .get(column)
                .expect("missing value")
                .to_string()
        })
        .collect()
}

fn bulk_payload(id: Uuid, spec: QuerySpec) -> BulkAnnotatePayload {
    BulkAnnotatePayload {
        project_id: id,
        target: "filtered".to_string(),
        row_indices: None,
        spec,
        patch: AnnotationPatch {
            flag: Some("safe".to_string()),
            ..Default::default()
        },
        preview_token: None,
    }
}

#[test]
fn query_export_and_bulk_preview_share_target_rows_and_order() {
    let (root, store, id, ids) = setup();

    let queried = query_ids(&store, id, spec(&ids));
    assert_eq!(queried, vec![6, 0]);

    let exported_ids = export_column(
        &store,
        &root,
        id,
        "id",
        "filtered",
        spec(&ids),
        vec![common::id(&ids, "id"), common::id(&ids, "msg")],
    );
    assert_eq!(exported_ids, vec!["r06".to_string(), "r00".to_string()]);

    // The search term lives in `hidden`; narrowing the output to `msg` alone
    // must not change the selected rows or their order.
    let exported_msgs = export_column(
        &store,
        &root,
        id,
        "msg",
        "filtered",
        spec(&ids),
        vec![common::id(&ids, "msg")],
    );
    assert_eq!(exported_msgs, vec!["m06".to_string(), "m00".to_string()]);

    let preview = bulk_preview(&store, bulk_payload(id, spec(&ids))).expect("preview failed");
    assert_eq!(preview.count, queried.len());
    assert!(!preview.preview_token.is_empty());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_update_requires_the_preview_token_and_applies_only_that_target() {
    let (root, store, id, ids) = setup();

    let preview = bulk_preview(&store, bulk_payload(id, spec(&ids))).expect("preview failed");
    assert_eq!(preview.count, 2);

    let missing = bulk_update(&store, bulk_payload(id, spec(&ids)))
        .expect_err("update without a preview token must fail");
    assert!(
        missing.to_string().contains("Target changed; preview again."),
        "unexpected error: {missing}"
    );

    let mut stale = bulk_payload(id, spec(&ids));
    stale.preview_token = Some("not-the-token".to_string());
    let mismatch = bulk_update(&store, stale).expect_err("stale token must fail");
    assert!(
        mismatch.to_string().contains("Target changed; preview again."),
        "unexpected error: {mismatch}"
    );

    // A changed condition must invalidate the token even when the target row
    // count happens to stay the same.
    let mut changed_spec = spec(&ids);
    changed_spec.search = Some("other".to_string());
    let mut changed = bulk_payload(id, changed_spec);
    changed.preview_token = Some(preview.preview_token.clone());
    let changed_error = bulk_update(&store, changed).expect_err("changed spec must fail");
    assert!(
        changed_error
            .to_string()
            .contains("Target changed; preview again."),
        "unexpected error: {changed_error}"
    );

    let mut current = bulk_payload(id, spec(&ids));
    current.preview_token = Some(preview.preview_token);
    let applied = bulk_update(&store, current).expect("matching token must apply");
    assert_eq!(applied.updated, 2);

    let flags = load_flags(&store.project_dir(&id).join("flags.json")).expect("load flags");
    assert_eq!(flags.get(&0).map(|entry| entry.flag.as_str()), Some("safe"));
    assert_eq!(flags.get(&6).map(|entry| entry.flag.as_str()), Some("safe"));
    assert_eq!(
        flags.get(&12).map(|entry| entry.flag.as_str()),
        Some("safe"),
        "an unselected annotated row must not change"
    );
    assert!(
        flags.get(&1).is_none(),
        "an unselected unannotated row must stay untouched"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_preview_token_detects_annotation_changes_in_the_target() {
    let (root, store, id, ids) = setup();
    let open_spec = QuerySpec {
        search: Some("needle".to_string()),
        search_columns: Some(vec![common::id(&ids, "hidden")]),
        ..Default::default()
    };

    let preview = bulk_preview(&store, bulk_payload(id, open_spec.clone())).expect("preview failed");
    assert_eq!(preview.count, 10);

    // Clearing row 12's flag does not remove it from this target, but it does
    // change the annotated count in the target.
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 12,
            flag: Some(String::new()),
            memo: None,
            tags: None,
            bookmarked: None,
        },
    )
    .expect("failed to clear flag");

    let mut stale = bulk_payload(id, open_spec);
    stale.preview_token = Some(preview.preview_token);
    let error = bulk_update(&store, stale).expect_err("annotation change must invalidate the token");
    assert!(
        error.to_string().contains("Target changed; preview again."),
        "unexpected error: {error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn scope_all_applies_the_requested_sort_order() {
    let (root, store, id, ids) = setup();
    let all_spec = QuerySpec {
        search: Some("does-not-match".to_string()),
        sort_key: Some(common::id(&ids, "rank")),
        sort_direction: Some("desc".to_string()),
        ..Default::default()
    };

    let ids = export_column(
        &store,
        &root,
        id,
        "id",
        "all",
        all_spec,
        vec![common::id(&ids, "id")],
    );
    assert_eq!(ids.len(), 20, "scope=all exports every row");
    assert_eq!(ids.first().map(String::as_str), Some("r19"));
    assert_eq!(ids.last().map(String::as_str), Some("r00"));

    let _ = fs::remove_dir_all(root);
}

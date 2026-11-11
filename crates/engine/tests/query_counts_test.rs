//! T03: QuerySpec search scope and count meanings.
//!
//! The search scope (`searchColumns`) is independent from visible columns, and
//! the four counts keep whole-project and result-set meanings separate.

use std::fs;

use uuid::Uuid;

use trivium_engine::ops;
use trivium_engine::ops::flags::UpdateFlagPayload;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::store::ProjectsStore;

mod common;

const CSV: &str = "name,detail\n\
                   uniquehit,alpha\n\
                   b,beta\n\
                   c,gamma\n\
                   d,delta\n\
                   e,epsilon\n\
                   f,zeta\n\
                   g,eta\n\
                   h,theta\n\
                   i,iota\n\
                   j,kappa\n";

fn setup_project() -> (std::path::PathBuf, ProjectsStore, Uuid, std::collections::HashMap<String, String>) {
    let root = std::env::temp_dir().join(format!("trivium-counts-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let csv = root.join("counts.csv");
    fs::write(&csv, CSV).expect("failed to write fixture");
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
    let ids = common::column_ids(&store, id);
    (root, store, id, ids)
}

fn annotate(store: &ProjectsStore, id: Uuid, row_index: usize, flag: &str) {
    ops::flags::update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index,
            flag: Some(flag.to_string()),
            memo: None,
            tags: None,
            bookmarked: None,
        },
    )
    .expect("failed to annotate row");
}

fn query(
    store: &ProjectsStore,
    id: Uuid,
    search: Option<&str>,
    search_columns: Option<Vec<String>>,
) -> ops::rows::QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec {
                search: search.map(str::to_string),
                search_columns,
                flag_filter: None,
                bookmarked_only: None,
                sort_key: None,
                sort_direction: None,
                condition: None,
            },
            offset: None,
            limit: None,
        },
    )
    .expect("query failed")
}

#[test]
fn payload_accepts_camel_case_search_columns() {
    let id = Uuid::new_v4();
    let json = format!(r#"{{"projectId":"{id}","searchColumns":["name"]}}"#);
    let parsed: QueryRowsPayload = serde_json::from_str(&json).expect("payload should parse");
    assert_eq!(parsed.spec.search_columns, Some(vec!["name".to_string()]));
}

#[test]
fn whole_project_and_result_annotation_counts_are_distinct() {
    let (root, store, id, _ids) = setup_project();
    annotate(&store, id, 0, "safe");
    annotate(&store, id, 1, "suspicious");
    annotate(&store, id, 2, "critical");

    let result = query(&store, id, Some("uniquehit"), None);
    assert_eq!(result.total_rows, 10);
    assert_eq!(result.total_filtered_rows, 1);
    assert_eq!(result.annotated_total, 3);
    assert_eq!(result.annotated_filtered, 1);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].row_index, 0);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn default_and_explicit_all_column_search_match() {
    let (root, store, id, ids) = setup_project();
    let all_columns = vec![common::id(&ids, "name"), common::id(&ids, "detail")];

    let default_scope = query(&store, id, Some("alpha"), None);
    let explicit_scope = query(&store, id, Some("alpha"), Some(all_columns.clone()));
    let empty_scope = query(&store, id, Some("alpha"), Some(Vec::new()));

    assert_eq!(default_scope.total_filtered_rows, 1);
    assert_eq!(explicit_scope.total_filtered_rows, 1);
    assert_eq!(empty_scope.total_filtered_rows, 1);
    assert_eq!(
        default_scope.rows[0].row_index,
        explicit_scope.rows[0].row_index
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn explicit_search_columns_narrow_the_scope() {
    let (root, store, id, ids) = setup_project();

    // "alpha" only appears in `detail`.
    let narrowed = query(
        &store,
        id,
        Some("alpha"),
        Some(vec![common::id(&ids, "name")]),
    );
    assert_eq!(narrowed.total_filtered_rows, 0);

    // The same value is found when `detail` is inside the scope.
    let widened = query(
        &store,
        id,
        Some("alpha"),
        Some(vec![common::id(&ids, "name"), common::id(&ids, "detail")]),
    );
    assert_eq!(widened.total_filtered_rows, 1);

    let _ = fs::remove_dir_all(root);
}

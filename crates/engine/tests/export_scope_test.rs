//! T23: export scopes, annotation columns, and atomic destination handling.

use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::jobs::{request_cancel, try_begin_job, JobContext, JobKind};
use trivium_engine::ops::export::{
    export_project, export_project_with_job, ExportProjectPayload, ExportResult,
};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::projects::{self, CreateProjectPayload};
use trivium_engine::ops::rows;
use trivium_engine::ops::rows::{QueryRowsPayload, QuerySpec};
use trivium_engine::store::ProjectsStore;

mod common;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-export-scope-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn create(store: &ProjectsStore, root: &Path) -> Uuid {
    let path = root.join("events.csv");
    let text = "id,msg\n\
                0,alpha\n\
                1,beta\n\
                2,alpha\n\
                3,gamma\n\
                4,alpha\n";
    fs::write(&path, text).expect("failed to write fixture");
    projects::create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id
}

fn search_spec(id_column: String) -> QuerySpec {
    QuerySpec {
        search: Some("alpha".to_string()),
        search_columns: None,
        condition: None,
        flag_filter: None,
        bookmarked_only: None,
        sort_key: Some(id_column),
        sort_direction: Some("desc".to_string()),
    }
}

fn export_rows(store: &ProjectsStore, payload: ExportProjectPayload) -> ExportResult {
    export_project(store, payload).expect("export failed")
}

fn read_csv(path: &Path) -> (Vec<String>, Vec<Vec<String>>) {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .expect("failed to open export");
    let headers = reader
        .headers()
        .expect("failed to read header")
        .iter()
        .map(str::to_string)
        .collect();
    let rows = reader
        .records()
        .map(|record| {
            record
                .expect("failed to read record")
                .iter()
                .map(str::to_string)
                .collect()
        })
        .collect();
    (headers, rows)
}

fn column_index(headers: &[String], name: &str) -> usize {
    headers
        .iter()
        .position(|header| header == name)
        .unwrap_or_else(|| panic!("column not found: {name}"))
}

#[test]
fn selected_scope_uses_query_order_and_drops_duplicates() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);
    let ids = common::column_ids(&store, id);
    let id_column = common::id(&ids, "id");

    let query = rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: search_spec(id_column.clone()),
            offset: None,
            limit: None,
        },
    )
    .expect("query failed");
    let result_order: Vec<usize> = query.rows.iter().map(|row| row.row_index).collect();
    assert_eq!(result_order, vec![4, 2, 0]);

    let destination = root.join("selected.csv");
    let result = export_rows(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("selected".to_string()),
            spec: search_spec(id_column.clone()),
            columns: Some(vec![id_column]),
            row_indices: Some(vec![2, 0, 2]),
            include_annotations: None,
        },
    );
    let (headers, exported) = read_csv(&destination);
    let index = column_index(&headers, "id");
    let exported_ids: Vec<&str> = exported.iter().map(|row| row[index].as_str()).collect();
    assert_eq!(exported_ids, vec!["2", "0"]);
    assert_eq!(result.rows, 2);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn empty_selection_is_an_error_and_never_falls_back() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);

    for row_indices in [Some(Vec::new()), None] {
        let destination = root.join("no-selection.csv");
        let error = export_project(
            &store,
            ExportProjectPayload {
                project_id: id,
                destination: destination.to_string_lossy().to_string(),
                scope: Some("selected".to_string()),
                spec: QuerySpec::default(),
                columns: None,
                row_indices,
                include_annotations: None,
            },
        )
        .expect_err("empty selection must not export all rows");
        assert!(
            error
                .to_string()
                .contains("Selected export requires row indices")
                || error.to_string().contains("Select at least one row"),
            "unexpected error: {error}"
        );
        assert!(!destination.exists(), "no file may be written");
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn filtered_scope_matches_query_row_ids_order_and_count() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);
    let ids = common::column_ids(&store, id);
    let id_column = common::id(&ids, "id");

    let query = rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: search_spec(id_column.clone()),
            offset: None,
            limit: None,
        },
    )
    .expect("query failed");
    let expected: Vec<String> = query
        .rows
        .iter()
        .map(|row| row.row_index.to_string())
        .collect();

    let destination = root.join("filtered.csv");
    let result = export_rows(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("filtered".to_string()),
            spec: search_spec(id_column.clone()),
            columns: Some(vec![id_column]),
            row_indices: None,
            include_annotations: None,
        },
    );
    let (headers, exported) = read_csv(&destination);
    let index = column_index(&headers, "id");
    let exported_ids: Vec<String> = exported.iter().map(|row| row[index].clone()).collect();
    assert_eq!(exported_ids, expected);
    assert_eq!(result.rows, query.total_filtered_rows);
    assert_eq!(exported.len(), query.total_filtered_rows);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn annotation_columns_are_independent_and_optional() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: Some("safe".to_string()),
            memo: Some(Some("note".to_string())),
            tags: Some(vec!["alpha".to_string(), "beta".to_string()]),
            bookmarked: Some(true),
        },
    )
    .expect("annotation failed");

    let without = root.join("without-annotations.csv");
    export_rows(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: without.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: Some(false),
        },
    );
    let (headers, _) = read_csv(&without);
    assert!(
        headers.iter().all(|header| !header.starts_with("trivium-")),
        "include_annotations=false must not add trivium columns: {headers:?}"
    );

    let with = root.join("with-annotations.csv");
    export_rows(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: with.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: Some(true),
        },
    );
    let (headers, exported) = read_csv(&with);
    let memo = column_index(&headers, "trivium-memo");
    let tags = column_index(&headers, "trivium-tags");
    let bookmarked = column_index(&headers, "trivium-bookmarked");
    assert_eq!(
        exported[0][memo], "note",
        "tags must not be copied into the memo"
    );
    assert_eq!(exported[0][tags], "alpha;beta");
    assert_eq!(exported[0][bookmarked], "1");
    assert_eq!(exported[1][tags], "");
    assert_eq!(exported[1][bookmarked], "0");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn data_column_with_trivium_name_renames_the_annotation_column() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = root.join("collision.csv");
    let text = "id,msg,trivium-tags\n\
                0,alpha,source-a\n\
                1,beta,source-b\n";
    fs::write(&path, text).expect("failed to write fixture");
    let id = projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id;
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: None,
            memo: None,
            tags: Some(vec!["user-tag".to_string()]),
            bookmarked: None,
        },
    )
    .expect("annotation failed");

    let destination = root.join("collision-export.csv");
    let result = export_rows(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: Some(true),
        },
    );
    let (headers, exported) = read_csv(&destination);
    let source = column_index(&headers, "trivium-tags");
    let added = column_index(&headers, "trivium-tags_2");
    assert_eq!(exported[0][source], "source-a", "source data must survive");
    assert_eq!(exported[0][added], "user-tag", "annotation uses the alias");
    assert!(
        result
            .warnings
            .iter()
            .any(|warning| warning.contains("trivium-tags_2")),
        "the alias mapping must be reported: {:?}",
        result.warnings
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn destination_equal_to_source_evidence_is_rejected() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = root.join("events.csv");
    let id = create(&store, &root);
    let before = fs::read_to_string(&path).expect("failed to read source");

    let error = export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: path.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: None,
        },
    )
    .expect_err("destination equal to the source must fail");
    assert!(
        error.to_string().contains("Choose a different file name"),
        "unexpected error: {error}"
    );
    assert_eq!(
        fs::read_to_string(&path).expect("failed to read source"),
        before,
        "the evidence file must not be modified"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn failed_finalize_keeps_the_existing_destination_and_removes_the_temporary_file() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);

    let destination = root.join("output.csv");
    fs::create_dir_all(&destination).expect("failed to create destination dir");
    fs::write(destination.join("keep.txt"), "keep").expect("failed to write sentinel");

    let error = export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: None,
        },
    )
    .expect_err("writing over a directory must fail");
    assert!(!error.to_string().is_empty());
    assert_eq!(
        fs::read_to_string(destination.join("keep.txt")).expect("sentinel missing"),
        "keep",
        "the existing destination must not be broken"
    );
    for entry in fs::read_dir(&root).expect("failed to list root") {
        let name = entry.expect("entry").file_name();
        assert!(
            !name.to_string_lossy().starts_with("output.csv.tmp."),
            "temporary file left behind: {:?}",
            name
        );
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn cancelled_export_leaves_the_destination_untouched() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);
    let destination = root.join("existing.csv");
    fs::write(&destination, "existing").expect("failed to write existing destination");

    let guard = try_begin_job(JobKind::Export, Some(id)).expect("job");
    assert!(request_cancel(guard.id()));
    let job = JobContext::with_job(guard, None);

    let error = export_project_with_job(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: None,
        },
        &job,
    )
    .expect_err("cancelled export must fail");
    assert_eq!(error.to_string(), "Cancelled.");
    assert_eq!(
        fs::read_to_string(&destination).expect("destination missing"),
        "existing",
        "a cancelled export must not change the destination"
    );
    for entry in fs::read_dir(&root).expect("failed to list root") {
        let name = entry.expect("entry").file_name();
        assert!(
            !name.to_string_lossy().starts_with("existing.csv.tmp."),
            "temporary file left behind: {:?}",
            name
        );
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn export_then_restore_roundtrips_tags_bookmark_and_memo() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 1,
            flag: Some("critical".to_string()),
            memo: Some(Some("triage".to_string())),
            tags: Some(vec!["host".to_string(), "triage".to_string()]),
            bookmarked: Some(true),
        },
    )
    .expect("annotation failed");

    let destination = root.join("roundtrip.csv");
    export_rows(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some("all".to_string()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: Some(true),
        },
    );

    let reimported = projects::create_project(
        &store,
        CreateProjectPayload {
            path: destination.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: Some(true),
        },
    )
    .expect("reimport failed");
    let row = rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: reimported.meta.id,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .expect("query failed")
    .rows
    .into_iter()
    .find(|row| row.row_index == 1)
    .expect("row missing");
    assert_eq!(row.flag, "critical");
    assert_eq!(row.memo.as_deref(), Some("triage"));
    assert_eq!(row.tags, vec!["host".to_string(), "triage".to_string()]);
    assert!(row.bookmarked);

    let _ = fs::remove_dir_all(root);
}

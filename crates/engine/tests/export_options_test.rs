//! F27 export options: filtered scope, visible columns, and sort reflection.

use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::QueryRowsPayload;
use trivium_engine::ops::{projects, rows};
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-export-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn write_fixture(root: &Path) -> PathBuf {
    let path = root.join("events.csv");
    let text = "ts,eventid,msg\n\
                2024-01-01T00:00:01,9223372036854775807,max row\n\
                2024-01-01T00:00:02,9223372036854775806,neighbor row\n\
                2024-01-01T00:00:03,5,small row\n";
    fs::write(&path, text).expect("failed to write fixture");
    path
}

fn create(store: &ProjectsStore, root: &Path) -> uuid::Uuid {
    let path = write_fixture(root);
    let summary = projects::create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
        },
    )
    .expect("failed to create project");
    summary.meta.id
}

fn run_export(store: &ProjectsStore, root: &Path, payload: ExportProjectPayload) -> String {
    let destination = root.join("export.csv");
    export_project(
        store,
        ExportProjectPayload {
            destination: destination.to_string_lossy().to_string(),
            ..payload
        },
    )
    .expect("export failed");
    fs::read_to_string(&destination).expect("failed to read export")
}

#[test]
fn default_scope_exports_every_row() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);

    let exported = run_export(
        &store,
        &root,
        ExportProjectPayload {
            project_id: id,
            destination: String::new(),
            scope: None,
            search: None,
            condition: None,
            columns: None,
            sort_key: None,
            sort_direction: None,
        },
    );
    let lines: Vec<&str> = exported.lines().collect();
    assert_eq!(lines.len(), 4, "header plus every data row");
    assert!(lines[0].starts_with("trivium-safe,"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn filtered_scope_row_count_matches_query_total() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);

    let query = rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            search: Some("max row".to_string()),
            search_columns: None,
            flag_filter: None,
            bookmarked_only: None,
            offset: None,
            limit: None,
            sort_key: None,
            sort_direction: None,
            condition: None,
        },
    )
    .expect("query failed");

    let exported = run_export(
        &store,
        &root,
        ExportProjectPayload {
            project_id: id,
            destination: String::new(),
            scope: Some("filtered".to_string()),
            search: Some("max row".to_string()),
            condition: None,
            columns: None,
            sort_key: None,
            sort_direction: None,
        },
    );
    let lines: Vec<&str> = exported.lines().collect();
    assert_eq!(
        lines.len() - 1,
        query.total_filtered_rows,
        "filtered export row count must equal total_filtered_rows"
    );
    assert!(
        exported.contains("max row") && !exported.contains("neighbor row"),
        "only the matching row must be exported"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn columns_option_limits_output_columns() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);

    let exported = run_export(
        &store,
        &root,
        ExportProjectPayload {
            project_id: id,
            destination: String::new(),
            scope: None,
            search: None,
            condition: None,
            columns: Some(vec!["msg".to_string()]),
            sort_key: None,
            sort_direction: None,
        },
    );
    let header = exported.lines().next().expect("export must have a header");
    assert_eq!(
        header.split(',').collect::<Vec<_>>(),
        vec![
            "trivium-safe",
            "trivium-suspicious",
            "trivium-critical",
            "trivium-memo",
            "msg"
        ],
        "only the requested column plus derived trivium columns must remain"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn input_flag_and_memo_columns_survive_export_with_annotations() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = root.join("annotated.csv");
    let text = "flag,memo,msg\n\
                safe,original memo one,first message\n\
                review,original memo two,second message\n\
                hold,original memo three,third message\n";
    fs::write(&path, text).expect("failed to write fixture");

    let summary = projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
        },
    )
    .expect("failed to create project");
    let id = summary.meta.id;

    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: Some("safe".to_string()),
            memo: Some(Some("investigated".to_string())),
            tags: Some(vec!["alpha".to_string()]),
            bookmarked: None,
        },
    )
    .expect("failed to annotate row");

    let exported = run_export(
        &store,
        &root,
        ExportProjectPayload {
            project_id: id,
            destination: String::new(),
            scope: None,
            search: None,
            condition: None,
            columns: None,
            sort_key: None,
            sort_direction: None,
        },
    );
    let lines: Vec<&str> = exported.lines().collect();
    assert_eq!(
        lines[0].split(',').collect::<Vec<_>>(),
        vec![
            "trivium-safe",
            "trivium-suspicious",
            "trivium-critical",
            "trivium-memo",
            "flag",
            "memo",
            "msg"
        ],
        "input flag/memo columns must coexist with the trivium annotation columns"
    );

    let first: Vec<&str> = lines[1].split(',').collect();
    assert_eq!(
        first,
        vec![
            "1",
            "0",
            "0",
            "investigated [alpha]",
            "safe",
            "original memo one",
            "first message"
        ],
        "annotation and the original flag/memo/msg values must both be present"
    );

    let second: Vec<&str> = lines[2].split(',').collect();
    assert_eq!(second[4], "review", "unannotated input flag must be kept");
    assert_eq!(second[5], "original memo two", "unannotated input memo must be kept");
    assert_eq!(second[6], "second message");

    let third: Vec<&str> = lines[3].split(',').collect();
    assert_eq!(third[4], "hold");
    assert_eq!(third[5], "original memo three");
    assert_eq!(third[6], "third message");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn sort_options_reflect_exported_row_order() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let id = create(&store, &root);

    let build_payload = |direction: &str| ExportProjectPayload {
        project_id: id,
        destination: String::new(),
        scope: None,
        search: None,
        condition: None,
        columns: None,
        sort_key: Some("eventid".to_string()),
        sort_direction: Some(direction.to_string()),
    };

    let asc = run_export(&store, &root, build_payload("asc"));
    let asc_lines: Vec<&str> = asc.lines().collect();
    assert!(
        asc_lines[1].contains("small row"),
        "ascending export must start with the smallest eventid"
    );

    let desc = run_export(&store, &root, build_payload("desc"));
    let desc_lines: Vec<&str> = desc.lines().collect();
    assert!(
        desc_lines[1].contains("max row"),
        "descending export must start with i64::MAX"
    );

    let _ = fs::remove_dir_all(root);
}

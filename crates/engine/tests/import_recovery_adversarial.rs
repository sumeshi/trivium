use std::{fs, path::Path};

use trivium_engine::{
    import::options::{ImportIssueAction, ImportOptions},
    ops::projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
    project_io::{read_project_dataframe, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD},
    store::ProjectsStore,
};

fn payload(path: &Path, recover: bool) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().into_owned(),
        description: None,
        delimiter: None,
        restore_annotations: None,
        import_options: Some(ImportOptions {
            recover,
            ..Default::default()
        }),
    }
}

#[test]
fn invalid_utf8_fields_cannot_combine_into_a_valid_schema_discovery_record() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("split-utf8.csv");
    // Concatenating the invalid fields without their comma gives valid UTF-8
    // (U+00E9). Neither individual field is readable, so this record must not
    // add a second, fabricated data column during the recovery preflight.
    let bytes = b"value\n\xc3,\xa9\ngood\n";
    fs::write(&source, bytes).unwrap();
    let store = ProjectsStore::new(dir.path().join("cases")).unwrap();
    assert!(create_project(&store, payload(&source, false)).is_err());
    assert!(store.try_all().unwrap().is_empty());
    let summary = create_project(&store, payload(&source, true)).unwrap();
    assert_eq!(summary.meta.total_records, 1);
    assert_eq!(summary.meta.columns.len(), 1);
    assert_eq!(summary.meta.columns[0].source, "value");
    let report = summary.meta.import_report.as_ref().unwrap();
    assert_eq!(report.source_records, 2);
    assert_eq!(report.imported_records, 1);
    assert_eq!(report.skipped_records, 1);
    assert_eq!(report.generated_columns, 0);
    assert_eq!(report.issues.len(), 1);
    assert_eq!(report.issues[0].source_record, Some(2));
    assert_eq!(report.issues[0].action, ImportIssueAction::Skipped);
    let loaded = load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .unwrap();

    let loaded_rows = common::rows(&store, loaded.project.meta.id);
    assert_eq!(
        loaded_rows[0].cells[&summary.meta.columns[0].id]
            .text
            .as_deref(),
        Some("good")
    );
    let frame =
        read_project_dataframe(&store.project_dir(&summary.meta.id).join("data.parquet")).unwrap();
    assert_eq!(
        frame.column(INTERNAL_ROW_ID).unwrap().i64().unwrap().get(0),
        Some(0)
    );
    assert_eq!(
        frame
            .column(INTERNAL_SOURCE_RECORD)
            .unwrap()
            .i64()
            .unwrap()
            .get(0),
        Some(3)
    );
    assert_eq!(fs::read(source).unwrap(), bytes);
}

#[test]
fn unclosed_csv_quote_is_fatal_by_default_and_only_salvaged_by_explicit_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("unclosed.csv");
    let bytes = b"name,note\nfirst,valid\nbroken,\"open\nlooks,like another row\n";
    fs::write(&source, bytes).unwrap();
    let store = ProjectsStore::new(dir.path().join("cases")).unwrap();
    let error = create_project(&store, payload(&source, false)).unwrap_err();
    assert!(
        error.to_string().contains("Unterminated quoted CSV field"),
        "{error}"
    );
    assert!(store.try_all().unwrap().is_empty());
    assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
    let summary = create_project(&store, payload(&source, true)).unwrap();
    assert_eq!(summary.meta.total_records, 1);
    let report = summary.meta.import_report.as_ref().unwrap();
    assert!(report.unreadable_tail);
    assert_eq!(report.imported_records, 1);
    assert_eq!(report.source_records, 1);
    assert_eq!(
        report.skipped_records, 0,
        "unknown tail length is not a skipped-record count"
    );
    assert_eq!(report.issues[0].source_record, Some(3));
    assert_eq!(report.issues[0].action, ImportIssueAction::UnreadableTail);
    let loaded = load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .unwrap();

    let loaded_rows = common::rows(&store, loaded.project.meta.id);
    assert_eq!(loaded_rows.len(), 1);
    assert_eq!(
        loaded_rows[0].cells[&summary.meta.columns[0].id]
            .text
            .as_deref(),
        Some("first")
    );
    assert_eq!(fs::read(source).unwrap(), bytes);
}

mod common;

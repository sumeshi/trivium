//! Recovery is explicit, bounded, and preserves source values and positions.
use polars::prelude::DataFrame;
use std::{fs, path::Path};
use trivium_engine::{
    import::{
        csv::{BatchLimits, CsvImportSummary, CsvImporter, MAX_CSV_COLUMNS},
        options::{ImportIssueAction, ImportOptions},
    },
    jobs::JobContext,
    project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD},
};

fn read(
    path: &Path,
    options: &ImportOptions,
    limits: BatchLimits,
) -> Result<(DataFrame, CsvImportSummary), String> {
    let job = JobContext::none();
    let mut importer = CsvImporter::open_with_options(path, None, limits, options, &job)
        .map_err(|e| e.to_string())?;
    let mut frame: Option<DataFrame> = None;
    while let Some(batch) = importer.next_batch(&job).map_err(|e| e.to_string())? {
        if let Some(frame) = frame.as_mut() {
            frame.vstack_mut(&batch).unwrap();
        } else {
            frame = Some(batch);
        }
    }
    Ok((frame.unwrap_or_default(), importer.into_summary()))
}

fn limits() -> BatchLimits {
    BatchLimits {
        max_rows: 1,
        max_bytes: 1,
        read_buffer: 3,
    }
}
fn recovery() -> ImportOptions {
    ImportOptions {
        recover: true,
        ..ImportOptions::default()
    }
}

#[test]
fn headerless_keeps_first_record_and_generates_stable_source_names() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("headerless.csv");
    let source = b"00123,first\n00456,second\n";
    fs::write(&path, source).unwrap();
    let options = ImportOptions {
        has_headers: false,
        ..ImportOptions::default()
    };
    let (frame, summary) = read(&path, &options, limits()).unwrap();
    assert_eq!(summary.rows, 2);
    assert_eq!(summary.columns[0].label, "column 1");
    assert_eq!(summary.columns[0].source, "column 1");
    assert_eq!(
        frame.column("c000001").unwrap().str().unwrap().get(0),
        Some("00123")
    );
    assert_eq!(
        frame
            .column(INTERNAL_SOURCE_RECORD)
            .unwrap()
            .i64()
            .unwrap()
            .no_null_iter()
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(summary.report.generated_columns, 2);
    assert_eq!(summary.report.imported_records, 2);
    assert_eq!(summary.report.has_headers, Some(false));
    assert_eq!(fs::read(&path).unwrap(), source);
}

#[test]
fn unequal_records_require_recovery_and_missing_is_distinct_from_empty() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("unequal.csv");
    let source = "name,note\nalice,\"line1\nline2\"\nbob\ncarol,,tail\n";
    fs::write(&path, source).unwrap();
    assert!(read(&path, &ImportOptions::default(), limits()).is_err());
    let (frame, summary) = read(&path, &recovery(), limits()).unwrap();
    assert_eq!(summary.columns.len(), 3);
    assert_eq!(summary.columns[2].label, "column 3");
    assert_eq!(summary.rows, 3);
    let note = frame.column("c000002").unwrap().str().unwrap();
    assert_eq!(note.get(0), Some("line1\nline2"));
    assert_eq!(note.get(1), None);
    assert_eq!(note.get(2), Some(""));
    assert_eq!(
        frame.column("c000003").unwrap().str().unwrap().get(2),
        Some("tail")
    );
    let originals = frame.column(INTERNAL_ORIGINAL).unwrap().str().unwrap();
    assert_eq!(
        serde_json::from_str::<Vec<String>>(originals.get(1).unwrap()).unwrap(),
        vec!["bob"]
    );
    assert_eq!(
        frame
            .column(INTERNAL_ROW_ID)
            .unwrap()
            .i64()
            .unwrap()
            .no_null_iter()
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        frame
            .column(INTERNAL_SOURCE_RECORD)
            .unwrap()
            .i64()
            .unwrap()
            .no_null_iter()
            .collect::<Vec<_>>(),
        vec![2, 4, 5]
    );
    assert_eq!(summary.report.source_records, 3);
    assert_eq!(summary.report.imported_records, 3);
    assert_eq!(summary.report.repaired_records, 2);
    assert_eq!(summary.report.generated_columns, 1);
    assert_eq!(summary.report.skipped_records, 0);
    assert_eq!(fs::read_to_string(&path).unwrap(), source);
}

#[test]
fn invalid_utf8_is_skipped_at_a_complete_csv_record_boundary_only() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("encoding.csv");
    let source = b"name,note\nfirst,valid\ninvalid,\"bad\xff\nmultiline\"\nlast,valid\n";
    fs::write(&path, source).unwrap();
    assert!(read(&path, &ImportOptions::default(), limits()).is_err());
    let (frame, summary) = read(&path, &recovery(), limits()).unwrap();
    assert_eq!(summary.rows, 2);
    assert_eq!(summary.report.source_records, 3);
    assert_eq!(summary.report.skipped_records, 1);
    assert_eq!(
        frame
            .column(INTERNAL_SOURCE_RECORD)
            .unwrap()
            .i64()
            .unwrap()
            .no_null_iter()
            .collect::<Vec<_>>(),
        vec![2, 5]
    );
    assert_eq!(
        frame
            .column(INTERNAL_ROW_ID)
            .unwrap()
            .i64()
            .unwrap()
            .no_null_iter()
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(summary.report.issues[0].source_record, Some(3));
    assert_eq!(summary.report.issues[0].action, ImportIssueAction::Skipped);
    assert_eq!(fs::read(&path).unwrap(), source);
}

#[test]
fn unknown_quoted_tail_is_reported_without_guessing_new_record_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("tail.csv");
    fs::write(
        &path,
        "name,note\nfirst,valid\nbroken,\"open\nlooks,like another row\n",
    )
    .unwrap();
    assert!(read(&path, &ImportOptions::default(), limits()).is_err());
    let (frame, summary) = read(&path, &recovery(), limits()).unwrap();
    assert_eq!(frame.height(), 1);
    assert_eq!(summary.report.imported_records, 1);
    assert_eq!(summary.report.source_records, 1);
    assert_eq!(summary.report.skipped_records, 0);
    assert!(summary.report.unreadable_tail);
    assert_eq!(summary.report.issues[0].source_record, Some(3));
    assert_eq!(
        summary.report.issues[0].action,
        ImportIssueAction::UnreadableTail
    );
}

#[test]
fn every_invalid_record_prevents_an_empty_success_and_issue_examples_are_bounded() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("invalid.csv");
    fs::write(&path, b"value\n\xff\n").unwrap();
    assert!(read(&path, &recovery(), limits())
        .err()
        .unwrap()
        .contains("No readable CSV records"));
    let mut source = b"value\nvalid\n".to_vec();
    for _ in 0..100 {
        source.extend_from_slice(b"\xff\n");
    }
    fs::write(&path, source).unwrap();
    let (_, summary) = read(&path, &recovery(), limits()).unwrap();
    assert_eq!(summary.report.skipped_records, 100);
    assert_eq!(summary.report.issue_count, 100);
    assert_eq!(summary.report.issues.len(), 30);
}

#[test]
fn invalid_utf8_does_not_bypass_the_column_limit_in_recovery() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("wide.csv");
    let mut source = b"value\n\xff".to_vec();
    source.extend(std::iter::repeat_n(b',', MAX_CSV_COLUMNS));
    source.push(b'\n');
    fs::write(&path, source).unwrap();
    let error = read(&path, &recovery(), limits()).err().unwrap();
    assert!(error.contains("4097 data columns"), "{error}");
    assert!(error.contains("No columns were discarded"));
}

#[test]
fn recovery_reopens_headerless_input_without_losing_the_first_readable_row() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("headerless.csv");
    fs::write(&path, b"\xff\nfirst,001\nsecond\n").unwrap();
    let options = ImportOptions {
        has_headers: false,
        ..recovery()
    };
    let (frame, summary) = read(&path, &options, limits()).unwrap();
    assert_eq!(summary.rows, 2);
    assert_eq!(summary.report.skipped_records, 1);
    assert_eq!(summary.report.repaired_records, 1);
    assert_eq!(
        frame.column("c000001").unwrap().str().unwrap().get(0),
        Some("first")
    );
    assert_eq!(
        frame.column("c000002").unwrap().str().unwrap().get(0),
        Some("001")
    );
    assert_eq!(
        frame
            .column(INTERNAL_SOURCE_RECORD)
            .unwrap()
            .i64()
            .unwrap()
            .no_null_iter()
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[test]
fn empty_headers_have_names_without_rewriting_their_source_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("headers.csv");
    fs::write(&path, ",same,same\n1,2,3\n").unwrap();
    let (_, summary) = read(&path, &ImportOptions::default(), limits()).unwrap();
    assert_eq!(summary.columns[0].label, "column 1");
    assert_eq!(summary.columns[0].source, "");
    assert_ne!(summary.columns[1].id, summary.columns[2].id);
    assert_eq!(summary.report.generated_columns, 1);
    assert_eq!(summary.report.repaired_records, 0);
}

#[test]
fn only_one_encoding_marker_is_removed_and_literal_bom_characters_survive() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("bom.csv");
    let text = "\u{feff}first,value\nsecond,value\n";
    let mut utf8 = vec![0xEF, 0xBB, 0xBF];
    utf8.extend_from_slice(text.as_bytes());
    let mut utf16 = vec![0xFF, 0xFE];
    for unit in text.encode_utf16() {
        utf16.extend_from_slice(&unit.to_le_bytes());
    }
    for bytes in [utf8, utf16] {
        fs::write(&path, bytes).unwrap();
        for recover in [false, true] {
            let options = ImportOptions {
                has_headers: false,
                recover,
                ..ImportOptions::default()
            };
            let (frame, _) = read(&path, &options, limits()).unwrap();
            assert_eq!(
                frame.column("c000001").unwrap().str().unwrap().get(0),
                Some("\u{feff}first")
            );
        }
    }
}

#[test]
fn bom_and_quoted_first_field_allow_multiline_records_in_recovery() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("bom-quoted.csv");
    let mut source = vec![0xEF, 0xBB, 0xBF];
    source.extend_from_slice(b"\"header\ncontinued\",value\nfirst,\"quoted\nvalue\"\n");
    fs::write(&path, source).unwrap();
    let (frame, summary) = read(&path, &recovery(), limits()).unwrap();
    assert_eq!(summary.rows, 1);
    assert_eq!(summary.columns[0].label, "header\ncontinued");
    assert_eq!(
        frame.column("c000002").unwrap().str().unwrap().get(0),
        Some("quoted\nvalue")
    );
    assert!(!summary.report.unreadable_tail);
}

#[test]
fn recovered_case_persists_diagnostics_and_roundtrips_annotations_through_csv() {
    use sha2::{Digest, Sha256};
    use trivium_engine::{
        ops::{
            export::{export_project, ExportProjectPayload},
            flags::{update_flag, UpdateFlagPayload},
            projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
            rows::QuerySpec,
        },
        store::ProjectsStore,
    };
    let root = tempfile::tempdir().unwrap();
    let cases = root.path().join("cases");
    let source = root.path().join("recovered.csv");
    let bytes = b"name,note\nfirst,ok\n\xff,invalid\nlast,,extra\n";
    fs::write(&source, bytes).unwrap();
    let store = ProjectsStore::new(cases.clone()).unwrap();
    let project = create_project(
        &store,
        CreateProjectPayload {
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
            import_options: Some(recovery()),
        },
    )
    .unwrap();
    let report = project.meta.import_report.clone().unwrap();
    assert_eq!(report.encoding, "utf-8");
    assert_eq!(report.imported_records, 2);
    assert_eq!(report.skipped_records, 1);
    assert_eq!(report.repaired_records, 1);
    assert_eq!(report.generated_columns, 1);
    assert_eq!(
        project.meta.source_sha256.as_deref(),
        Some(format!("{:x}", Sha256::digest(bytes)).as_str())
    );
    let id = project.meta.id;
    drop(store);
    let store = ProjectsStore::new(cases).unwrap();
    assert_eq!(
        store.try_find(&id).unwrap().unwrap().import_report.as_ref(),
        Some(&report)
    );
    let note = "Sender: <alice@example.test>\n<CommandLine>whoami</CommandLine>";
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 1,
            flag: Some("critical".into()),
            memo: Some(Some(note.into())),
            tags: Some(vec!["review,comma".into(), "日本語".into()]),
            bookmarked: Some(true),
        },
    )
    .unwrap();
    let exported = root.path().join("exported.csv");
    export_project(
        &store,
        ExportProjectPayload {
            allow_overwrite: false,
            project_id: id,
            destination: exported.to_string_lossy().into(),
            scope: None,
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: Some(true),
        },
    )
    .unwrap();
    let restored = create_project(
        &store,
        CreateProjectPayload {
            path: exported.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: Some(true),
            import_options: None,
        },
    )
    .unwrap();
    let loaded = load_project(
        &store,
        ProjectRequest {
            project_id: restored.meta.id,
        },
    )
    .unwrap();

    let loaded_rows = common::rows(&store, loaded.project.meta.id);
    assert_eq!(loaded_rows.len(), 2);
    assert_eq!(loaded_rows[1].flag, "critical");
    assert_eq!(loaded_rows[1].memo.as_deref(), Some(note));
    assert_eq!(loaded_rows[1].tags, ["review,comma", "日本語"]);
    assert!(loaded_rows[1].bookmarked);
    assert_eq!(restored.meta.import_report.unwrap().skipped_records, 0);
    assert_eq!(fs::read(&source).unwrap(), bytes);
}

mod common;

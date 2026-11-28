//! Source locations use physical CR, LF, and CRLF lines without rewriting cells.
use std::{fs, path::Path};

use polars::prelude::DataFrame;
use trivium_engine::{
    import::{
        csv::{BatchLimits, CsvImportSummary, CsvImporter},
        options::{ImportIssueAction, ImportOptions},
    },
    jobs::JobContext,
    ops::projects::{create_project, CreateProjectPayload},
    project_io::{read_project_dataframe, INTERNAL_SOURCE_RECORD},
    store::ProjectsStore,
};

fn read(path: &Path, headers: bool, read_buffer: usize) -> (DataFrame, CsvImportSummary) {
    let job = JobContext::none();
    let options = ImportOptions {
        has_headers: headers,
        recover: true,
        ..Default::default()
    };
    let mut importer = CsvImporter::open_with_options(
        path,
        None,
        BatchLimits {
            max_rows: 100,
            max_bytes: 100,
            read_buffer,
        },
        &options,
        &job,
    )
    .unwrap();
    let mut frame: Option<DataFrame> = None;
    while let Some(batch) = importer.next_batch(&job).unwrap() {
        if let Some(frame) = frame.as_mut() {
            frame.vstack_mut(&batch).unwrap();
        } else {
            frame = Some(batch);
        }
    }
    (frame.unwrap(), importer.into_summary())
}

fn source_lines(frame: &DataFrame) -> Vec<i64> {
    frame
        .column(INTERNAL_SOURCE_RECORD)
        .unwrap()
        .i64()
        .unwrap()
        .no_null_iter()
        .collect()
}

#[test]
fn cr_only_records_and_quoted_cr_keep_physical_positions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("classic.csv");
    let bytes = b",value\r\r\"alpha\rinside\",1\rbeta\rcharlie,3\r";
    for read_buffer in [4, 64 * 1024] {
        fs::write(&path, bytes).unwrap();
        let (frame, summary) = read(&path, true, read_buffer);
        assert_eq!(source_lines(&frame), [3, 5, 6]);
        assert_eq!(
            frame.column("c000001").unwrap().str().unwrap().get(0),
            Some("alpha\rinside")
        );
        assert_eq!(summary.report.issues[0].source_record, Some(1));
        let repair = summary
            .report
            .issues
            .iter()
            .find(|issue| issue.reason.contains("missing trailing"))
            .unwrap();
        assert_eq!(repair.source_record, Some(5));
        assert_eq!(repair.action, ImportIssueAction::Repaired);
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn mixed_terminators_bom_invalid_utf8_and_pending_records_use_one_line_system() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed.csv");
    let bytes = b"\xef\xbb\xbf\r\nname,value\r\n\r\none,1\rtwo,\xff\rthree,3\n\"four\r\nquoted\",4\rfive\r\n";
    for read_buffer in [4, 64 * 1024] {
        fs::write(&path, bytes).unwrap();
        let (frame, summary) = read(&path, true, read_buffer);
        assert_eq!(source_lines(&frame), [4, 6, 7, 9]);
        assert_eq!(
            frame.column("c000001").unwrap().str().unwrap().get(2),
            Some("four\r\nquoted")
        );
        let skip = summary
            .report
            .issues
            .iter()
            .find(|issue| issue.action == ImportIssueAction::Skipped)
            .unwrap();
        assert_eq!(skip.source_record, Some(5));
        assert!(skip.reason.contains("line 5"), "{}", skip.reason);
        assert!(!skip.reason.contains("line: 4"), "{}", skip.reason);
        let repair = summary
            .report
            .issues
            .iter()
            .find(|issue| issue.action == ImportIssueAction::Repaired)
            .unwrap();
        assert_eq!(repair.source_record, Some(9));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn headerless_records_start_after_blank_lines_in_both_passes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("headerless.csv");
    fs::write(&path, b"\r\nfirst,x\r\r\nsecond\rthird,z\n").unwrap();
    let (frame, summary) = read(&path, false, 4);
    assert_eq!(source_lines(&frame), [2, 4, 5]);
    let repair = summary
        .report
        .issues
        .iter()
        .find(|issue| issue.reason.contains("missing trailing"))
        .unwrap();
    assert_eq!(repair.source_record, Some(4));
}

#[test]
fn utf16_bom_inputs_keep_quoted_cr_bytes_and_physical_record_starts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unicode.csv");
    let text = "name,note\rfirst,\"調査\rinside\"\rsecond\r";
    for big_endian in [false, true] {
        let mut bytes = if big_endian {
            vec![0xfe, 0xff]
        } else {
            vec![0xff, 0xfe]
        };
        for unit in text.encode_utf16() {
            bytes.extend_from_slice(&if big_endian {
                unit.to_be_bytes()
            } else {
                unit.to_le_bytes()
            });
        }
        fs::write(&path, &bytes).unwrap();
        let (frame, summary) = read(&path, true, 4);
        assert_eq!(source_lines(&frame), [2, 4]);
        assert_eq!(
            frame.column("c000002").unwrap().str().unwrap().get(0),
            Some("調査\rinside")
        );
        assert_eq!(summary.report.issues[0].source_record, Some(4));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn unreadable_tail_points_to_the_record_start_and_identifies_its_later_open_quote() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("open.csv");
    let bytes = b"name,note\rvalid,1\r\"record\rinside\",\"tail\rstill open";
    fs::write(&path, bytes).unwrap();
    let (frame, summary) = read(&path, true, 4);
    assert_eq!(source_lines(&frame), [2]);
    let tail = summary
        .report
        .issues
        .iter()
        .find(|issue| issue.action == ImportIssueAction::UnreadableTail)
        .unwrap();
    assert_eq!(tail.source_record, Some(3));
    assert!(
        tail.reason.contains("line 4 (record starting on line 3)"),
        "{}",
        tail.reason
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn strict_header_and_unequal_record_errors_use_physical_lines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.csv");
    fs::write(&path, b"\r\rname,\xff\r").unwrap();
    let error = CsvImporter::open(&path, None, BatchLimits::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("line 3"), "{error}");
    fs::write(&path, b"name,value\rfirst,1\rshort\r").unwrap();
    let mut importer = CsvImporter::open(&path, None, BatchLimits::default()).unwrap();
    let error = importer
        .next_batch(&JobContext::none())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("line 3"), "{error}");
}

#[test]
fn incomplete_bom_candidates_before_terminators_do_not_shift_recovered_record_ordinals() {
    for bytes in [b"\xef\nvalid\n".as_slice(), b"\xef\xbb\rvalid\r"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial-marker.csv");
        fs::write(&path, bytes).unwrap();
        let store = ProjectsStore::new(dir.path().join("cases")).unwrap();
        let summary = create_project(
            &store,
            CreateProjectPayload {
                path: path.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
                import_options: Some(ImportOptions {
                    has_headers: false,
                    recover: true,
                    ..Default::default()
                }),
            },
        )
        .unwrap();
        assert_eq!(summary.meta.total_records, 1);
        let frame =
            read_project_dataframe(&store.project_dir(&summary.meta.id).join("data.parquet"))
                .unwrap();
        assert_eq!(source_lines(&frame), [2]);
        assert_eq!(
            frame.column("c000001").unwrap().str().unwrap().get(0),
            Some("valid")
        );
        let report = summary.meta.import_report.unwrap();
        let skipped = report
            .issues
            .iter()
            .find(|issue| issue.action == ImportIssueAction::Skipped)
            .unwrap();
        assert_eq!(skipped.source_record, Some(1));
        assert!(skipped.reason.contains("line 1"));
        assert_eq!(
            (
                report.source_records,
                report.imported_records,
                report.skipped_records
            ),
            (2, 1, 1)
        );
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn partial_bom_at_eof_has_a_source_position_and_non_bom_unicode_is_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prefix.csv");
    for bytes in [b"\xef".as_slice(), b"\xef\xbb"] {
        fs::write(&path, bytes).unwrap();
        let error = CsvImporter::open(&path, None, BatchLimits::default())
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("line 1"), "{error}");
        assert!(!error.contains("Could not determine"), "{error}");
    }
    let original = "\u{f000}first\rvalid\r";
    fs::write(&path, original).unwrap();
    let (frame, _) = read(&path, false, 4);
    assert_eq!(source_lines(&frame), [1, 2]);
    assert_eq!(
        frame.column("c000001").unwrap().str().unwrap().get(0),
        Some("\u{f000}first")
    );
    assert_eq!(fs::read_to_string(path).unwrap(), original);
}

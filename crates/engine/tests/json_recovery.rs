use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use polars::prelude::DataFrame;
use tempfile::TempDir;
use trivium_engine::import::jsonl::{BatchLimits, JsonImportSummary, JsonImporter, RecordFormat};
use trivium_engine::import::options::{ImportIssueAction, ImportOptions, ImportReport};
use trivium_engine::jobs::{
    queue_job, request_cancel, JobContext, JobKind, JobProgress, ProgressFn,
};
use trivium_engine::ops::projects::{create_project, CreateProjectPayload};
use trivium_engine::project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD};
use trivium_engine::store::ProjectsStore;

static JOB_LOCK: Mutex<()> = Mutex::new(());

fn recover() -> ImportOptions {
    ImportOptions {
        recover: true,
        ..Default::default()
    }
}

fn read(path: &Path, format: RecordFormat) -> (JsonImportSummary, Vec<DataFrame>) {
    let job = JobContext::none();
    let importer = JsonImporter::open_with_options(
        path,
        format,
        BatchLimits {
            max_rows: 1,
            max_bytes: 1024,
        },
        &recover(),
        &job,
    )
    .expect("open recoverable fixture");
    let mut frames = Vec::new();
    let summary = importer
        .write_batches(&job, |batch| {
            frames.push(batch.clone());
            Ok(())
        })
        .expect("write recovered records");
    (summary, frames)
}

fn payload(path: &Path, options: Option<ImportOptions>) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().into_owned(),
        description: None,
        delimiter: None,
        restore_annotations: None,
        import_options: options,
    }
}

fn ids(frames: &[DataFrame], column: &str) -> Vec<i64> {
    frames
        .iter()
        .flat_map(|frame| {
            frame
                .column(column)
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
        })
        .collect()
}

#[test]
fn jsonl_recovery_skips_only_bad_physical_lines_and_keeps_dense_ids() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("damaged.jsonl");
    let mut bytes = br#"{"ok":"first"}

{"broken":
[]
{"nested":{"user":"alice","user":"bob"}}
{"user":"alice","\u0075ser":"bob"}
{"bad":""#
        .to_vec();
    bytes.extend_from_slice(&[0xff, b'"', b'}', b'\n']);
    bytes.extend_from_slice(
        br#"{"ok":"last","big":170141183460469231731687303715884105728}
"#,
    );
    fs::write(&path, bytes).unwrap();
    let (summary, frames) = read(&path, RecordFormat::JsonLines);
    assert_eq!(summary.rows, 2);
    assert_eq!(summary.report.source_records, 7);
    assert_eq!(summary.report.imported_records, 2);
    assert_eq!(summary.report.skipped_records, 5);
    assert!(!summary.report.unreadable_tail);
    assert_eq!(ids(&frames, INTERNAL_ROW_ID), [0, 1]);
    assert_eq!(ids(&frames, INTERNAL_SOURCE_RECORD), [1, 8]);
    assert_eq!(
        summary
            .columns
            .iter()
            .map(|c| c.source.as_str())
            .collect::<Vec<_>>(),
        ["/ok", "/big"]
    );
    assert_eq!(
        summary
            .report
            .issues
            .iter()
            .map(|i| i.source_record)
            .collect::<Vec<_>>(),
        [Some(3), Some(4), Some(5), Some(6), Some(7)]
    );
    assert!(summary
        .report
        .issues
        .iter()
        .all(|i| i.action == ImportIssueAction::Skipped));
    let original = frames[1]
        .column(INTERNAL_ORIGINAL)
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert!(original.contains("170141183460469231731687303715884105728"));
    assert!(JsonImporter::open(
        &path,
        RecordFormat::JsonLines,
        BatchLimits::default(),
        &JobContext::none()
    )
    .is_err());
}

#[test]
fn array_recovery_skips_complete_nonobjects_and_duplicate_keys_without_guessing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("damaged.json");
    fs::write(
        &path,
        r#"[{"ok":1},null,{"x":{"a":1,"a":2}},{"a":1,"\u0061":2},{"ok":5}]"#,
    )
    .unwrap();
    let (summary, frames) = read(&path, RecordFormat::Json);
    assert_eq!(summary.rows, 2);
    assert_eq!(summary.report.source_records, 5);
    assert_eq!(summary.report.skipped_records, 3);
    assert!(!summary.report.unreadable_tail);
    assert_eq!(ids(&frames, INTERNAL_SOURCE_RECORD), [1, 5]);
    assert_eq!(ids(&frames, INTERNAL_ROW_ID), [0, 1]);
    assert!(summary.report.issues[1]
        .reason
        .contains("Duplicate JSON key"));
}

#[test]
fn malformed_array_tail_recovers_only_the_complete_prefix_and_reports_unknown_remainder() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("truncated.json");
    for body in [
        r#"[{"a":1},{"a":"unfinished"#,
        r#"[{"a":1},broken,{"a":3}]"#,
        r#"[{"a":1},]"#,
    ] {
        fs::write(&path, body).unwrap();
        let (summary, frames) = read(&path, RecordFormat::Json);
        assert_eq!(summary.rows, 1);
        assert_eq!(summary.report.source_records, 1);
        assert_eq!(
            summary.report.skipped_records, 0,
            "unknown suffix is not a known record count"
        );
        assert!(summary.report.unreadable_tail);
        assert_eq!(summary.report.issues[0].source_record, Some(2));
        assert_eq!(
            summary.report.issues[0].action,
            ImportIssueAction::UnreadableTail
        );
        assert_eq!(ids(&frames, INTERNAL_SOURCE_RECORD), [1]);
        assert!(JsonImporter::open(
            &path,
            RecordFormat::Json,
            BatchLimits::default(),
            &JobContext::none()
        )
        .is_err());
    }
}

#[test]
fn trailing_data_after_a_closed_array_is_reported_instead_of_silently_accepted() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("trailing.json");
    fs::write(&path, r#"[{"a":1}] {"a":2}"#).unwrap();
    let (summary, _) = read(&path, RecordFormat::Json);
    assert_eq!(summary.rows, 1);
    assert!(summary.report.unreadable_tail);
    assert_eq!(summary.report.issues[0].source_record, None);
}

#[test]
fn failed_recovery_never_publishes_an_empty_case() {
    let dir = TempDir::new().unwrap();
    let store = ProjectsStore::new(dir.path().join("cases")).unwrap();
    for (name, body) in [
        ("bad.jsonl", "broken\nnull\n"),
        ("bad.json", "[broken,{\"a\":1}]"),
        ("single.json", "{\"a\":1,\"a\":2}"),
    ] {
        let path = dir.path().join(name);
        fs::write(&path, body).unwrap();
        assert!(create_project(&store, payload(&path, Some(recover()))).is_err());
        assert!(store.try_all().unwrap().is_empty());
        assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
    }
}

#[test]
fn legitimate_empty_sources_are_not_classified_as_failed_recovery() {
    let dir = TempDir::new().unwrap();
    for (name, body, format) in [
        ("empty.json", "[]", RecordFormat::Json),
        ("empty.jsonl", " \n\n", RecordFormat::JsonLines),
    ] {
        let path = dir.path().join(name);
        fs::write(&path, body).unwrap();
        let (summary, frames) = read(&path, format);
        assert_eq!(summary.rows, 0);
        assert_eq!(summary.report, ImportReport::default());
        assert!(frames.is_empty());
    }
}

#[test]
fn reports_are_bounded_and_persist_when_the_project_is_reopened() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("cases");
    let path = dir.path().join("many.jsonl");
    let mut body = "not-json\n".repeat(100);
    body.push_str("{\"a\":1}\n");
    fs::write(&path, body).unwrap();
    let store = ProjectsStore::new(root.clone()).unwrap();
    assert!(
        create_project(&store, payload(&path, None)).is_err(),
        "strict is the default"
    );
    let summary = create_project(&store, payload(&path, Some(recover()))).unwrap();
    let report = summary.meta.import_report.as_ref().unwrap();
    assert_eq!(report.skipped_records, 100);
    assert_eq!(report.issue_count, 100);
    assert_eq!(report.issues.len(), ImportReport::MAX_ISSUES);
    drop(store);
    let reopened = ProjectsStore::new(root).unwrap();
    let saved = reopened.try_find(&summary.meta.id).unwrap().unwrap();
    assert_eq!(saved.import_report, summary.meta.import_report);
    assert_eq!(saved.total_records, 1);
}

#[test]
fn cancellation_is_observed_even_when_every_scanned_line_is_invalid() {
    let _lock = JOB_LOCK.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("invalid.jsonl");
    fs::write(&path, "not-json\n".repeat(2_000)).unwrap();
    let guard = queue_job(JobKind::Import, None).unwrap();
    let id = guard.id();
    let emit: ProgressFn = Arc::new(move |progress: JobProgress| {
        if progress.message.as_deref() == Some("Scanning JSON") {
            request_cancel(id);
        }
    });
    let job = JobContext::with_job(guard, Some(emit));
    let error = JsonImporter::open_with_options(
        &path,
        RecordFormat::JsonLines,
        BatchLimits::default(),
        &recover(),
        &job,
    )
    .err()
    .expect("invalid lines must not hide cancellation");
    assert_eq!(error.to_string(), "Cancelled.");
}

#[test]
fn recovery_keeps_schema_limit_failures_fatal_instead_of_skipping_records() {
    let dir = TempDir::new().unwrap();
    let mut wide = serde_json::Map::new();
    for index in 0..=trivium_engine::import::jsonl::MAX_JSON_COLUMNS {
        wide.insert(format!("k{index}"), serde_json::Value::Bool(true));
    }
    let wide = serde_json::Value::Object(wide);
    for (name, body, format) in [
        (
            "too-wide.jsonl",
            format!("{{\"a\":1}}\n{wide}\n"),
            RecordFormat::JsonLines,
        ),
        (
            "too-wide.json",
            format!("[{{\"a\":1}},{wide}]"),
            RecordFormat::Json,
        ),
    ] {
        let path = dir.path().join(name);
        fs::write(&path, body).unwrap();
        let error = JsonImporter::open_with_options(
            &path,
            format,
            BatchLimits::default(),
            &recover(),
            &JobContext::none(),
        )
        .err()
        .expect("resource errors must remain fatal");
        assert!(error.to_string().contains("exceeds the limit"));
    }
}

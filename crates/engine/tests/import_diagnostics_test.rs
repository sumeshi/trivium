use std::{fs, path::Path};

use serde_json::Value;
use trivium_engine::{
    import::{
        diagnostics::{issue, IssueSink, ISSUES_FILE},
        options::{ImportIssueAction, ImportOptions, ImportReport},
    },
    jobs::{queue_job, request_cancel, JobContext, JobKind},
    models::ProjectSummary,
    ops::{
        import_report::{
            export_import_report, export_import_report_with_job, ExportImportReportPayload,
        },
        projects::{create_project, CreateProjectPayload},
    },
    storage::update_project_meta,
    store::ProjectsStore,
};

fn create(
    store: &ProjectsStore,
    path: &Path,
    delimiter: Option<&str>,
    recover: bool,
) -> ProjectSummary {
    create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().into_owned(),
            description: None,
            delimiter: delimiter.map(str::to_owned),
            restore_annotations: Some(false),
            import_options: Some(ImportOptions {
                recover,
                ..Default::default()
            }),
        },
    )
    .unwrap()
}

fn export(id: uuid::Uuid, path: &Path, overwrite: bool) -> ExportImportReportPayload {
    ExportImportReportPayload {
        project_id: id,
        destination: path.to_string_lossy().into_owned(),
        allow_overwrite: overwrite,
    }
}

fn lines(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn full_streamed_diagnostics_survive_reopen_without_duplicating_json_passes() {
    let root = tempfile::tempdir().unwrap();
    let cases = root.path().join("cases");
    let store = ProjectsStore::new(cases.clone()).unwrap();
    let source = root.path().join("damaged.jsonl");
    fs::write(
        &source,
        format!("{}{{\"value\":\"kept\"}}\n", "not-json\n".repeat(80)),
    )
    .unwrap();
    let project = create(&store, &source, None, true);
    let report = project.meta.import_report.as_ref().unwrap();
    assert_eq!(report.issue_count, 80);
    assert_eq!(report.issues.len(), 30);
    assert_eq!(report.recover, Some(true));
    assert_eq!(report.restore_annotations, Some(false));
    assert_eq!(report.full_diagnostics, Some(true));
    let journal = lines(&store.project_dir(&project.meta.id).join(ISSUES_FILE));
    assert_eq!(journal.len(), 80, "only the schema pass journals issues");
    assert_eq!(journal[0]["sourceRecord"], 1);
    assert_eq!(journal[79]["sourceRecord"], 80);
    drop(store);
    let store = ProjectsStore::new(cases).unwrap();
    let destination = root.path().join("import-report.jsonl");
    let result =
        export_import_report(&store, export(project.meta.id, &destination, false)).unwrap();
    assert_eq!(result.issues_written, 80);
    assert!(result.complete);
    let output = lines(&destination);
    assert_eq!(output.len(), 81);
    assert_eq!(output[0]["type"], "summary");
    assert_eq!(output[0]["diagnosticsComplete"], true);
    assert_eq!(
        output[0]["project"]["sourceSha256"],
        project.meta.source_sha256.as_deref().unwrap()
    );
    assert_eq!(output[0]["report"]["issues"], serde_json::json!([]));
    for (index, entry) in output[1..].iter().enumerate() {
        assert_eq!(entry["type"], "issue");
        assert_eq!(entry["sourceRecord"], index + 1);
        assert_eq!(entry["action"], "skipped");
        assert!(
            !entry.to_string().contains("not-json"),
            "do not include the source record body"
        );
    }
}

#[test]
fn actual_csv_settings_are_saved_and_clean_imports_leave_no_empty_journal() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let source = root.path().join("tab.csv");
    fs::write(&source, "value\tnote\n00123\tkept\n").unwrap();
    let project = create(&store, &source, Some("\t"), false);
    let report = project.meta.import_report.as_ref().unwrap();
    assert_eq!(report.delimiter.as_deref(), Some("\t"));
    assert_eq!(report.recover, Some(false));
    assert_eq!(report.restore_annotations, Some(false));
    assert_eq!(report.full_diagnostics, Some(true));
    assert_eq!(report.issue_count, 0);
    assert!(!store
        .project_dir(&project.meta.id)
        .join(ISSUES_FILE)
        .exists());
    let destination = root.path().join("clean-report.jsonl");
    let result =
        export_import_report(&store, export(project.meta.id, &destination, false)).unwrap();
    assert!(result.complete);
    assert_eq!(result.issues_written, 0);
    assert_eq!(lines(&destination).len(), 1);
}

#[test]
fn only_finalized_diagnostics_can_be_exported_even_when_the_examples_are_complete() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    for count in [0, 1, 40] {
        let source = root.path().join(format!("source-{count}.jsonl"));
        fs::write(
            &source,
            format!("{}{{\"value\":1}}\n", "[]\n".repeat(count)),
        )
        .unwrap();
        let project = create(&store, &source, None, true);
        let destination = root.path().join(format!("report-{count}.jsonl"));
        fs::write(&destination, "previous output").unwrap();
        for finalized in [None, Some(false)] {
            update_project_meta(&store.project_dir(&project.meta.id), |meta| {
                meta.import_report.as_mut().unwrap().full_diagnostics = finalized;
            })
            .unwrap();
            let error =
                export_import_report(&store, export(project.meta.id, &destination, true))
                    .unwrap_err();
            assert!(error.to_string().contains("incomplete or invalid"));
            assert_eq!(fs::read_to_string(&destination).unwrap(), "previous output");
        }
    }
}

#[test]
fn diagnostic_export_obeys_overwrite_consent_and_preserves_managed_case_files() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let source = root.path().join("ordinary.csv");
    fs::write(&source, "value\nkept\n").unwrap();
    let project = create(&store, &source, None, false);
    let destination = root.path().join("existing.jsonl");
    fs::write(&destination, "unchanged").unwrap();
    let error =
        export_import_report(&store, export(project.meta.id, &destination, false)).unwrap_err();
    assert!(error
        .to_string()
        .starts_with("Overwrite confirmation required:"));
    assert_eq!(fs::read_to_string(&destination).unwrap(), "unchanged");
    export_import_report(&store, export(project.meta.id, &destination, true)).unwrap();
    assert_eq!(lines(&destination)[0]["type"], "summary");
    let managed = store.project_dir(&project.meta.id).join("data.parquet");
    let before = fs::read(&managed).unwrap();
    assert!(export_import_report(&store, export(project.meta.id, &managed, true)).is_err());
    assert_eq!(fs::read(managed).unwrap(), before);
    let guard = queue_job(JobKind::Export, Some(project.meta.id)).unwrap();
    assert!(request_cancel(guard.id()));
    let job = JobContext::with_job(guard, None);
    let before = fs::read(&destination).unwrap();
    assert!(export_import_report_with_job(
        &store,
        export(project.meta.id, &destination, true),
        &job
    )
    .is_err());
    assert_eq!(fs::read(destination).unwrap(), before);
}

#[test]
fn corrupted_or_missing_complete_journal_never_exports_an_apparently_complete_report() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let source = root.path().join("bad.jsonl");
    fs::write(&source, "invalid\n{\"value\":1}\n").unwrap();
    let project = create(&store, &source, None, true);
    let journal = store.project_dir(&project.meta.id).join(ISSUES_FILE);
    let original = fs::read(&journal).unwrap();
    let destination = root.path().join("saved.jsonl");
    fs::write(&destination, "previous output").unwrap();
    for damaged in [
        b"not JSON\n".as_slice(),
        b"",
        b"{\"sourceRecord\":1,\"action\":\"skipped\",\"reason\":\"changed\"}\n",
    ] {
        fs::write(&journal, damaged).unwrap();
        assert!(export_import_report(&store, export(project.meta.id, &destination, true)).is_err());
        assert_eq!(fs::read_to_string(&destination).unwrap(), "previous output");
    }
    fs::remove_file(&journal).unwrap();
    assert!(export_import_report(&store, export(project.meta.id, &destination, true)).is_err());
    assert_eq!(fs::read_to_string(&destination).unwrap(), "previous output");
    fs::write(&journal, original).unwrap();
    update_project_meta(&store.project_dir(&project.meta.id), |meta| {
        let report = meta.import_report.as_mut().unwrap();
        report.issue_count = 0;
        report.issues.clear();
    })
    .unwrap();
    assert!(export_import_report(&store, export(project.meta.id, &destination, true)).is_err());
    assert_eq!(fs::read_to_string(destination).unwrap(), "previous output");
}

#[test]
fn journal_write_failures_and_incorrect_counts_cannot_be_finalized() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(ISSUES_FILE);
    let sink = IssueSink::create(&path).unwrap();
    fs::create_dir(&path).unwrap();
    let mut report = ImportReport::default();
    issue(
        &mut report,
        Some(&sink),
        Some(7),
        ImportIssueAction::Skipped,
        "invalid record",
    );
    assert_eq!(report.issue_count, 1);
    assert!(sink
        .finish(1)
        .unwrap_err()
        .to_string()
        .contains("diagnostics"));
    fs::remove_dir(path).unwrap();
    let sink = IssueSink::create(&root.path().join("count.jsonl")).unwrap();
    assert!(sink.finish(1).is_err());
    assert!(sink.finish(0).is_ok());
}

//! Export an import's complete diagnostics without loading them all into RAM.
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    error::AppError,
    export::atomic::{validate_destination, write_atomically},
    import::{
        diagnostics::ISSUES_FILE,
        options::{ImportIssue, ImportReport},
    },
    jobs::JobContext,
    store::ProjectsStore,
};

const MAX_ISSUE_LINE_BYTES: usize = 16 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportImportReportPayload {
    pub project_id: Uuid,
    pub destination: String,
    #[serde(default)]
    pub allow_overwrite: bool,
}

#[derive(Debug, Clone, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ExportImportReportResult {
    pub destination: String,
    pub issues_written: usize,
    /// Whether every recorded issue is present, independently of source recovery.
    pub complete: bool,
}

pub fn export_import_report(
    store: &ProjectsStore,
    payload: ExportImportReportPayload,
) -> Result<ExportImportReportResult, AppError> {
    export_import_report_with_job(store, payload, &JobContext::none())
}

pub fn export_import_report_with_job(
    store: &ProjectsStore,
    payload: ExportImportReportPayload,
    job: &JobContext,
) -> Result<ExportImportReportResult, AppError> {
    job.check_cancelled()?;
    let meta = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let report = meta
        .import_report
        .as_ref()
        .ok_or_else(|| AppError::Message("This case has no saved import report.".into()))?;
    if report.full_diagnostics != Some(true) || report.issues.len() > report.issue_count {
        return Err(invalid_journal());
    }
    let destination = PathBuf::from(&payload.destination);
    let mut summary_report = report.clone();
    summary_report.issues.clear();
    let written = write_atomically(
        &destination,
        job,
        || validate_destination(&destination, store, payload.allow_overwrite),
        |_, writer| {
            write_line(
                writer,
                &json!({
                    "type": "summary", "version": 1, "diagnosticsComplete": true,
                    "project": { "id": meta.id, "name": meta.name,
                        "sourceFileName": meta.source_file_name, "sourceSha256": meta.source_sha256,
                        "sourceFormat": meta.source_format, "sourceSize": meta.source_size },
                    "report": summary_report,
                }),
            )?;
            let path = store.project_dir(&meta.id).join(ISSUES_FILE);
            let metadata = match std::fs::symlink_metadata(&path) {
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound && report.issue_count == 0 =>
                {
                    return Ok(0)
                }
                result => result.context("failed to inspect saved import diagnostics")?,
            };
            if !metadata.file_type().is_file() {
                return Err(AppError::Message(
                    "Saved import diagnostics are not a regular case file.".into(),
                ));
            }
            let file = crate::source_file::open_regular(&path)
                .context("failed to open saved import diagnostics")?;
            write_journal(BufReader::new(file), writer, report, job)
        },
    )?;
    Ok(ExportImportReportResult {
        destination: payload.destination,
        issues_written: written,
        complete: true,
    })
}

fn write_journal(
    mut reader: impl BufRead,
    writer: &mut impl Write,
    report: &ImportReport,
    job: &JobContext,
) -> Result<usize, AppError> {
    let mut line = Vec::new();
    let mut count = 0;
    loop {
        job.check_cancelled()?;
        line.clear();
        loop {
            let bytes = reader
                .fill_buf()
                .context("failed to read saved import diagnostics")?;
            if bytes.is_empty() {
                break;
            }
            let take = bytes
                .iter()
                .position(|&byte| byte == b'\n')
                .map_or(bytes.len(), |index| index + 1);
            if line.len().saturating_add(take) > MAX_ISSUE_LINE_BYTES {
                return Err(invalid_journal());
            }
            line.extend_from_slice(&bytes[..take]);
            let ended = bytes[take - 1] == b'\n';
            reader.consume(take);
            if ended {
                break;
            }
        }
        if line.is_empty() {
            break;
        }
        if line.last() != Some(&b'\n') || count >= report.issue_count {
            return Err(invalid_journal());
        }
        let issue: ImportIssue = serde_json::from_slice(&line).map_err(|_| invalid_journal())?;
        if issue.reason.chars().count() > 320
            || report
                .issues
                .get(count)
                .is_some_and(|sample| sample != &issue)
        {
            return Err(invalid_journal());
        }
        write_issue(writer, &issue)?;
        count += 1;
    }
    if count != report.issue_count {
        return Err(invalid_journal());
    }
    Ok(count)
}

fn write_issue(writer: &mut impl Write, issue: &ImportIssue) -> Result<(), AppError> {
    write_line(
        writer,
        &json!({ "type": "issue", "sourceRecord": issue.source_record,
        "action": issue.action, "reason": issue.reason }),
    )
}

fn write_line(writer: &mut impl Write, value: &impl Serialize) -> Result<(), AppError> {
    serde_json::to_writer(&mut *writer, value).context("failed to write import report")?;
    writer
        .write_all(b"\n")
        .context("failed to write import report")?;
    Ok(())
}

fn invalid_journal() -> AppError {
    AppError::Message(
        "Saved import diagnostics are incomplete or invalid. No report was exported.".into(),
    )
}

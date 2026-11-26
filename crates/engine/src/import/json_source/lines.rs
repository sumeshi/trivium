//! UTF-8 and selected legacy/Unicode JSONL share exactly the same physical-line
//! traversal in schema discovery and batch writing. No replacement text or
//! placeholder records are fed into the source JSON.
use std::{
    io::{Seek, SeekFrom},
    path::Path,
};

use serde_json::Value;

use crate::{
    error::AppError,
    import::{
        diagnostics::{self, IssueSink},
        options::{ImportIssueAction, ImportReport, TextEncoding},
        record_limit::{RecordBudget, MAX_RECORD_BYTES},
        strict_json,
        text_encoding::{label, resolve_encoding},
    },
    jobs::JobContext,
};

mod decode;
mod reader;
use decode::DecodeError;

pub(super) fn for_each_line<F>(
    path: &Path,
    requested: TextEncoding,
    recover: bool,
    job: &JobContext,
    sink: Option<&IssueSink>,
    visit: &mut F,
) -> Result<ImportReport, AppError>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    job.check_cancelled()?;
    let mut file = crate::source_file::open_regular(path)
        .map_err(|error| AppError::Message(format!("Failed to open file: {error}")))?;
    let (encoding, skip, _) = resolve_encoding(&mut file, requested)?;
    file.seek(SeekFrom::Start(skip))
        .map_err(|error| AppError::Message(format!("Failed to read file: {error}")))?;
    let (width, big_endian) = match encoding {
        TextEncoding::Utf16Le => (2, false),
        TextEncoding::Utf16Be => (2, true),
        TextEncoding::Utf32Le => (4, false),
        TextEncoding::Utf32Be => (4, true),
        _ => (1, false),
    };
    let mut reader = reader::Lines::new(file, width, big_endian, MAX_RECORD_BYTES);
    // Project creation records the original encoding/BOM once, independently
    // from parser diagnostics (which must match across both import passes).
    let mut report = ImportReport::default();
    let mut line = 0usize;
    while let Some(raw) = reader.next(job, recover)? {
        line += 1;
        job.check_cancelled()?;
        if line.is_multiple_of(1_024) {
            job.report_progress(line as u64, None, Some("Scanning JSON".into()));
        }
        if raw.incomplete_unit && recover {
            report.unreadable_tail = true;
            diagnostics::issue(&mut report, sink, Some(line), ImportIssueAction::UnreadableTail,
                format!("Incomplete {} unit at end of source. The unreadable remainder was not imported.", label(encoding)));
            break;
        }
        let decoded = if raw.oversized {
            Err(DecodeError::Record(format!(
                "JSONL record exceeds the {MAX_RECORD_BYTES}-byte record limit."
            )))
        } else {
            decode::decode(&raw.bytes, encoding, MAX_RECORD_BYTES, job)
        };
        let text = match decoded {
            Ok(text) => text,
            Err(DecodeError::Fatal(error)) => return Err(error),
            Err(DecodeError::Record(reason)) => {
                reject(&mut report, sink, line, reason, recover)?;
                continue;
            }
        };
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        let budget = RecordBudget::new(job);
        let parsed = strict_json::from_reader(budget.reader(text.as_bytes()));
        // An interrupted parse is an operation failure, never a skipped row.
        job.check_cancelled()?;
        let value = match parsed {
            Ok(value) if value.is_object() => value,
            Ok(_) => {
                if !recover {
                    return Err(AppError::Message(format!(
                        "JSONL line {line} is not a JSON object."
                    )));
                }
                reject(
                    &mut report,
                    sink,
                    line,
                    "Record is not a JSON object.".into(),
                    recover,
                )?;
                continue;
            }
            Err(error) => {
                reject(&mut report, sink, line, error.to_string(), recover)?;
                continue;
            }
        };
        report.source_records += 1;
        visit(line, value)?;
        report.imported_records += 1;
    }
    job.check_cancelled()?;
    if report.imported_records == 0 && report.issue_count > 0 {
        let first = &report.issues[0];
        let position = first
            .source_record
            .map(|record| format!(" at source record {record}"))
            .unwrap_or_default();
        return Err(AppError::Message(format!(
            "No readable JSON records were found. No project was created. First issue{position}: {}", first.reason
        )));
    }
    Ok(report)
}

fn reject(
    report: &mut ImportReport,
    sink: Option<&IssueSink>,
    line: usize,
    reason: String,
    recover: bool,
) -> Result<(), AppError> {
    if !recover {
        return Err(AppError::Message(format!(
            "Failed to parse JSONL line {line}: {reason}"
        )));
    }
    report.source_records += 1;
    report.skipped_records += 1;
    diagnostics::issue(report, sink, Some(line), ImportIssueAction::Skipped, reason);
    Ok(())
}

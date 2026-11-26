//! Recovery never searches for a guessed next JSON boundary. JSONL has
//! physical line boundaries; JSON arrays expose only fully parsed elements.

use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::de::{Deserializer as _, SeqAccess, Visitor};
use serde_json::{value::RawValue, Deserializer, Value};

use super::{starts_with_array, strip_utf8_bom, RecordFormat};
use crate::error::AppError;
use crate::import::options::{ImportIssueAction, ImportReport};
use crate::import::strict_json;
use crate::jobs::JobContext;

fn skip(report: &mut ImportReport, record: usize, reason: impl AsRef<str>) {
    report.skipped_records += 1;
    report.issue(Some(record), ImportIssueAction::Skipped, reason);
}

fn object(raw: &[u8]) -> Result<Value, String> {
    let value = strict_json::from_reader(raw).map_err(|error| error.to_string())?;
    if value.is_object() {
        Ok(value)
    } else {
        Err("Record is not a JSON object.".into())
    }
}

fn progress(job: &JobContext, records: usize) -> Result<(), AppError> {
    if records.is_multiple_of(1_024) {
        job.report_progress(records as u64, None, Some("Scanning JSON".into()));
    }
    job.check_cancelled()
}

fn json_lines<F>(
    mut reader: BufReader<File>,
    job: &JobContext,
    visit: &mut F,
) -> Result<ImportReport, AppError>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    let mut report = ImportReport::default();
    let mut bytes = Vec::new();
    let mut line = 0;
    loop {
        job.check_cancelled()?;
        bytes.clear();
        if reader.read_until(b'\n', &mut bytes).map_err(read_error)? == 0 {
            break;
        }
        line += 1;
        progress(job, line)?;
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text.trim(),
            Err(error) => {
                report.source_records += 1;
                skip(&mut report, line, format!("Invalid UTF-8: {error}"));
                continue;
            }
        };
        if text.is_empty() {
            continue;
        }
        report.source_records += 1;
        match object(text.as_bytes()) {
            Ok(value) => {
                visit(line, value)?;
                report.imported_records += 1;
            }
            Err(reason) => skip(&mut report, line, reason),
        }
    }
    Ok(report)
}

struct ArrayVisitor<'a, F> {
    visit: &'a mut F,
    report: &'a mut ImportReport,
    fatal: &'a mut Option<AppError>,
    job: &'a JobContext,
}

impl<'de, F> Visitor<'de> for ArrayVisitor<'_, F>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an array of JSON objects")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        loop {
            if let Err(error) = self.job.check_cancelled() {
                *self.fatal = Some(error);
                return Err(serde::de::Error::custom("import cancelled"));
            }
            let Some(raw) = seq.next_element::<Box<RawValue>>()? else {
                break;
            };
            self.report.source_records += 1;
            let record = self.report.source_records;
            let result = match object(raw.get().as_bytes()) {
                Ok(value) => (self.visit)(record, value).map(|()| {
                    self.report.imported_records += 1;
                }),
                Err(reason) => {
                    skip(self.report, record, reason);
                    Ok(())
                }
            }
            .and_then(|()| progress(self.job, record));
            if let Err(error) = result {
                *self.fatal = Some(error);
                return Err(serde::de::Error::custom("record visitor failed"));
            }
        }
        Ok(())
    }
}

fn json_array<F>(
    reader: BufReader<File>,
    job: &JobContext,
    visit: &mut F,
) -> Result<ImportReport, AppError>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    let mut report = ImportReport::default();
    let mut fatal = None;
    let mut deserializer = Deserializer::from_reader(reader);
    let result = deserializer.deserialize_seq(ArrayVisitor {
        visit,
        report: &mut report,
        fatal: &mut fatal,
        job,
    });
    if let Some(error) = fatal {
        return Err(error);
    }
    let (result, next_record) = match result {
        Ok(()) => (deserializer.end(), None),
        Err(error) => (Err(error), Some(report.source_records + 1)),
    };
    if let Err(error) = result {
        if error.is_io() {
            return Err(AppError::Message(format!("Failed to read JSON: {error}")));
        }
        report.unreadable_tail = true;
        report.issue(
            next_record,
            ImportIssueAction::UnreadableTail,
            format!("Stopped at an unreadable JSON tail: {error}. Remaining records are unknown."),
        );
    }
    Ok(report)
}

fn read_error(error: std::io::Error) -> AppError {
    AppError::Message(format!("Failed to read file: {error}"))
}

pub(super) fn for_each_record<F>(
    path: &Path,
    format: RecordFormat,
    job: &JobContext,
    mut visit: F,
) -> Result<ImportReport, AppError>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    job.check_cancelled()?;
    let file = File::open(path)
        .map_err(|error| AppError::Message(format!("Failed to open file: {error}")))?;
    let mut reader = BufReader::new(file);
    strip_utf8_bom(&mut reader)?;
    let report = match format {
        RecordFormat::JsonLines => json_lines(reader, job, &mut visit)?,
        RecordFormat::Json if starts_with_array(&mut reader)? => {
            json_array(reader, job, &mut visit)?
        }
        RecordFormat::Json => {
            // A single malformed object has no independently safe boundary
            // from which another source record could be recovered.
            let value = strict_json::from_reader(reader)
                .map_err(|error| AppError::Message(format!("Failed to parse JSON: {error}")))?;
            if !value.is_object() {
                return Err(AppError::Message(
                    "The JSON file must contain an object or an array of objects.".into(),
                ));
            }
            visit(1, value)?;
            ImportReport {
                source_records: 1,
                imported_records: 1,
                ..Default::default()
            }
        }
    };
    job.check_cancelled()?;
    if report.imported_records == 0 && report.issue_count > 0 {
        let first = &report.issues[0];
        let position = first
            .source_record
            .map(|record| format!(" at source record {record}"))
            .unwrap_or_default();
        return Err(AppError::Message(format!(
            "No readable JSON records were found. No project was created. First issue{position}: {}",
            first.reason
        )));
    }
    Ok(report)
}

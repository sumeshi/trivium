//! Recovery never searches for a guessed next JSON boundary. JSONL has
//! physical line boundaries; JSON arrays expose only fully parsed elements.

use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use serde::de::{Deserializer as _, SeqAccess, Visitor};
use serde_json::{value::RawValue, Deserializer, Value};

use super::{starts_with_array, strip_utf8_bom};
use crate::error::AppError;
use crate::import::strict_json;
use crate::import::{
    diagnostics::{self, IssueSink},
    options::{ImportIssueAction, ImportReport},
    record_limit::RecordBudget,
};
use crate::jobs::JobContext;

fn skip(
    report: &mut ImportReport,
    sink: Option<&IssueSink>,
    record: usize,
    reason: impl AsRef<str>,
) {
    report.skipped_records += 1;
    diagnostics::issue(
        report,
        sink,
        Some(record),
        ImportIssueAction::Skipped,
        reason,
    );
}

fn object(raw: &[u8], job: &JobContext) -> Result<Value, String> {
    let value = strict_json::from_reader(RecordBudget::new(job).reader(raw))
        .map_err(|error| error.to_string())?;
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

struct ArrayVisitor<'a, F> {
    visit: &'a mut F,
    report: &'a mut ImportReport,
    fatal: &'a mut Option<AppError>,
    job: &'a JobContext,
    budget: &'a RecordBudget,
    sink: Option<&'a IssueSink>,
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
            self.budget.reset();
            let Some(raw) = seq.next_element::<Box<RawValue>>()? else {
                break;
            };
            self.report.source_records += 1;
            let record = self.report.source_records;
            let result = match object(raw.get().as_bytes(), self.job) {
                Ok(value) => (self.visit)(record, value).map(|()| {
                    self.report.imported_records += 1;
                }),
                Err(reason) => {
                    skip(self.report, self.sink, record, reason);
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
    sink: Option<&IssueSink>,
    visit: &mut F,
) -> Result<ImportReport, AppError>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    json_array_with_budget(reader, job, sink, visit, &RecordBudget::new(job))
}

fn json_array_with_budget<R: Read, F>(
    reader: R,
    job: &JobContext,
    sink: Option<&IssueSink>,
    visit: &mut F,
    budget: &RecordBudget,
) -> Result<ImportReport, AppError>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    let mut report = ImportReport::default();
    let mut fatal = None;
    let mut deserializer = Deserializer::from_reader(budget.reader(reader));
    let result = deserializer.deserialize_seq(ArrayVisitor {
        visit,
        report: &mut report,
        fatal: &mut fatal,
        job,
        budget,
        sink,
    });
    job.check_cancelled()?;
    if let Some(error) = fatal {
        return Err(error);
    }
    let (result, next_record) = match result {
        Ok(()) => {
            budget.reset();
            (deserializer.end(), None)
        }
        Err(error) => (Err(error), Some(report.source_records + 1)),
    };
    if let Err(error) = result {
        if error.is_io() && !budget.exceeded() {
            return Err(AppError::Message(format!("Failed to read JSON: {error}")));
        }
        report.unreadable_tail = true;
        diagnostics::issue(
            &mut report,
            sink,
            next_record,
            ImportIssueAction::UnreadableTail,
            format!("Stopped at an unreadable JSON tail: {error}. Remaining records are unknown."),
        );
    }
    Ok(report)
}

pub(super) fn for_each_record<F>(
    path: &Path,
    job: &JobContext,
    sink: Option<&IssueSink>,
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
    let report = if starts_with_array(&mut reader, job)? {
        json_array(reader, job, sink, &mut visit)?
    } else {
        // A single malformed object has no independently safe boundary
        // from which another source record could be recovered.
        let value = strict_json::from_reader(RecordBudget::new(job).reader(reader));
        job.check_cancelled()?;
        let value =
            value.map_err(|error| AppError::Message(format!("Failed to parse JSON: {error}")))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn array_limit_keeps_only_its_complete_prefix_and_reports_unknown_tail() {
        let job = JobContext::none();
        let budget = RecordBudget::with_limit(&job, 16);
        let mut values = Vec::new();
        let report = json_array_with_budget(
            Cursor::new(br#"[{"v":1},{"v":"12345678901234567890"},{"v":3}]"#),
            &job,
            None,
            &mut |record, value| {
                values.push((record, value));
                Ok(())
            },
            &budget,
        )
        .unwrap();
        assert_eq!(values, [(1, serde_json::json!({"v":1}))]);
        assert_eq!(
            (
                report.source_records,
                report.imported_records,
                report.skipped_records
            ),
            (1, 1, 0)
        );
        assert!(report.unreadable_tail);
        assert_eq!(report.issues[0].source_record, Some(2));
        assert!(report.issues[0].reason.contains("16 byte"));
    }

    #[test]
    fn large_arrays_of_small_records_do_not_hit_the_per_record_limit() {
        let job = JobContext::none();
        let budget = RecordBudget::with_limit(&job, 12);
        let source = format!("[{}]", vec![r#"{"v":1}"#; 100].join(","));
        let mut count = 0;
        let report = json_array_with_budget(
            Cursor::new(source),
            &job,
            None,
            &mut |_, _| {
                count += 1;
                Ok(())
            },
            &budget,
        )
        .unwrap();
        assert_eq!(count, 100);
        assert_eq!(report.imported_records, 100);
        assert!(!report.unreadable_tail);
    }

    #[test]
    fn a_real_io_error_cannot_be_reclassified_as_a_recoverable_tail() {
        struct Broken {
            prefix: Cursor<&'static [u8]>,
        }
        impl Read for Broken {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                match self.prefix.read(buffer)? {
                    0 => Err(std::io::Error::other("disk failure")),
                    count => Ok(count),
                }
            }
        }
        let job = JobContext::none();
        let error = json_array_with_budget(
            Broken {
                prefix: Cursor::new(br#"[{"v":1},"#),
            },
            &job,
            None,
            &mut |_, _| Ok(()),
            &RecordBudget::with_limit(&job, 64),
        )
        .unwrap_err();
        assert!(error.to_string().contains("disk failure"));
    }
}

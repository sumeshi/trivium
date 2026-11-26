//! Full diagnostics go to an owned staging file; the metadata keeps 30 examples.
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::Mutex;

use super::options::{ImportIssue, ImportIssueAction, ImportReport};
use crate::error::AppError;

pub const ISSUES_FILE: &str = "import-issues.jsonl";

#[derive(Clone)]
pub struct IssueSink(Arc<Mutex<Journal>>);

struct Journal {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    count: usize,
    error: Option<String>,
    finished: bool,
}

impl IssueSink {
    /// The path is inside the case's staging directory. A clean import creates
    /// no empty diagnostic file; the first issue claims its name atomically.
    pub fn create(path: &Path) -> Result<Self, AppError> {
        match fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                return Err(AppError::Message(
                    "Import diagnostic file already exists.".into(),
                ))
            }
            Err(error) => return Err(failure(error)),
        }
        Ok(Self(Arc::new(Mutex::new(Journal {
            path: path.to_path_buf(),
            writer: None,
            count: 0,
            error: None,
            finished: false,
        }))))
    }

    fn record(&self, issue: &ImportIssue) {
        let mut journal = self.0.lock();
        if journal.error.is_some() {
            return;
        }
        if journal.finished {
            journal.error = Some("Import diagnostics were written after completion.".into());
            return;
        }
        let result = (|| -> std::io::Result<()> {
            if journal.writer.is_none() {
                journal.writer = Some(BufWriter::new(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&journal.path)?,
                ));
            }
            let writer = journal.writer.as_mut().unwrap();
            serde_json::to_writer(&mut *writer, issue).map_err(std::io::Error::other)?;
            writer.write_all(b"\n")
        })();
        match result {
            Ok(()) => journal.count += 1,
            Err(error) => journal.error = Some(error.to_string()),
        }
    }

    /// A failed journal is an import failure, never an apparently complete
    /// report. Flush and close before the staging directory is published.
    pub fn finish(&self, expected_issues: usize) -> Result<(), AppError> {
        let mut journal = self.0.lock();
        if let Some(error) = &journal.error {
            return Err(failure(error));
        }
        if journal.count != expected_issues {
            return Err(AppError::Message(
                "Import diagnostic count does not match the report.".into(),
            ));
        }
        if let Some(mut writer) = journal.writer.take() {
            let result = writer.flush().and_then(|()| writer.get_ref().sync_all());
            drop(writer);
            if let Err(error) = result {
                journal.error = Some(error.to_string());
                return Err(failure(error));
            }
        }
        journal.finished = true;
        Ok(())
    }
}

/// Record once in both destinations, with the same bounded reason and order.
/// I/O errors are retained by the sink and must be checked with `finish`.
pub fn issue(
    report: &mut ImportReport,
    sink: Option<&IssueSink>,
    source_record: Option<usize>,
    action: ImportIssueAction,
    reason: impl AsRef<str>,
) {
    let entry = ImportIssue {
        source_record,
        action,
        reason: reason.as_ref().chars().take(320).collect(),
    };
    report.issue(entry.source_record, entry.action.clone(), &entry.reason);
    if let Some(sink) = sink {
        sink.record(&entry);
    }
}

fn failure(error: impl std::fmt::Display) -> AppError {
    AppError::Message(format!("Failed to save import diagnostics: {error}"))
}

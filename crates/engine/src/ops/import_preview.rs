//! A bounded, read-only sample. Preview never creates or hashes a project.
use std::{
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{error::AppError, import::options::ImportOptions, jobs::JobContext};

mod csv;
mod decode;
mod json;

pub const MAX_PREVIEW_BYTES: usize = 256 * 1024;
const MAX_ROWS: usize = 20;
const MAX_COLUMNS: usize = 20;
const MAX_CELL_CHARS: usize = 200;
const MAX_DISPLAY_CHARS: usize = 20_000;

#[derive(Debug, Deserialize)]
pub struct PreviewImportPayload {
    pub path: String,
    #[serde(default)]
    pub delimiter: Option<String>,
    #[serde(default, rename = "importOptions")]
    pub import_options: Option<ImportOptions>,
}

#[derive(Debug, Clone, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ImportPreview {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub encoding: String,
    pub bom: bool,
    pub truncated: bool,
    pub suggested_delimiter: Option<String>,
    pub warnings: Vec<String>,
}

impl ImportPreview {
    fn warning(&mut self, text: impl AsRef<str>) {
        if self.warnings.len() < 8 {
            self.warnings
                .push(text.as_ref().chars().take(240).collect());
        }
    }

    fn clip(&mut self, text: &str, limit: usize) -> String {
        let mut chars = text.chars();
        let mut result: String = chars.by_ref().take(limit).collect();
        if chars.next().is_some() {
            result.pop();
            result.push('…');
            self.truncated = true;
        }
        result
    }

    fn add_row(&mut self, cells: impl IntoIterator<Item = Option<String>>) -> bool {
        let row = cells
            .into_iter()
            .take(MAX_COLUMNS)
            .map(|value| value.map(|text| self.clip(&text, MAX_CELL_CHARS)))
            .collect::<Vec<_>>();
        let existing = self
            .rows
            .iter()
            .flatten()
            .flatten()
            .map(|text| text.chars().count())
            .sum::<usize>();
        let incoming = row
            .iter()
            .flatten()
            .map(|text| text.chars().count())
            .sum::<usize>();
        if self.rows.len() == MAX_ROWS || existing.saturating_add(incoming) > MAX_DISPLAY_CHARS {
            self.truncated = true;
            return false;
        }
        self.rows.push(row);
        true
    }
}

pub fn preview_import(payload: PreviewImportPayload) -> Result<ImportPreview, AppError> {
    preview_import_with_job(payload, &JobContext::none())
}

pub fn preview_import_with_job(
    payload: PreviewImportPayload,
    job: &JobContext,
) -> Result<ImportPreview, AppError> {
    job.check_cancelled()?;
    let options = payload.import_options.unwrap_or_default();
    let path = Path::new(&payload.path);
    let mut input = crate::source_file::open_regular(path)
        .map_err(|error| AppError::Message(format!("Failed to open preview source: {error}")))?;
    let (encoding, skip, bom) =
        crate::import::text_encoding::resolve_encoding(&mut input, options.encoding)?;
    input
        .seek(SeekFrom::Start(skip))
        .map_err(|error| AppError::Message(format!("Failed to read preview: {error}")))?;
    let mut bytes = Vec::new();
    let mut buffer = [0; 16 * 1024];
    while bytes.len() <= MAX_PREVIEW_BYTES {
        job.check_cancelled()?;
        let remaining = (MAX_PREVIEW_BYTES + 1 - bytes.len()).min(buffer.len());
        let count = match input.read(&mut buffer[..remaining]) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result
                .map_err(|error| AppError::Message(format!("Failed to read preview: {error}")))?,
        };
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let complete = bytes.len() <= MAX_PREVIEW_BYTES;
    bytes.truncate(MAX_PREVIEW_BYTES);
    let mut result = ImportPreview {
        columns: Vec::new(),
        rows: Vec::new(),
        encoding: crate::import::text_encoding::label(encoding).into(),
        bom,
        truncated: !complete,
        suggested_delimiter: None,
        warnings: Vec::new(),
    };
    let (text, complete) = decode::prefix(&bytes, encoding, complete, &mut result, job)?;
    if !complete {
        result.truncated = true;
        result.warning("Only a bounded source sample is shown. The full import may find additional records or errors.");
    }
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("json") => json::preview(&text, complete, false, options.recover, &mut result, job)?,
        Some("jsonl" | "ndjson") => {
            json::preview(&text, complete, true, options.recover, &mut result, job)?
        }
        _ => csv::preview(
            &text,
            complete,
            payload.delimiter.as_deref(),
            options.has_headers,
            &mut result,
            job,
        )?,
    }
    job.check_cancelled()?;
    Ok(result)
}

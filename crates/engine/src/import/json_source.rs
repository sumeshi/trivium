//! Record streaming shared by the JSON and JSONL importers.
//!
//! Both passes use this module so the column union from pass 1 and the values
//! written in pass 2 see the same records, paths, and error messages.

use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

use serde::de::{Deserializer as _, SeqAccess, Visitor};
use serde_json::{Deserializer, Value};

use crate::error::AppError;
use crate::import::strict_json::{self, ValueSeed};

/// Record layout of a JSON source. `Json` accepts a single object or one
/// object per array element; `JsonLines` accepts one object per physical line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordFormat {
    Json,
    JsonLines,
}

/// Escapes one JSON Pointer token (`~` -> `~0`, `/` -> `~1`).
fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

/// Visits every leaf of an object tree with its absolute JSON Pointer path.
/// Arrays, scalars, and nested empty objects are leaves. An empty root object
/// is still a record, but contributes no data columns.
pub(crate) fn visit_leaves<'a, F>(value: &'a Value, prefix: &str, visit: &mut F)
where
    F: FnMut(String, &'a Value),
{
    match value {
        Value::Object(map) if !map.is_empty() || prefix.is_empty() => {
            for (key, child) in map {
                let path = format!("{}/{}", prefix, escape_pointer_token(key));
                visit_leaves(child, &path, visit);
            }
        }
        other => visit(prefix.to_string(), other),
    }
}

/// Converts a present JSON value into its display cell text. Null is kept as
/// a Polars null, not an empty string.
pub(crate) fn value_to_cell(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

/// Pulls values from a top-level JSON array one element at a time.
struct ArrayVisitor<'a, F> {
    visit: &'a mut F,
    error: &'a mut Option<AppError>,
    element: usize,
}

impl<'de, 'a, F> Visitor<'de> for ArrayVisitor<'a, F>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an array of JSON objects")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        loop {
            let record_no = self.element + 1;
            let value = seq
                .next_element_seed(ValueSeed::new(format!("/{}", self.element)))
                .map_err(|err| {
                    *self.error = Some(AppError::Message(format!(
                        "Failed to parse JSON: record {} in the JSON array: {}",
                        record_no, err
                    )));
                    err
                })?;
            let Some(value) = value else { break };
            self.element = record_no;
            if !value.is_object() {
                *self.error = Some(AppError::Message(format!(
                    "Record {} in the JSON array is not an object.",
                    record_no
                )));
                return Err(serde::de::Error::custom("array element is not an object"));
            }
            if let Err(err) = (self.visit)(record_no, value) {
                *self.error = Some(err);
                return Err(serde::de::Error::custom("record visitor failed"));
            }
        }
        Ok(())
    }
}

/// Calls `visit` once per record without keeping previous records.
///
/// JSONL is read line by line and keeps the physical line number (blank lines
/// included) for errors. A JSON array is streamed through `deserialize_seq`,
/// so each element is dropped before the next one is parsed. A single JSON
/// object is one record.
pub(crate) fn for_each_record<F>(
    path: &Path,
    format: RecordFormat,
    mut visit: F,
) -> Result<(), AppError>
where
    F: FnMut(usize, Value) -> Result<(), AppError>,
{
    match format {
        RecordFormat::JsonLines => {
            let file = File::open(path)
                .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
            for (line_no, line) in BufReader::new(file).lines().enumerate() {
                let line =
                    line.map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value = strict_json::from_reader(trimmed.as_bytes()).map_err(|e| {
                    AppError::Message(format!("Failed to parse JSONL line {}: {}", line_no + 1, e))
                })?;
                if !value.is_object() {
                    return Err(AppError::Message(format!(
                        "JSONL line {} is not a JSON object.",
                        line_no + 1
                    )));
                }
                visit(line_no + 1, value)?;
            }
            Ok(())
        }
        RecordFormat::Json => {
            let mut reader = BufReader::new(
                File::open(path)
                    .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?,
            );
            strip_utf8_bom(&mut reader)?;
            let is_array = starts_with_array(&mut reader)?;

            if is_array {
                let mut error = None;
                let mut visit = visit;
                let visitor = ArrayVisitor {
                    visit: &mut visit,
                    error: &mut error,
                    element: 0,
                };
                let mut deserializer = Deserializer::from_reader(reader);
                let result = deserializer.deserialize_seq(visitor);
                if let Some(error) = error {
                    return Err(error);
                }
                result.map_err(|e| AppError::Message(format!("Failed to parse JSON: {}", e)))?;
                deserializer
                    .end()
                    .map_err(|e| AppError::Message(format!("Failed to parse JSON: {}", e)))?;
                Ok(())
            } else {
                let value = strict_json::from_reader(reader)
                    .map_err(|e| AppError::Message(format!("Failed to parse JSON: {}", e)))?;
                if !value.is_object() {
                    return Err(AppError::Message(
                        "The JSON file must contain an object or an array of objects.".into(),
                    ));
                }
                visit(1, value)
            }
        }
    }
}

/// Looks past arbitrarily long leading whitespace without retaining it.
/// Rewind before parsing so syntax errors keep their original line/column.
fn starts_with_array(reader: &mut BufReader<File>) -> Result<bool, AppError> {
    let read_error = |error| AppError::Message(format!("Failed to read file: {}", error));
    let start = reader.stream_position().map_err(read_error)?;
    let is_array = loop {
        let buffer = reader.fill_buf().map_err(read_error)?;
        if let Some(byte) = buffer.iter().find(|byte| !byte.is_ascii_whitespace()) {
            break *byte == b'[';
        }
        let consumed = buffer.len();
        if consumed == 0 {
            break false;
        }
        reader.consume(consumed);
    };
    reader.seek(SeekFrom::Start(start)).map_err(read_error)?;
    Ok(is_array)
}

/// Drops a leading UTF-8 BOM from a JSON document. JSONL does not use this.
fn strip_utf8_bom(reader: &mut BufReader<File>) -> Result<(), AppError> {
    let buffer = reader
        .fill_buf()
        .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
    if buffer.starts_with(&[0xEF, 0xBB, 0xBF]) {
        reader.consume(3);
    }
    Ok(())
}

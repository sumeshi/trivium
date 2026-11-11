use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::error::AppError;
use crate::import::jsonl::build_import;
use crate::models::ImportedData;

/// Reads a JSON file containing a single object or an array of objects.
///
/// A single object becomes one record and each array element becomes one
/// record. The record-to-column conversion is shared with JSONL, so nested
/// objects flatten to JSON Pointer paths and the original object is kept per
/// row. A UTF-8 BOM is ignored.
pub fn load(path: &Path) -> Result<ImportedData, AppError> {
    let mut text = fs::read_to_string(path)
        .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
    if text.starts_with('\u{feff}') {
        text.remove(0);
    }
    let value: Value = serde_json::from_str(&text)
        .map_err(|e| AppError::Message(format!("Failed to parse JSON: {}", e)))?;

    let records: Vec<(usize, Value)> = match value {
        Value::Object(_) => vec![(1, value)],
        Value::Array(items) => items
            .into_iter()
            .enumerate()
            .map(|(index, item)| {
                if item.is_object() {
                    Ok((index + 1, item))
                } else {
                    Err(AppError::Message(format!(
                        "Record {} in the JSON array is not an object.",
                        index + 1
                    )))
                }
            })
            .collect::<Result<_, _>>()?,
        _ => {
            return Err(AppError::Message(
                "The JSON file must contain an object or an array of objects.".into(),
            ))
        }
    };

    build_import(&records, "JSON")
}

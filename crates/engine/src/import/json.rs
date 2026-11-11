use std::path::Path;

use crate::error::AppError;
use crate::import::jsonl::{self, RecordFormat};
use crate::models::ImportedData;

/// Reads a JSON file containing a single object or an array of objects.
///
/// A single object becomes one record and each array element becomes one
/// record. The record-to-column conversion is shared with JSONL, so nested
/// objects flatten to JSON Pointer paths and the original object is kept per
/// row. A UTF-8 BOM is ignored. Array elements are streamed one at a time
/// instead of materializing the whole array.
pub fn load(path: &Path) -> Result<ImportedData, AppError> {
    jsonl::load_with_format(path, RecordFormat::Json)
}

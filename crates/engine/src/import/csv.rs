use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use polars::prelude::*;

use crate::error::AppError;
use crate::models::FlagEntry;
use crate::value_utils::anyvalue_to_search_string;

/// Maps a delimiter option string to a single-byte polars separator.
/// An empty string represents polars' automatic detection.
pub fn build_delimiter(delimiter: Option<&str>) -> Result<Option<u8>, AppError> {
    let Some(text) = delimiter else {
        return Ok(None);
    };
    if text.is_empty() {
        return Ok(None);
    }
    match text {
        "\t" => return Ok(Some(b'\t')),
        "," => return Ok(Some(b',')),
        ";" => return Ok(Some(b';')),
        "|" => return Ok(Some(b'|')),
        _ => {}
    }
    let bytes = text.as_bytes();
    if bytes.len() == 1 && bytes[0].is_ascii() {
        Ok(Some(bytes[0]))
    } else {
        Err(AppError::Message(format!(
            "Invalid delimiter: {}.",
            text
        )))
    }
}

/// Reads a CSV, lifts `trivium-*` columns into flag entries, and assigns `__rowid`.
pub fn load(
    path: &Path,
    delimiter: Option<u8>,
) -> Result<(DataFrame, HashMap<usize, FlagEntry>), AppError> {
    let file = File::open(path)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let parsed = match delimiter {
        Some(separator) => CsvReadOptions::default()
            .with_parse_options(CsvParseOptions::default().with_separator(separator))
            .into_reader_with_file_handle(file)
            .finish(),
        None => CsvReader::new(file).finish(),
    };
    let mut df = parsed
        .map_err(|_| AppError::Message("Failed to parse the CSV data.".into()))?;

    let imported_flags = take_trivium_columns(&mut df);
    let row_ids: Vec<i64> = (0..df.height()).map(|idx| idx as i64).collect();
    let row_id_series = Series::new("__rowid", row_ids);
    df.with_column(row_id_series)
        .map_err(|_| AppError::Message("Failed to add row ids to the dataset.".into()))?;
    Ok((df, imported_flags))
}

fn take_trivium_columns(df: &mut DataFrame) -> HashMap<usize, FlagEntry> {
    let mut imported_flags: HashMap<usize, FlagEntry> = HashMap::new();
    let has_safe = df.get_column_names().iter().any(|c| c == &"trivium-safe");
    let has_suspicious = df
        .get_column_names()
        .iter()
        .any(|c| c == &"trivium-suspicious");
    let has_critical = df
        .get_column_names()
        .iter()
        .any(|c| c == &"trivium-critical");
    let has_memo = df.get_column_names().iter().any(|c| c == &"trivium-memo");
    if !(has_safe || has_suspicious || has_critical || has_memo) {
        return imported_flags;
    }

    let safe_col = if has_safe {
        df.column("trivium-safe").ok()
    } else {
        None
    };
    let suspicious_col = if has_suspicious {
        df.column("trivium-suspicious").ok()
    } else {
        None
    };
    let critical_col = if has_critical {
        df.column("trivium-critical").ok()
    } else {
        None
    };
    let memo_col = if has_memo {
        df.column("trivium-memo").ok()
    } else {
        None
    };

    let height = df.height();
    for row_idx in 0..height {
        let mut best_flag = String::new();
        if let Some(series) = critical_col.as_ref() {
            if let Ok(v) = series.get(row_idx) {
                if let Some(text) = anyvalue_to_search_string(&v) {
                    if text != "0" && !text.is_empty() {
                        best_flag = "critical".to_string();
                    }
                }
            }
        }
        if best_flag.is_empty() {
            if let Some(series) = suspicious_col.as_ref() {
                if let Ok(v) = series.get(row_idx) {
                    if let Some(text) = anyvalue_to_search_string(&v) {
                        if text != "0" && !text.is_empty() {
                            best_flag = "suspicious".to_string();
                        }
                    }
                }
            }
        }
        if best_flag.is_empty() {
            if let Some(series) = safe_col.as_ref() {
                if let Ok(v) = series.get(row_idx) {
                    if let Some(text) = anyvalue_to_search_string(&v) {
                        if text != "0" && !text.is_empty() {
                            best_flag = "safe".to_string();
                        }
                    }
                }
            }
        }
        let memo_val = if let Some(series) = memo_col.as_ref() {
            series
                .get(row_idx)
                .ok()
                .and_then(|v| anyvalue_to_search_string(&v))
        } else {
            None
        };
        let flag_entry = FlagEntry {
            flag: best_flag.clone(),
            memo: memo_val
                .map(|m| m.trim().to_string())
                .filter(|m| !m.is_empty()),
            tags: Vec::new(),
            bookmarked: false,
        };
        if !flag_entry.flag.is_empty() || flag_entry.memo.is_some() {
            imported_flags.insert(row_idx, flag_entry);
        }
    }

    if let Ok(next) = df.drop("trivium-safe") {
        *df = next;
    }
    if let Ok(next) = df.drop("trivium-suspicious") {
        *df = next;
    }
    if let Ok(next) = df.drop("trivium-critical") {
        *df = next;
    }
    if let Ok(next) = df.drop("trivium-memo") {
        *df = next;
    }
    imported_flags
}

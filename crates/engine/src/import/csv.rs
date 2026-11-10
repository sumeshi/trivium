use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read};
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

/// Decodes a UTF-16 payload (BOM already stripped) into UTF-8 bytes.
fn utf16_bytes_to_utf8(payload: &[u8], big_endian: bool) -> Result<Vec<u8>, AppError> {
    if payload.len() % 2 != 0 {
        return Err(AppError::Message(
            "Invalid UTF-16 data: odd byte length.".into(),
        ));
    }
    let units = payload.chunks_exact(2).map(|pair| {
        if big_endian {
            u16::from_be_bytes([pair[0], pair[1]])
        } else {
            u16::from_le_bytes([pair[0], pair[1]])
        }
    });
    let mut text = String::with_capacity(payload.len());
    for decoded in char::decode_utf16(units) {
        text.push(decoded.map_err(|_| {
            AppError::Message("Invalid UTF-16 data: unpaired surrogate.".into())
        })?);
    }
    Ok(text.into_bytes())
}

/// Detects UTF-16LE/BE BOMs and returns the payload decoded as UTF-8.
/// Plain UTF-8 payloads (with or without a UTF-8 BOM) are returned unchanged.
fn decode_bom(bytes: Vec<u8>) -> Result<Vec<u8>, AppError> {
    if bytes.starts_with(&[0xFF, 0xFE]) {
        utf16_bytes_to_utf8(&bytes[2..], false)
    } else if bytes.starts_with(&[0xFE, 0xFF]) {
        utf16_bytes_to_utf8(&bytes[2..], true)
    } else {
        Ok(bytes)
    }
}

/// Reads a CSV, lifts `trivium-*` columns into flag entries, and assigns `__rowid`.
pub fn load(
    path: &Path,
    delimiter: Option<u8>,
) -> Result<(DataFrame, HashMap<usize, FlagEntry>), AppError> {
    let mut file = File::open(path)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let mut raw = Vec::new();
    file.read_to_end(&mut raw)
        .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
    let bytes = decode_bom(raw)?;
    let cursor = Cursor::new(bytes);
    let parsed = match delimiter {
        Some(separator) => CsvReadOptions::default()
            .with_parse_options(CsvParseOptions::default().with_separator(separator))
            .into_reader_with_file_handle(cursor)
            .finish(),
        None => CsvReader::new(cursor).finish(),
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

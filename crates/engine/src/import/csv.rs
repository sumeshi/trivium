use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;

use polars::prelude::*;

use crate::error::AppError;
use crate::models::{ColumnMeta, FlagEntry, ImportedData};
use crate::project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID};

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

/// Internal column id assigned to a source column by its position.
fn column_id(position: usize) -> String {
    format!("c{:06}", position + 1)
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
/// Plain UTF-8 payloads (with or without a UTF-8 BOM) are returned unchanged
/// apart from dropping the UTF-8 BOM.
fn decode_bom(bytes: Vec<u8>) -> Result<Vec<u8>, AppError> {
    if bytes.starts_with(&[0xFF, 0xFE]) {
        utf16_bytes_to_utf8(&bytes[2..], false)
    } else if bytes.starts_with(&[0xFE, 0xFF]) {
        utf16_bytes_to_utf8(&bytes[2..], true)
    } else if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        Ok(bytes[3..].to_vec())
    } else {
        Ok(bytes)
    }
}

/// Reads a CSV and assigns one internal id per source column position.
///
/// The original cell strings are kept in the `__original` JSON column. The
/// display frame is built by replacing the header row with the internal ids, so
/// duplicate or empty headers cannot drop a column. `trivium-*` columns are only
/// read as annotation data when `restore_annotations` is explicit; the columns
/// themselves are always kept as ordinary data.
pub fn load(
    path: &Path,
    delimiter: Option<u8>,
    restore_annotations: bool,
) -> Result<ImportedData, AppError> {
    let mut file = File::open(path)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let mut raw = Vec::new();
    file.read_to_end(&mut raw)
        .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
    let bytes = decode_bom(raw)?;

    // `None` keeps the existing comma default for the parser. The polars reader
    // previously performed automatic detection; the raw cell reader needs a
    // concrete separator, so the same default is applied here.
    let separator = delimiter.unwrap_or(b',');
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(separator)
        .flexible(false)
        .from_reader(Cursor::new(&bytes));
    let headers = reader
        .headers()
        .map_err(|_| AppError::Message("Failed to parse the CSV data.".into()))?
        .clone();
    let source_headers: Vec<String> = headers.iter().map(|header| header.to_string()).collect();
    if source_headers.is_empty() {
        return Err(AppError::Message(
            "The CSV data has no columns.".into(),
        ));
    }

    let mut records: Vec<csv::StringRecord> = Vec::new();
    for record in reader.records() {
        let record = record
            .map_err(|e| AppError::Message(format!("Failed to parse the CSV data: {}", e)))?;
        records.push(record);
    }

    let columns: Vec<ColumnMeta> = source_headers
        .iter()
        .enumerate()
        .map(|(order, header)| ColumnMeta {
            id: column_id(order),
            label: if header.is_empty() {
                format!("column {}", order + 1)
            } else {
                header.clone()
            },
            source: header.clone(),
            order,
        })
        .collect();

    // Every display column is the original cell text as Utf8. Cell text is
    // never parsed for storage, so leading zeros, padding, long integers, and
    // datetime spellings keep their source representation. The csv reader has
    // already resolved quoting, embedded newlines, and escaping.
    let mut series: Vec<Series> = Vec::with_capacity(columns.len());
    for column in &columns {
        let values: Vec<&str> = records
            .iter()
            .map(|record| record.get(column.order).unwrap_or(""))
            .collect();
        series.push(Series::new(column.id.as_str(), values));
    }
    let mut df = DataFrame::new(series)
        .map_err(|_| AppError::Message("Failed to build the dataset.".into()))?;
    if df.height() != records.len() {
        return Err(AppError::Message(
            "The CSV row count changed while building the dataset.".into(),
        ));
    }

    let originals: Vec<String> = records
        .iter()
        .map(|record| {
            let cells: Vec<&str> = record.iter().collect();
            serde_json::to_string(&cells)
                .map_err(|_| AppError::Message("Failed to store the original CSV record.".into()))
        })
        .collect::<Result<_, _>>()?;
    df.with_column(
        Series::new(INTERNAL_ORIGINAL, originals),
    )
    .map_err(|_| AppError::Message("Failed to store the original CSV records.".into()))?;

    let row_ids: Vec<i64> = (0..df.height()).map(|idx| idx as i64).collect();
    let row_id_series = Series::new(INTERNAL_ROW_ID, row_ids);
    df.with_column(row_id_series)
        .map_err(|_| AppError::Message("Failed to add row ids to the dataset.".into()))?;

    let imported_flags = if restore_annotations {
        extract_trivium_flags(&columns, &records)
    } else {
        HashMap::new()
    };

    Ok(ImportedData {
        frame: df,
        columns,
        flags: imported_flags,
    })
}

/// Lifts explicit `trivium-*` columns into flag entries. The columns stay in
/// the frame; this only creates the annotation side channel.
fn extract_trivium_flags(
    columns: &[ColumnMeta],
    records: &[csv::StringRecord],
) -> HashMap<usize, FlagEntry> {
    let find = |name: &str| {
        columns
            .iter()
            .find(|column| column.source == name)
            .map(|column| column.order)
    };
    let safe = find("trivium-safe");
    let suspicious = find("trivium-suspicious");
    let critical = find("trivium-critical");
    let memo = find("trivium-memo");
    if safe.is_none() && suspicious.is_none() && critical.is_none() && memo.is_none() {
        return HashMap::new();
    }

    let flag_value = |record: &csv::StringRecord, index: Option<usize>| -> Option<String> {
        index
            .and_then(|column| record.get(column))
            .map(str::to_string)
            .filter(|text| !text.is_empty() && text != "0")
    };

    let mut imported_flags: HashMap<usize, FlagEntry> = HashMap::new();
    for (row_idx, record) in records.iter().enumerate() {
        let flag = flag_value(record, critical)
            .map(|_| "critical".to_string())
            .or_else(|| flag_value(record, suspicious).map(|_| "suspicious".to_string()))
            .or_else(|| flag_value(record, safe).map(|_| "safe".to_string()))
            .unwrap_or_default();
        let memo_value = memo
            .and_then(|column| record.get(column))
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string);
        if flag.is_empty() && memo_value.is_none() {
            continue;
        }
        imported_flags.insert(
            row_idx,
            FlagEntry {
                flag,
                memo: memo_value,
                tags: Vec::new(),
                bookmarked: false,
            },
        );
    }
    imported_flags
}

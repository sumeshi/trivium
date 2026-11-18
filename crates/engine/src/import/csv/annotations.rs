//! Explicit CSV annotation restoration; ordinary imports keep these as data.
use super::FlagColumns;
use crate::{error::AppError, models::FlagEntry};

fn boolean_cell(
    record: &csv::StringRecord,
    index: Option<usize>,
    name: &str,
) -> Result<bool, AppError> {
    let Some(index) = index else { return Ok(false) };
    let original = record.get(index).unwrap_or_default();
    let value = original.trim();
    if value.is_empty() || value == "0" || value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else if value == "1" || value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else {
        Err(AppError::Message(format!(
            "Invalid {name} annotation at column {}: {original:?}. Expected 0/1 or true/false.",
            index + 1
        )))
    }
}

pub(super) fn flag_entry(
    columns: FlagColumns,
    record: &csv::StringRecord,
) -> Result<Option<FlagEntry>, AppError> {
    let mut active = Vec::new();
    for (index, name, flag) in [
        (columns.safe, "trivium-safe", "safe"),
        (columns.suspicious, "trivium-suspicious", "suspicious"),
        (columns.critical, "trivium-critical", "critical"),
    ] {
        if boolean_cell(record, index, name)? {
            active.push((index.expect("a present boolean cell"), name, flag));
        }
    }
    if active.len() > 1 {
        let values = active
            .iter()
            .map(|(index, name, _)| format!("{name}={:?}", record.get(*index).unwrap_or_default()))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(AppError::Message(format!(
            "Conflicting flag annotations: {values}. Only one flag may be true."
        )));
    }
    let flag = active
        .first()
        .map(|(_, _, flag)| *flag)
        .unwrap_or_default()
        .to_string();
    let bookmarked = boolean_cell(record, columns.bookmarked, "trivium-bookmarked")?;
    let memo_value = columns
        .memo
        .and_then(|column| record.get(column))
        .filter(|text| !text.is_empty())
        .map(str::to_string);
    // The explicit JSON column wins, including [] (which clears legacy tags).
    // Never guess JSON syntax inside an old semicolon-separated tag cell.
    let tags = if let Some(column) = columns.tags_json {
        serde_json::from_str::<Vec<String>>(record.get(column).unwrap_or_default()).map_err(
            |e| AppError::Message(format!("Invalid trivium-tags-json annotation: {}", e)),
        )?
    } else {
        columns
            .tags
            .and_then(|column| record.get(column))
            .map(|text| {
                text.split(';')
                    .map(str::trim)
                    .filter(|tag| !tag.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };
    if flag.is_empty() && memo_value.is_none() && tags.is_empty() && !bookmarked {
        return Ok(None);
    }
    Ok(Some(FlagEntry {
        flag,
        memo: memo_value,
        tags,
        bookmarked,
    }))
}

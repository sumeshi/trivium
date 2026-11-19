use std::io::Write;
use std::path::Path;

use anyhow::{bail, Context, Result};
use csv::{ReaderBuilder, WriterBuilder};

use crate::flags::normalize_flag_value;
use crate::models::{ColumnMeta, IocCellMatch, IocEntry};

pub const MAX_IOC_CELL_VALUE_BYTES: usize = 64 * 1024;

pub fn load_ioc_entries(project_dir: &Path) -> Result<Vec<IocEntry>> {
    let Some(data) = crate::storage::load_setting_bytes(project_dir, "iocs")? else {
        return Ok(Vec::new());
    };
    let mut entries: Vec<IocEntry> =
        serde_json::from_slice(&data).context("failed to parse saved IOC definitions")?;
    for entry in &mut entries {
        // Keep invalid legacy values visible for correction. Losing the value
        // here used to let the editor silently turn an unknown flag into Safe.
        let normalized = normalize_flag_value(&entry.flag);
        entry.flag = if normalized.is_empty() {
            entry.flag.trim().to_string()
        } else {
            normalized
        };
        entry.tag = entry.tag.trim().to_string();
        entry.query = entry.query.trim().to_string();
    }
    Ok(entries)
}

pub fn save_ioc_entries(project_dir: &Path, entries: &[IocEntry]) -> Result<()> {
    // Validate the entire replacement before writing any project setting.
    let entries = prepare_ioc_entries(entries.to_vec())?;
    save_prepared_ioc_entries(project_dir, &entries)
}

pub(crate) fn save_prepared_ioc_entries(project_dir: &Path, entries: &[IocEntry]) -> Result<()> {
    let data = serde_json::to_vec(entries).context("failed to serialize ioc entries")?;
    crate::storage::save_setting_bytes(project_dir, "iocs", &data)
}

pub fn read_ioc_csv(path: &Path) -> Result<Vec<IocEntry>> {
    let source = crate::source_file::open_regular(path)
        .with_context(|| format!("failed to open IOC CSV {:?}", path))?;
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(source);
    let (column_at, value_at) = cell_headers(reader.headers()?)?;
    let mut entries = Vec::new();
    for (index, record) in reader.records().enumerate() {
        let record = record.with_context(|| "failed to read IOC CSV record")?;
        let flag_value = record.get(0).unwrap_or("");
        let line = record
            .position()
            .map(|position| position.line())
            .unwrap_or(index as u64 + 2);
        let flag = flag_value.to_string();
        let tag = record.get(1).unwrap_or("").to_string();
        let query = record.get(2).unwrap_or("").to_string();
        let cell = match (column_at, value_at) {
            (Some(column), Some(value)) => {
                let column = record.get(column).unwrap_or("");
                let value = record.get(value).unwrap_or("");
                if column.is_empty() && value.is_empty() {
                    None
                } else {
                    Some(IocCellMatch {
                        column: column.into(),
                        value: value.into(),
                    })
                }
            }
            _ => None,
        };
        if let Some(entry) = prepare_ioc_entry(
            IocEntry {
                flag,
                tag,
                query,
                cell,
            },
            &format!("IOC CSV line {line}"),
        )? {
            entries.push(entry);
        }
    }
    Ok(entries)
}

pub(crate) fn write_ioc_csv_to_writer(entries: &[IocEntry], writer: impl Write) -> Result<()> {
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(writer);
    let cell_columns = entries.iter().any(|entry| entry.cell.is_some());
    writer
        .write_record(if cell_columns {
            vec!["flag", "tag", "query", "column", "value"]
        } else {
            vec!["flag", "tag", "query"]
        })
        .context("failed to write IOC CSV header")?;
    for entry in entries {
        let mut record = vec![
            entry.flag.as_str(),
            entry.tag.as_str(),
            entry.query.as_str(),
        ];
        if cell_columns {
            record.push(entry.cell.as_ref().map_or("", |cell| cell.column.as_str()));
            record.push(entry.cell.as_ref().map_or("", |cell| cell.value.as_str()));
        }
        writer
            .write_record(record)
            .context("failed to write IOC CSV row")?;
    }
    writer.flush().context("failed to flush IOC CSV writer")
}

fn validate_ioc_flag(value: &str, location: &str) -> Result<String> {
    let flag = normalize_flag_value(value);
    if flag.is_empty() {
        bail!("{location}: invalid IOC flag {value:?}. Expected Safe, Suspicious, or Critical.");
    }
    Ok(flag)
}

fn validate_ioc_query(query: &str, location: &str) -> Result<()> {
    crate::search::validate_search_query(query)
        .map_err(|error| anyhow::anyhow!("{location}: {error}"))
}

pub fn prepare_ioc_entries(entries: Vec<IocEntry>) -> Result<Vec<IocEntry>> {
    let mut prepared = Vec::with_capacity(entries.len());
    for (index, entry) in entries.into_iter().enumerate() {
        if let Some(entry) = prepare_ioc_entry(entry, &format!("IOC rule {}", index + 1))? {
            prepared.push(entry);
        }
    }
    prepared.sort_by(|a, b| a.tag.cmp(&b.tag));
    Ok(prepared)
}

fn prepare_ioc_entry(entry: IocEntry, location: &str) -> Result<Option<IocEntry>> {
    let flag = validate_ioc_flag(&entry.flag, location)?;
    let query = entry.query.trim().to_string();
    if let Some(cell) = &entry.cell {
        if !query.is_empty() {
            bail!("{location}: choose a query or a literal cell match, not both.");
        }
        if cell.column.trim().is_empty() {
            bail!("{location}: a literal cell match requires a nonempty column.");
        }
        if cell.value.len() > MAX_IOC_CELL_VALUE_BYTES {
            bail!("{location}: literal cell value exceeds the 64 KiB limit.");
        }
    } else if query.is_empty() {
        return Ok(None);
    } else {
        validate_ioc_query(&query, location)?;
    }
    Ok(Some(IocEntry {
        flag,
        tag: entry.tag.trim().into(),
        query,
        cell: entry.cell,
    }))
}

/// Optional cell fields extend the legacy positional three-column CSV.
fn cell_headers(headers: &csv::StringRecord) -> Result<(Option<usize>, Option<usize>)> {
    let mut column = None;
    let mut value = None;
    for (index, header) in headers.iter().enumerate().skip(3) {
        let target = if header.trim().eq_ignore_ascii_case("column") {
            &mut column
        } else if header.trim().eq_ignore_ascii_case("value") {
            &mut value
        } else {
            continue;
        };
        if target.replace(index).is_some() {
            bail!("IOC CSV header: duplicate optional {header:?} column.");
        }
    }
    if column.is_some() != value.is_some() {
        bail!("IOC CSV header: optional column and value fields must appear together.");
    }
    Ok((column, value))
}

pub(crate) fn validate_cell_columns(entries: &[IocEntry], columns: &[ColumnMeta]) -> Result<()> {
    for (index, entry) in entries.iter().enumerate() {
        if let Some(cell) = &entry.cell {
            if !columns.iter().any(|column| column.id == cell.column) {
                bail!(
                    "IOC rule {}: column {:?} was not found in this project.",
                    index + 1,
                    cell.column
                );
            }
        }
    }
    Ok(())
}

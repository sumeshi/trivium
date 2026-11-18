use std::io::Write;
use std::path::Path;

use anyhow::{bail, Context, Result};
use csv::{ReaderBuilder, WriterBuilder};

use crate::flags::normalize_flag_value;
use crate::models::IocEntry;

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
    let data = serde_json::to_vec(&entries).context("failed to serialize ioc entries")?;
    crate::storage::save_setting_bytes(project_dir, "iocs", &data)
}

pub fn read_ioc_csv(path: &Path) -> Result<Vec<IocEntry>> {
    let source = crate::source_file::open_regular(path)
        .with_context(|| format!("failed to open IOC CSV {:?}", path))?;
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(source);
    let mut entries = Vec::new();
    for (index, record) in reader.records().enumerate() {
        let record = record.with_context(|| "failed to read IOC CSV record")?;
        let flag_value = record.get(0).unwrap_or("");
        let line = record
            .position()
            .map(|position| position.line())
            .unwrap_or(index as u64 + 2);
        let flag = validate_ioc_flag(flag_value, &format!("IOC CSV line {line}"))?;
        let tag = record.get(1).unwrap_or("").trim().to_string();
        let query = record.get(2).unwrap_or("").trim().to_string();
        if query.is_empty() {
            continue;
        }
        validate_ioc_query(&query, &format!("IOC CSV line {line}"))?;
        entries.push(IocEntry { flag, tag, query });
    }
    Ok(entries)
}

pub(crate) fn write_ioc_csv_to_writer(entries: &[IocEntry], writer: impl Write) -> Result<()> {
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(writer);
    writer
        .write_record(["flag", "tag", "query"])
        .context("failed to write IOC CSV header")?;
    for entry in entries {
        writer
            .write_record([
                entry.flag.as_str(),
                entry.tag.as_str(),
                entry.query.as_str(),
            ])
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
        let flag = validate_ioc_flag(&entry.flag, &format!("IOC rule {}", index + 1))?;
        let query = entry.query.trim().to_string();
        if !query.is_empty() {
            validate_ioc_query(&query, &format!("IOC rule {}", index + 1))?;
            prepared.push(IocEntry {
                flag,
                tag: entry.tag.trim().to_string(),
                query,
            });
        }
    }
    prepared.sort_by(|a, b| a.tag.cmp(&b.tag));
    Ok(prepared)
}

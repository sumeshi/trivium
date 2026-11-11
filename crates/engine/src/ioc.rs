use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use csv::{ReaderBuilder, WriterBuilder};

use crate::flags::{normalize_flag_value, severity_rank};
use crate::models::{IocEntry, ProjectRow};
use crate::project_io::read_project_dataframe;
use crate::storage::load_flags;

pub mod evaluate;
pub use evaluate::{
    cached_ioc_outcomes, cached_ioc_outcomes_without_frame, cached_ioc_page_outcomes,
    evaluate_iocs, IocRowOutcome,
};

/// Applies frame-level IOC outcomes to materialized page rows.
///
/// Precedence: a non-empty user flag wins over the IOC flag; matched tags are
/// appended to the memo for every matching row.
pub fn apply_ioc_outcomes_to_rows(rows: &mut [ProjectRow], outcomes: &[IocRowOutcome]) {
    for row in rows.iter_mut() {
        let Some(outcome) = outcomes.get(row.row_index) else {
            continue;
        };
        if severity_rank(&normalize_flag_value(&row.flag)) == 0 && !outcome.flag.is_empty() {
            row.flag = outcome.flag.clone();
        }
        let mut memo = row.memo.clone().unwrap_or_default();
        for tag in &outcome.tags {
            if !memo.contains(tag) {
                if !memo.is_empty() && !memo.ends_with(' ') {
                    memo.push(' ');
                }
                memo.push_str(tag);
            }
        }
        let trimmed = memo.trim().to_string();
        row.memo = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
}

pub fn load_ioc_entries(project_dir: &Path) -> Result<Vec<IocEntry>> {
    let path = project_dir.join("iocs.json");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read(&path).with_context(|| format!("failed to read ioc file {:?}", path))?;
    let mut entries: Vec<IocEntry> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse ioc file {:?}", path))?;
    for entry in &mut entries {
        entry.flag = normalize_flag_value(&entry.flag);
        entry.tag = entry.tag.trim().to_string();
        entry.query = entry.query.trim().to_string();
    }
    Ok(entries)
}

pub fn save_ioc_entries(project_dir: &Path, entries: &[IocEntry]) -> Result<()> {
    let path = project_dir.join("iocs.json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to prepare ioc dir {:?}", parent))?;
    }
    let data = serde_json::to_vec_pretty(entries).context("failed to serialize ioc entries")?;
    crate::storage::write_file_atomic(&path, &data)
}

pub fn read_ioc_csv(path: &Path) -> Result<Vec<IocEntry>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("failed to open IOC CSV {:?}", path))?;
    let mut entries = Vec::new();
    for record in reader.records() {
        let record = record.with_context(|| "failed to read IOC CSV record")?;
        let flag_value = record.get(0).unwrap_or("").trim().to_string();
        let tag = record.get(1).unwrap_or("").trim().to_string();
        let query = record.get(2).unwrap_or("").trim().to_string();
        if query.is_empty() {
            continue;
        }
        entries.push(IocEntry {
            flag: normalize_flag_value(&flag_value),
            tag,
            query,
        });
    }
    Ok(entries)
}

pub fn write_ioc_csv(entries: &[IocEntry], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create export dir {:?}", parent))?;
    }
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("failed to create IOC CSV {:?}", path))?;
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

pub fn calculate_ioc_applied_records(project_dir: &Path) -> Result<usize> {
    let parquet_path = project_dir.join("data.parquet");
    let df = read_project_dataframe(&parquet_path)?;
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path)?;
    let iocs = load_ioc_entries(project_dir)?;
    if iocs.is_empty() {
        return Ok(0);
    }

    // An IOC flag applies only where the user has not set one. Use the cached
    // path so an appended IOC set only evaluates the appended entries.
    let outcomes = cached_ioc_outcomes(project_dir, &df, &iocs);
    let mut count = 0;
    for (row_idx, outcome) in outcomes.iter().enumerate() {
        let user_flag = flags
            .get(&row_idx)
            .map(|entry| normalize_flag_value(&entry.flag))
            .unwrap_or_default();
        if severity_rank(&user_flag) == 0 && !outcome.flag.is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

pub fn prepare_ioc_entries(entries: Vec<IocEntry>) -> Vec<IocEntry> {
    let mut prepared: Vec<IocEntry> = entries
        .into_iter()
        .map(|entry| IocEntry {
            flag: normalize_flag_value(&entry.flag),
            tag: entry.tag.trim().to_string(),
            query: entry.query.trim().to_string(),
        })
        .filter(|entry| !entry.query.is_empty())
        .collect();
    prepared.sort_by(|a, b| a.tag.cmp(&b.tag));
    prepared
}

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use csv::{ReaderBuilder, WriterBuilder};

use crate::flags::{normalize_flag_value, severity_rank};
use crate::models::{IocEntry, ProjectRow};
use crate::project_io::read_project_dataframe;
use crate::search::{build_search_mask_boolean, to_rpn, tokenize_search_query, SearchToken};
use crate::storage::load_flags;
use crate::value_utils::{anyvalue_to_search_string, value_to_search_string};

pub fn row_contains_query(row: &ProjectRow, query: &str) -> bool {
    if query.trim().is_empty() {
        return false;
    }
    // Build concatenated lowercase text and per-column lowercase texts for the row
    let mut row_text = String::new();
    let mut per_col: HashMap<String, Vec<String>> = HashMap::new();
    for (col, value) in &row.data {
        if let Some(text) = value_to_search_string(value) {
            let lower = text.to_lowercase();
            if !lower.is_empty() {
                if !row_text.is_empty() {
                    row_text.push(' ');
                }
                row_text.push_str(&lower);
                per_col.insert(col.to_lowercase(), vec![lower]);
            }
        }
    }
    if row_text.is_empty() {
        return false;
    }
    // Boolean evaluation using the shared tokenizer and RPN evaluator
    let tokens = tokenize_search_query(query);
    let mut terms: Vec<(Option<String>, String)> = Vec::new();
    for t in &tokens {
        if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t {
            let key = (col.clone(), text.clone());
            if !text.is_empty() && !terms.contains(&key) {
                terms.push(key);
            }
        }
    }
    if terms.is_empty() {
        return false;
    }
    let rpn = to_rpn(&tokens);
    let mask = build_search_mask_boolean(&rpn, &terms, &vec![row_text], Some(&per_col));
    mask.get(0).copied().unwrap_or(false)
}

pub fn apply_iocs_to_rows(rows: &mut [ProjectRow], entries: &[IocEntry]) {
    if entries.is_empty() {
        return;
    }
    for row in rows {
        let mut best_flag = normalize_flag_value(&row.flag);
        let mut best_rank = severity_rank(&best_flag);
        let mut memo = row.memo.clone().unwrap_or_default();
        let mut memo_changed = false;

        for entry in entries {
            let query = entry.query.trim();
            if query.is_empty() {
                continue;
            }
            if !row_contains_query(row, query) {
                continue;
            }

            let severity = normalize_flag_value(&entry.flag);
            let severity_rank_value = severity_rank(&severity);
            // If user already set a flag, keep it (user wins). Only apply IOC when no user flag.
            if best_rank == 0 && severity_rank_value > 0 {
                best_rank = severity_rank_value;
                best_flag = severity.clone();
            }

            let tag = entry.tag.trim();
            if !tag.is_empty() {
                let token = format!("[{}]", tag);
                if !memo.contains(&token) {
                    if !memo.is_empty() && !memo.ends_with(' ') {
                        memo.push(' ');
                    }
                    memo.push_str(&token);
                    memo_changed = true;
                }
            }
        }

        // Persist the resolved flag into the row only if it was empty before
        if severity_rank(&normalize_flag_value(&row.flag)) == 0 && best_rank > 0 {
            row.flag = best_flag.clone();
        }
        if memo_changed {
            let trimmed = memo.trim().to_string();
            row.memo = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            };
        }
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
    fs::write(&path, data).with_context(|| format!("failed to write ioc file {:?}", path))
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

    let mut ioc_applied_count = 0;
    let column_names: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|column| *column != "__rowid")
        .map(|column| column.to_string())
        .collect();

    for row_idx in 0..df.height() {
        let flag_entry = flags.get(&row_idx);
        let user_flag = flag_entry
            .as_ref()
            .map(|entry| normalize_flag_value(&entry.flag))
            .unwrap_or_default();

        // Count IOC applications (only when no user flag exists)
        if severity_rank(&user_flag) == 0 && !iocs.is_empty() {
            let mut has_ioc_match = false;

            // Build searchable text once for this row
            let mut row_text = String::new();
            for column in &column_names {
                if let Ok(series) = df.column(column) {
                    if let Ok(value) = series.get(row_idx) {
                        if let Some(text) = anyvalue_to_search_string(&value) {
                            let lower = text.to_lowercase();
                            if !lower.is_empty() {
                                if !row_text.is_empty() {
                                    row_text.push(' ');
                                }
                                row_text.push_str(&lower);
                            }
                        }
                    }
                }
            }
            for entry in &iocs {
                let query = entry.query.trim();
                if query.is_empty() {
                    continue;
                }
                let tokens = tokenize_search_query(query);
                let mut terms: Vec<(Option<String>, String)> = Vec::new();
                for t in &tokens {
                    if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } =
                        t
                    {
                        let key = (col.clone(), text.clone());
                        if !text.is_empty() && !terms.contains(&key) {
                            terms.push(key);
                        }
                    }
                }
                if terms.is_empty() {
                    continue;
                }
                let rpn = to_rpn(&tokens);
                // Build single-row per-column map
                let mut single_per_col: HashMap<String, Vec<String>> = HashMap::new();
                for column in &column_names {
                    let mut s = String::new();
                    if let Ok(series) = df.column(column) {
                        if let Ok(v) = series.get(row_idx) {
                            if let Some(t) = anyvalue_to_search_string(&v) {
                                s = t.to_lowercase();
                            }
                        }
                    }
                    single_per_col.insert(column.to_lowercase(), vec![s]);
                }
                let mask = build_search_mask_boolean(
                    &rpn,
                    &terms,
                    &vec![row_text.clone()],
                    Some(&single_per_col),
                );
                if mask.get(0).copied().unwrap_or(false) {
                    has_ioc_match = true;
                    break;
                }
            }

            if has_ioc_match {
                ioc_applied_count += 1;
            }
        }
    }

    Ok(ioc_applied_count)
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

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::PathBuf;

use anyhow::Context;
use polars::prelude::*;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    error::AppError,
    flags::normalize_flag_value,
    ioc::{evaluate_iocs, load_ioc_entries},
    project_io::read_project_dataframe,
    storage::load_flags,
    store::ProjectsStore,
};

#[derive(Debug, Deserialize)]
pub struct ExportProjectPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
}

/// Exports the project data with derived trivium columns to a CSV file.
pub fn export_project(
    store: &ProjectsStore,
    payload: ExportProjectPayload,
) -> Result<(), AppError> {
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    let mut df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let mut safe_flags: Vec<i32> = vec![0; df.height()];
    let mut suspicious_flags: Vec<i32> = vec![0; df.height()];
    let mut critical_flags: Vec<i32> = vec![0; df.height()];
    let mut memo_series: Vec<String> = vec![String::new(); df.height()];

    let ioc_outcomes = if iocs.is_empty() {
        Vec::new()
    } else {
        evaluate_iocs(&df, &iocs)
    };

    for i in 0..df.height() {
        let outcome = ioc_outcomes.get(i).cloned().unwrap_or_default();
        let ioc_flag = outcome.flag;
        let memo_tags = outcome.tags;

        // B2 unified rule: a non-empty user flag wins the flag column; a user
        // entry with an empty flag (memo-only) does not block the IOC flag.
        // The user memo always survives, and matched tags are appended.
        let user_flag = flags
            .get(&i)
            .map(|entry| normalize_flag_value(entry.flag.trim()))
            .unwrap_or_default();
        let final_flag = if user_flag.is_empty() {
            ioc_flag
        } else {
            user_flag
        };
        let mut final_memo = flags
            .get(&i)
            .and_then(|entry| entry.memo.clone())
            .unwrap_or_default();

        for tag in memo_tags {
            if !final_memo.contains(&tag) {
                if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                    final_memo.push(' ');
                }
                final_memo.push_str(&tag);
            }
        }

        // F19 user tags are exported as `[tag]` tokens after the IOC tags, so
        // they survive the CSV round trip as memo text. Re-import keeps the
        // whole memo verbatim (create_project does not parse tags back out),
        // so the tag becomes part of the memo on import. That is the known
        // F19 limit; do not change the import-side memo handling for it.
        let user_tags = flags
            .get(&i)
            .map(|entry| entry.tags.clone())
            .unwrap_or_default();
        for tag in user_tags {
            let token = format!("[{}]", tag);
            if !final_memo.contains(&token) {
                if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                    final_memo.push(' ');
                }
                final_memo.push_str(&token);
            }
        }

        match final_flag.as_str() {
            "safe" => safe_flags[i] = 1,
            "suspicious" => suspicious_flags[i] = 1,
            "critical" => critical_flags[i] = 1,
            _ => {}
        }
        memo_series[i] = final_memo;
    }

    if let Ok(next) = df.drop("__rowid") {
        df = next;
    }
    if let Ok(next) = df.drop("flag") {
        df = next;
    }
    if let Ok(next) = df.drop("memo") {
        df = next;
    }

    let mut out_cols: Vec<Series> = Vec::new();
    out_cols.push(Series::new("trivium-safe", safe_flags));
    out_cols.push(Series::new("trivium-suspicious", suspicious_flags));
    out_cols.push(Series::new("trivium-critical", critical_flags));
    out_cols.push(Series::new("trivium-memo", memo_series));
    for name in df.get_column_names() {
        if let Ok(series) = df.column(name) {
            out_cols.push(series.clone());
        }
    }
    df = DataFrame::new(out_cols).map_err(|e| AppError::Other(e.into()))?;

    let destination = PathBuf::from(payload.destination);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create export dir {:?}", parent))
            .map_err(AppError::from)?;
    }

    let file = File::create(&destination)
        .with_context(|| format!("failed to create export file {:?}", destination))
        .map_err(AppError::from)?;
    let mut writer = CsvWriter::new(BufWriter::new(file));
    let mut df_out = df.clone();
    writer
        .finish(&mut df_out)
        .context("failed to write export CSV")
        .map_err(AppError::from)?;
    Ok(())
}

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::PathBuf;

use anyhow::Context;
use polars::prelude::*;
use serde::Deserialize;
use tauri::State;
use uuid::Uuid;

use crate::{
    error::AppError,
    flags::{normalize_flag_value, severity_rank},
    ioc::load_ioc_entries,
    project_io::read_project_dataframe,
    search::{build_search_mask_boolean, to_rpn, tokenize_search_query, SearchToken},
    state::AppState,
    storage::load_flags,
};

use super::utils::build_row_search_text;

#[derive(Debug, Deserialize)]
pub struct ExportProjectPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
}

/// Exports the project data with derived trivium columns to a CSV file.
#[tauri::command]
pub fn export_project(state: State<AppState>, payload: ExportProjectPayload) -> Result<(), String> {
    let meta = state
        .projects
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = state.projects.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    let mut df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let mut safe_flags: Vec<i32> = vec![0; df.height()];
    let mut suspicious_flags: Vec<i32> = vec![0; df.height()];
    let mut critical_flags: Vec<i32> = vec![0; df.height()];
    let mut memo_series: Vec<String> = vec![String::new(); df.height()];

    let column_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let column_series: HashMap<&str, &Series> =
        df.get_columns().iter().map(|s| (s.name(), s)).collect();

    for i in 0..df.height() {
        let mut ioc_flag = String::new();
        let mut ioc_rank = 0;
        let mut memo_tags = Vec::new();

        if !iocs.is_empty() {
            let (row_text, single_per_col) =
                build_row_search_text(&column_names, &column_series, i);
            let single_row = vec![row_text];
            for ioc_entry in &iocs {
                let query = ioc_entry.query.trim();
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
                let mask =
                    build_search_mask_boolean(&rpn, &terms, &single_row, Some(&single_per_col));
                let row_matches = mask.get(0).copied().unwrap_or(false);

                if row_matches {
                    let severity = normalize_flag_value(&ioc_entry.flag);
                    let severity_rank_value = severity_rank(&severity);
                    if severity_rank_value > ioc_rank {
                        ioc_rank = severity_rank_value;
                        ioc_flag = severity.clone();
                    }
                    let tag = ioc_entry.tag.trim();
                    if !tag.is_empty() {
                        let token = format!("[{}]", tag);
                        if !memo_tags.contains(&token) {
                            memo_tags.push(token);
                        }
                    }
                }
            }
        }

        let final_flag: String;
        let mut final_memo: String;

        if let Some(user_entry) = flags.get(&i) {
            final_flag = normalize_flag_value(user_entry.flag.trim());
            final_memo = user_entry.memo.clone().unwrap_or_default();
        } else {
            final_flag = ioc_flag;
            final_memo = String::new();
        }

        for tag in memo_tags {
            if !final_memo.contains(&tag) {
                if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                    final_memo.push(' ');
                }
                final_memo.push_str(&tag);
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

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::Context;
use polars::prelude::*;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    error::AppError,
    flags::normalize_flag_value,
    ioc::{cached_ioc_outcomes, load_ioc_entries},
    ops::query::{data_columns, ordered_row_indices, resolve_target},
    project_io::{
        read_project_dataframe, ProjectColumns, INTERNAL_ORIGINAL, INTERNAL_ROW_ID,
    },
    storage::load_flags,
    store::ProjectsStore,
};

/// Export selection. `scope = "all"` (default) exports every row; `"filtered"`
/// exports the rows selected by the flattened `QuerySpec`. Input/output column
/// restriction is `columns`; search scope is `QuerySpec.searchColumns`.
#[derive(Debug, Deserialize)]
pub struct ExportProjectPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(flatten)]
    pub spec: crate::ops::query::QuerySpec,
    /// Output only these base columns (derived trivium columns are kept).
    #[serde(default)]
    pub columns: Option<Vec<String>>,
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
    // IOC evaluation runs on the full column set so queries targeting hidden
    // columns keep matching even when the export restricts output columns.
    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);
    let project_columns = ProjectColumns::from_meta(&meta, &df);
    let all_columns = data_columns(&df);

    // Resolve rows and order before touching output columns. The search scope
    // is `spec.search_columns`, never the output column list.
    let final_indices: Vec<IdxSize> = if payload.scope.as_deref() == Some("filtered") {
        resolve_target(
            &df,
            &project_dir,
            &payload.spec,
            &all_columns,
            &flags,
            &ioc_outcomes,
        )
        .filtered
        .into_iter()
        .map(|row_idx| row_idx as IdxSize)
        .collect()
    } else {
        ordered_row_indices(
            &df,
            payload.spec.sort_key.as_deref(),
            payload.spec.sort_direction.as_deref(),
        )
        .into_iter()
        .map(|row_idx| row_idx as IdxSize)
        .collect()
    };

    let mut safe_flags: Vec<i32> = vec![0; df.height()];
    let mut suspicious_flags: Vec<i32> = vec![0; df.height()];
    let mut critical_flags: Vec<i32> = vec![0; df.height()];
    let mut memo_series: Vec<String> = vec![String::new(); df.height()];

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

    // Apply the output column restriction after the target and annotations are
    // fixed. Only the internal row id is removed here. `flag` and `memo` used
    // to be internal annotation columns, but input CSVs may legitimately have
    // columns with those names, so dropping them unconditionally erased user
    // data. Annotation metadata now lives in `flags.json` and is exported as
    // the `trivium-*` columns below.
    if let Some(columns) = &payload.columns {
        // Output columns are ids, never labels. Filtering happens after the
        // target and annotations are fixed, and internal columns are excluded.
        let selected: Vec<String> = columns
            .iter()
            .filter(|name| all_columns.contains(name))
            .cloned()
            .collect();
        df = df
            .select(&selected)
            .map_err(|e| AppError::Message(format!("Failed to select columns: {}", e)))?;
    }
    for internal in [INTERNAL_ROW_ID, INTERNAL_ORIGINAL] {
        if let Ok(next) = df.drop(internal) {
            df = next;
        }
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

    let index_column = IdxCa::from_vec("", final_indices);
    df = df
        .take(&index_column)
        .map_err(|e| AppError::Message(format!("Failed to select export rows: {}", e)))?;

    let destination = PathBuf::from(payload.destination);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create export dir {:?}", parent))
            .map_err(AppError::from)?;
    }

    // Export headers are display labels, not internal ids. Duplicate labels
    // are written as-is until T23 adds the id correspondence metadata.
    let headers: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|name| project_columns.label(name))
        .collect();

    let file = File::create(&destination)
        .with_context(|| format!("failed to create export file {:?}", destination))
        .map_err(AppError::from)?;
    let mut writer = BufWriter::new(file);

    // Polars rejects duplicate DataFrame column names, so write the header
    // separately to keep duplicate display labels intact.
    let mut header_bytes = Vec::new();
    {
        let mut header_writer = csv::Writer::from_writer(&mut header_bytes);
        header_writer
            .write_record(&headers)
            .context("failed to write export CSV header")?;
        header_writer
            .flush()
            .context("failed to flush export CSV header")?;
    }
    writer
        .write_all(&header_bytes)
        .context("failed to write export CSV header")?;

    let mut writer = CsvWriter::new(writer).include_header(false);
    let mut df_out = df.clone();
    writer
        .finish(&mut df_out)
        .context("failed to write export CSV")
        .map_err(AppError::from)?;
    Ok(())
}

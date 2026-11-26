mod annotations;
mod metadata;
mod settings;
mod sqlite;
mod undo;

pub use annotations::{
    apply_flag_batch, count_flagged, load_flag, load_flag_snapshot, load_flags,
    load_flags_for_rows, load_page_flag_snapshot, patch_flag_atomic, remove_flag, save_flags,
    upsert_flag,
};
pub use metadata::{
    bump_project_revision, insert_project_meta, project_revision, project_was_registered,
    read_project_meta, remove_project_meta, update_project_meta,
};
pub use settings::{
    load_setting, load_setting_bytes, remove_setting, save_setting, save_setting_bytes,
    save_setting_bytes_checked,
};
pub use sqlite::{project_database_path, PROJECT_DATABASE};
pub use undo::{apply_bulk_patch, undo_bulk_patch};

use std::{collections::HashMap, fs, path::Path};

use anyhow::{Context, Result};
use polars::prelude::DataFrame;

use crate::{
    models::ColorRule,
    project_io::is_internal_column,
    value_utils::{anyvalue_to_json, value_display_length},
};

/// Unknown flag text remains stored, but behaves as Unflagged in every
/// query and counter. Memo/tag/bookmark-only entries are independent.
pub(crate) fn is_recognized_flag(entry: &crate::models::FlagEntry) -> bool {
    !crate::flags::normalize_flag_value(&entry.flag).is_empty()
}

pub fn load_color_rules(project_dir: &Path) -> Result<Option<Vec<ColorRule>>> {
    load_setting_bytes(project_dir, "colors")?
        .map(|bytes| serde_json::from_slice(&bytes).context("invalid color rules"))
        .transpose()
}

pub fn save_color_rules(project_dir: &Path, rules: &[ColorRule]) -> Result<()> {
    save_setting_bytes(project_dir, "colors", &serde_json::to_vec(rules)?)
}

/// External files (for example user-selected IOC exports) still use atomic IO.
/// Project settings use their SQLite transaction instead.
pub fn write_file_atomic(path: &Path, data: &[u8]) -> Result<()> {
    write_file_atomic_with(path, data, |temporary, bytes| fs::write(temporary, bytes))
}

fn write_file_atomic_with(
    path: &Path,
    data: &[u8],
    write: impl FnOnce(&Path, &[u8]) -> std::io::Result<()>,
) -> Result<()> {
    let temp = path.with_extension(format!(
        "tmp.{}.{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let result = write(&temp, data)
        .with_context(|| format!("failed to write temporary file {temp:?}"))
        .and_then(|_| {
            fs::rename(&temp, path).with_context(|| format!("failed to replace {path:?}"))
        });
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

pub fn load_column_metrics(project_dir: &Path) -> Result<Option<HashMap<String, usize>>> {
    load_setting_bytes(project_dir, "column_max_chars")?
        .map(|bytes| serde_json::from_slice(&bytes).context("invalid column metrics"))
        .transpose()
}

pub fn save_column_metrics(project_dir: &Path, metrics: &HashMap<String, usize>) -> Result<()> {
    save_setting_bytes(
        project_dir,
        "column_max_chars",
        &serde_json::to_vec(metrics)?,
    )
}

pub const COLUMN_WIDTH_SAMPLE_ROWS: usize = 1_000;

pub fn compute_column_max_chars(df: &DataFrame) -> HashMap<String, usize> {
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| !is_internal_column(name))
        .map(str::to_string)
        .collect();
    let mut widths: HashMap<String, usize> = columns
        .iter()
        .map(|name| (name.clone(), name.chars().count()))
        .collect();
    for column in &columns {
        if let Ok(series) = df.column(column) {
            for value in series.iter().take(COLUMN_WIDTH_SAMPLE_ROWS) {
                let length = value_display_length(&anyvalue_to_json(&value));
                if let Some(current) = widths.get_mut(column) {
                    *current = (*current).max(length);
                }
            }
        }
    }
    widths
}

#[cfg(test)]
mod tests;

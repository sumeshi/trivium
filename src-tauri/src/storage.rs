use std::{
    collections::HashMap,
    fs,
    path::Path,
};

use anyhow::{Context, Result};
use polars::prelude::DataFrame;

use crate::{
    models::FlagEntry,
    value_utils::{anyvalue_to_json, value_display_length},
};

pub fn load_flags(path: &Path) -> Result<HashMap<usize, FlagEntry>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let data = fs::read(path)
        .with_context(|| format!("failed to read flags file {:?}", path))?;
    let map: HashMap<usize, FlagEntry> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse flags file {:?}", path))?;
    Ok(map)
}

pub fn save_flags(path: &Path, flags: &HashMap<usize, FlagEntry>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create flag dir {:?}", parent))?;
    }
    let data = serde_json::to_vec_pretty(flags)?;
    fs::write(path, data)
        .with_context(|| format!("failed to write flags file {:?}", path))
}

pub fn load_column_metrics(path: &Path) -> Result<Option<HashMap<String, usize>>> {
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read(path)
        .with_context(|| format!("failed to read column metrics file {:?}", path))?;
    let map: HashMap<String, usize> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse column metrics file {:?}", path))?;
    Ok(Some(map))
}

pub fn save_column_metrics(path: &Path, metrics: &HashMap<String, usize>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create column metrics dir {:?}", parent))?;
    }
    let data = serde_json::to_vec_pretty(metrics)
        .with_context(|| format!("failed to serialize column metrics for {:?}", path))?;
    fs::write(path, data)
        .with_context(|| format!("failed to write column metrics file {:?}", path))
}

pub fn compute_column_max_chars(df: &DataFrame) -> HashMap<String, usize> {
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| name != &"__rowid")
        .map(|name| name.to_string())
        .collect();
    let mut column_max_chars: HashMap<String, usize> = columns
        .iter()
        .map(|name| {
            let header_len = name.chars().count();
            (name.clone(), header_len)
        })
        .collect();
    for column in &columns {
        if let Ok(series) = df.column(column) {
            for value in series.iter() {
                let json_value = anyvalue_to_json(&value);
                let display_len = value_display_length(&json_value);
                if let Some(entry) = column_max_chars.get_mut(column) {
                    if display_len > *entry {
                        *entry = display_len;
                    }
                }
            }
        }
    }
    column_max_chars
}

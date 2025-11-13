use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use polars::prelude::DataFrame;
use sled::Db;

use crate::{
    models::FlagEntry,
    value_utils::{anyvalue_to_json, value_display_length},
};

const SEARCHABLE_CACHE_KEY: &[u8] = b"searchable_cache";
const IOC_FLAG_CACHE_KEY: &[u8] = b"ioc_flag_cache";

fn encode_row_key(row_index: usize) -> [u8; 8] {
    (row_index as u64).to_be_bytes()
}

fn decode_row_key(bytes: &[u8]) -> Option<usize> {
    if bytes.len() != 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    Some(u64::from_be_bytes(buf) as usize)
}

fn flags_db_path(path: &Path) -> PathBuf {
    path.with_extension("db")
}

fn cache_db_path(project_dir: &Path) -> PathBuf {
    project_dir.join("cache.db")
}

fn open_flags_db(path: &Path) -> Result<Db> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create flags db dir {:?}", parent))?;
    }
    sled::open(path).with_context(|| format!("failed to open flags db at {:?}", path))
}

fn open_cache_db(path: &Path) -> Result<Db> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create cache db dir {:?}", parent))?;
    }
    sled::open(path).with_context(|| format!("failed to open cache db at {:?}", path))
}

fn read_flags_from_json(path: &Path) -> Result<HashMap<usize, FlagEntry>> {
    let data = fs::read(path).with_context(|| format!("failed to read flags file {:?}", path))?;
    let map: HashMap<usize, FlagEntry> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse flags file {:?}", path))?;
    Ok(map)
}

fn write_flags_to_db(path: &Path, flags: &HashMap<usize, FlagEntry>) -> Result<()> {
    let db = open_flags_db(path)?;
    let mut to_delete: Vec<Vec<u8>> = Vec::new();
    for result in db.iter() {
        let (key, _) = result.with_context(|| "failed to iterate existing flag entries")?;
        to_delete.push(key.as_ref().to_vec());
    }
    for key in to_delete {
        db.remove(key)
            .with_context(|| "failed to clear existing flag entry")?;
    }
    for (row_index, entry) in flags {
        let key = encode_row_key(*row_index);
        let value = serde_json::to_vec(entry)
            .with_context(|| format!("failed to serialize flag entry for row {}", row_index))?;
        db.insert(key, value)
            .with_context(|| format!("failed to persist flag entry for row {}", row_index))?;
    }
    db.flush()
        .with_context(|| format!("failed to flush flags db {:?}", path))?;
    Ok(())
}

fn read_flags_from_db(path: &Path) -> Result<HashMap<usize, FlagEntry>> {
    let db = open_flags_db(path)?;
    let mut map = HashMap::new();
    for result in db.iter() {
        let (key, value) = result.with_context(|| "failed to iterate flag entries")?;
        let Some(idx) = decode_row_key(key.as_ref()) else {
            continue;
        };
        let entry: FlagEntry = serde_json::from_slice(&value)
            .with_context(|| format!("failed to deserialize flag entry for row {}", idx))?;
        map.insert(idx, entry);
    }
    Ok(map)
}

fn remove_legacy_flags_file(path: &Path) {
    if path.exists() {
        let _ = fs::remove_file(path);
    }
}

pub fn load_flags(path: &Path) -> Result<HashMap<usize, FlagEntry>> {
    let db_path = flags_db_path(path);
    if db_path.exists() {
        return read_flags_from_db(&db_path);
    }
    if path.exists() {
        let flags = read_flags_from_json(path)?;
        write_flags_to_db(&db_path, &flags)?;
        remove_legacy_flags_file(path);
        return Ok(flags);
    }
    Ok(HashMap::new())
}

pub fn save_flags(path: &Path, flags: &HashMap<usize, FlagEntry>) -> Result<()> {
    let db_path = flags_db_path(path);
    write_flags_to_db(&db_path, flags)?;
    remove_legacy_flags_file(path);
    Ok(())
}

pub fn load_column_metrics(path: &Path) -> Result<Option<HashMap<String, usize>>> {
    if !path.exists() {
        return Ok(None);
    }
    let data =
        fs::read(path).with_context(|| format!("failed to read column metrics file {:?}", path))?;
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
    fs::write(path, data).with_context(|| format!("failed to write column metrics file {:?}", path))
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

pub fn upsert_flag(path: &Path, row_index: usize, entry: &FlagEntry) -> Result<()> {
    let db = open_flags_db(&flags_db_path(path))?;
    let key = encode_row_key(row_index);
    let value = serde_json::to_vec(entry)
        .with_context(|| format!("failed to serialize flag entry for row {}", row_index))?;
    db.insert(key, value)
        .with_context(|| format!("failed to persist flag entry for row {}", row_index))?;
    db.flush()
        .with_context(|| format!("failed to flush flag entry for row {}", row_index))?;
    Ok(())
}

pub fn remove_flag(path: &Path, row_index: usize) -> Result<()> {
    let db = open_flags_db(&flags_db_path(path))?;
    let key = encode_row_key(row_index);
    db.remove(key)
        .with_context(|| format!("failed to delete flag entry for row {}", row_index))?;
    db.flush()
        .with_context(|| format!("failed to flush flags db while deleting row {}", row_index))?;
    Ok(())
}

pub fn count_flagged(path: &Path) -> Result<usize> {
    let db = open_flags_db(&flags_db_path(path))?;
    let mut count = 0;
    for result in db.iter() {
        let (_, value) = result.with_context(|| "failed to iterate flag entries for counting")?;
        let entry: FlagEntry = serde_json::from_slice(&value)
            .context("failed to parse flag entry while counting flagged rows")?;
        if !entry.flag.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

pub fn load_searchable_cache(project_dir: &Path) -> Result<Option<Vec<String>>> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    match db.get(SEARCHABLE_CACHE_KEY) {
        Ok(Some(value)) => {
            let cache: Vec<String> =
                serde_json::from_slice(&value).context("failed to deserialize searchable cache")?;
            Ok(Some(cache))
        }
        Ok(None) => Ok(None),
        Err(err) => Err(err).with_context(|| "failed to read searchable cache"),
    }
}

pub fn save_searchable_cache(project_dir: &Path, cache: &[String]) -> Result<()> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    let data = serde_json::to_vec(cache).context("failed to serialize searchable cache")?;
    db.insert(SEARCHABLE_CACHE_KEY, data)
        .with_context(|| "failed to persist searchable cache")?;
    db.flush()
        .with_context(|| "failed to flush searchable cache db")?;
    Ok(())
}

pub fn clear_searchable_cache(project_dir: &Path) -> Result<()> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    db.remove(SEARCHABLE_CACHE_KEY)
        .with_context(|| "failed to clear searchable cache")?;
    db.flush()
        .with_context(|| "failed to flush searchable cache db")?;
    Ok(())
}

pub fn load_ioc_flag_cache(project_dir: &Path) -> Result<Option<Vec<String>>> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    match db.get(IOC_FLAG_CACHE_KEY) {
        Ok(Some(value)) => {
            let cache: Vec<String> =
                serde_json::from_slice(&value).context("failed to deserialize IOC flag cache")?;
            Ok(Some(cache))
        }
        Ok(None) => Ok(None),
        Err(err) => Err(err).with_context(|| "failed to read IOC flag cache"),
    }
}

pub fn save_ioc_flag_cache(project_dir: &Path, cache: &[String]) -> Result<()> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    let data = serde_json::to_vec(cache).context("failed to serialize IOC flag cache")?;
    db.insert(IOC_FLAG_CACHE_KEY, data)
        .with_context(|| "failed to persist IOC flag cache")?;
    db.flush()
        .with_context(|| "failed to flush IOC flag cache db")?;
    Ok(())
}

pub fn clear_ioc_flag_cache(project_dir: &Path) -> Result<()> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    db.remove(IOC_FLAG_CACHE_KEY)
        .with_context(|| "failed to clear IOC flag cache")?;
    db.flush()
        .with_context(|| "failed to flush IOC flag cache db")?;
    Ok(())
}

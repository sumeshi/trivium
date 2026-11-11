use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use sled::Db;

use crate::{
    models::{ColorRule, FlagEntry},
    project_io::is_internal_column,
    value_utils::{anyvalue_to_json, value_display_length},
};

const SEARCHABLE_CACHE_KEY: &[u8] = b"searchable_cache";
const IOC_FLAG_CACHE_KEY: &[u8] = b"ioc_flag_cache";

/// One open sled handle per annotation database.
///
/// sled keeps an exclusive lock on its directory, so every read and write in
/// this process must share the same handle. `close_flags_db` removes a project's
/// handle when the project is deleted.
static FLAGS_DBS: OnceLock<Mutex<HashMap<PathBuf, Db>>> = OnceLock::new();

fn flags_dbs() -> &'static Mutex<HashMap<PathBuf, Db>> {
    FLAGS_DBS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Cached searchable text, keyed by the column set it was built from and the
/// row count.
///
/// Invalidation rules (B4):
/// - the key is the set of search-target columns plus the row count; a change
///   to either rebuilds the cache
/// - `set_hidden_columns` clears it
/// - IOC changes are irrelevant: searchable text does not depend on IOCs
/// - deleting a project removes `project_dir` (including `cache.db`), so no
///   per-cache clear is needed
///
/// A cache written by an older version (a bare `Vec<String>`) fails to
/// deserialize and is treated as absent, forcing a rebuild.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchableCacheEntry {
    pub columns: Vec<String>,
    pub text: Vec<String>,
}

/// Cached IOC evaluation, keyed by the IOC set signature and the row count.
///
/// Invalidation rules:
/// - the key is the signature of the normalized IOC set plus the row count;
///   any IOC change rebuilds the cache (appending entries at the end merges
///   incrementally instead of re-evaluating every row)
/// - user flag updates never invalidate it: the cache stores raw evaluation
///   results, independent of user flags
/// - the column layout is irrelevant: every data column participates in IOC
///   matching
/// - deleting a project removes `project_dir` (including `cache.db`), so no
///   per-cache clear is needed
///
/// `entries` holds the definition-order normalized entries so an appended
/// IOC set can be recognized. A cache written by an older version (rows with
/// `tags: Vec<String>` instead of `tags: Vec<InternalTag>`) fails to
/// deserialize; `load_ioc_flag_cache` returns `Err`, and
/// `cached_ioc_outcomes` logs it and falls back to a full re-evaluation,
/// which rewrites the cache in the current format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IocCacheEntry {
    /// Sorted normalized entries as JSON, for exact-match validation.
    pub signature: String,
    /// Definition-order normalized entries (flag, tag, query), for detecting
    /// append-only IOC changes.
    pub entries: Vec<(String, String, String)>,
    pub(crate) rows: Vec<crate::ioc::evaluate::InternalOutcome>,
}

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
    open_sled(path).with_context(|| format!("failed to open flags db at {:?}", path))
}

/// Returns the process-wide handle for an annotation database path.
fn flags_db(db_path: &Path) -> Result<Db> {
    let mut dbs = flags_dbs().lock();
    if let Some(db) = dbs.get(db_path) {
        return Ok(db.clone());
    }
    let db = open_flags_db(db_path)?;
    dbs.insert(db_path.to_path_buf(), db.clone());
    Ok(db)
}

fn close_flags_db_path(db_path: &Path) {
    if let Some(dbs) = FLAGS_DBS.get() {
        dbs.lock().remove(db_path);
    }
}

/// Closes the cached annotation handle for a project directory.
pub fn close_flags_db(project_dir: &Path) {
    close_flags_db_path(&flags_db_path(&project_dir.join("flags.json")));
}

fn open_cache_db(path: &Path) -> Result<Db> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create cache db dir {:?}", parent))?;
    }
    open_sled(path).with_context(|| format!("failed to open cache db at {:?}", path))
}

/// sled takes an exclusive flock on the database file. A previous instance's
/// background threads can still hold the lock for a short while after the
/// handle is dropped, so an immediate re-open fails with `WouldBlock`. Retry
/// briefly instead of surfacing the error (observed as a flaky failure on
/// slower CI runners when a test closes and re-opens the same database).
const SLED_OPEN_ATTEMPTS: usize = 50;
const SLED_OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(20);

fn open_sled(path: &Path) -> Result<sled::Db, sled::Error> {
    for attempt in 1..=SLED_OPEN_ATTEMPTS {
        match sled::open(path) {
            Ok(db) => return Ok(db),
            Err(sled::Error::Io(err))
                if err.kind() == std::io::ErrorKind::WouldBlock
                    && attempt < SLED_OPEN_ATTEMPTS =>
            {
                std::thread::sleep(SLED_OPEN_RETRY_INTERVAL);
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("retry loop always returns within the attempt limit")
}

fn read_flags_from_json(path: &Path) -> Result<HashMap<usize, FlagEntry>> {
    let data = fs::read(path).with_context(|| format!("failed to read flags file {:?}", path))?;
    let map: HashMap<usize, FlagEntry> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse flags file {:?}", path))?;
    Ok(map)
}

fn write_flags_to_db(path: &Path, flags: &HashMap<usize, FlagEntry>) -> Result<()> {
    // This path is used for imports (before the project directory is moved
    // into place) and legacy migration. Drop any cached handle first so the
    // database is not moved or rewritten while a process-wide handle is open.
    close_flags_db_path(path);
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
    let db = flags_db(path)?;
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

/// Loads per-project color rules. `None` means the project has never saved
/// rules; an empty file/array is a deliberate "no rules" state.
pub fn load_color_rules(project_dir: &Path) -> Result<Option<Vec<ColorRule>>> {
    let path = project_dir.join("colors.json");
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read(&path).with_context(|| format!("failed to read color rules {:?}", path))?;
    let rules: Vec<ColorRule> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse color rules {:?}", path))?;
    Ok(Some(rules))
}

/// Persists color rules as pretty JSON. Normalization belongs to the ops layer;
/// this function is plain IO.
pub fn save_color_rules(project_dir: &Path, rules: &[ColorRule]) -> Result<()> {
    let path = project_dir.join("colors.json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to prepare color rules dir {:?}", parent))?;
    }
    let data = serde_json::to_vec_pretty(rules).context("failed to serialize color rules")?;
    fs::write(&path, data).with_context(|| format!("failed to write color rules {:?}", path))
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

/// Number of leading rows used for the initial column-width estimate.
///
/// The estimate only feeds the first table layout. Scanning every cell made
/// this cost proportional to the whole project, so the estimate deliberately
/// stops after this sample. A wider value later in the data does not enlarge a
/// column until it is recomputed with a different rule.
pub const COLUMN_WIDTH_SAMPLE_ROWS: usize = 1_000;

pub fn compute_column_max_chars(df: &DataFrame) -> HashMap<String, usize> {
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| !is_internal_column(name))
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
            for value in series.iter().take(COLUMN_WIDTH_SAMPLE_ROWS) {
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

/// Reads one row's annotation directly, without loading the whole database.
pub fn load_flag(path: &Path, row_index: usize) -> Result<Option<FlagEntry>> {
    let db = flags_db(&flags_db_path(path))?;
    let key = encode_row_key(row_index);
    let value = db
        .get(key)
        .with_context(|| format!("failed to read flag entry for row {}", row_index))?;
    match value {
        Some(value) => {
            let entry: FlagEntry = serde_json::from_slice(&value).with_context(|| {
                format!("failed to deserialize flag entry for row {}", row_index)
            })?;
            Ok(Some(entry))
        }
        None => Ok(None),
    }
}

pub fn upsert_flag(path: &Path, row_index: usize, entry: &FlagEntry) -> Result<()> {
    let db = flags_db(&flags_db_path(path))?;
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
    let db = flags_db(&flags_db_path(path))?;
    let key = encode_row_key(row_index);
    db.remove(key)
        .with_context(|| format!("failed to delete flag entry for row {}", row_index))?;
    db.flush()
        .with_context(|| format!("failed to flush flags db while deleting row {}", row_index))?;
    Ok(())
}

/// Applies several annotation changes as one sled batch.
///
/// `None` removes the row's entry and `Some` upserts it. The batch is committed
/// with a single `apply_batch` and a single flush, so a failure cannot leave
/// only the rows processed before the failure modified.
pub fn apply_flag_batch(path: &Path, changes: &[(usize, Option<FlagEntry>)]) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let db = flags_db(&flags_db_path(path))?;
    let mut batch = sled::Batch::default();
    for (row_index, entry) in changes {
        let key = encode_row_key(*row_index).to_vec();
        match entry {
            Some(entry) => {
                let value = serde_json::to_vec(entry).with_context(|| {
                    format!("failed to serialize flag entry for row {}", row_index)
                })?;
                batch.insert(key, value);
            }
            None => batch.remove(key),
        }
    }
    db.apply_batch(batch)
        .with_context(|| format!("failed to apply flag batch to {:?}", path))?;
    db.flush()
        .with_context(|| format!("failed to flush flags db {:?}", path))?;
    Ok(())
}

pub fn count_flagged(path: &Path) -> Result<usize> {
    let db = flags_db(&flags_db_path(path))?;
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

pub fn load_searchable_cache(project_dir: &Path) -> Result<Option<SearchableCacheEntry>> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    match db.get(SEARCHABLE_CACHE_KEY) {
        Ok(Some(value)) => {
            let cache: SearchableCacheEntry = serde_json::from_slice(&value)
                .context("failed to deserialize searchable cache")?;
            Ok(Some(cache))
        }
        Ok(None) => Ok(None),
        Err(err) => Err(err).with_context(|| "failed to read searchable cache"),
    }
}

pub fn save_searchable_cache(
    project_dir: &Path,
    columns: &[String],
    text: &[String],
) -> Result<()> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    let entry = SearchableCacheEntry {
        columns: columns.to_vec(),
        text: text.to_vec(),
    };
    let data = serde_json::to_vec(&entry).context("failed to serialize searchable cache")?;
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

pub fn load_ioc_flag_cache(project_dir: &Path) -> Result<Option<IocCacheEntry>> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    match db.get(IOC_FLAG_CACHE_KEY) {
        Ok(Some(value)) => {
            let cache: IocCacheEntry =
                serde_json::from_slice(&value).context("failed to deserialize IOC flag cache")?;
            Ok(Some(cache))
        }
        Ok(None) => Ok(None),
        Err(err) => Err(err).with_context(|| "failed to read IOC flag cache"),
    }
}

pub(crate) fn save_ioc_flag_cache(
    project_dir: &Path,
    signature: &str,
    entries: &[(String, String, String)],
    rows: &[crate::ioc::evaluate::InternalOutcome],
) -> Result<()> {
    let db = open_cache_db(&cache_db_path(project_dir))?;
    let entry = IocCacheEntry {
        signature: signature.to_string(),
        entries: entries.to_vec(),
        rows: rows.to_vec(),
    };
    let data = serde_json::to_vec(&entry).context("failed to serialize IOC flag cache")?;
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

//! Byte-bounded transient analyses. Active reads pin their entries until done.
use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::SystemTime,
};

use parking_lot::Mutex;
use uuid::Uuid;

use crate::{
    error::AppError, formats::CompiledFormat, ops::query::QuerySpec, store::ProjectsStore,
};

use super::{ExtractResult, ExtractValue};

pub(super) const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MAX_RESULTS: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Revision {
    bytes: u64,
    modified: SystemTime,
}
impl Revision {
    pub fn read(path: &std::path::Path) -> Result<Self, AppError> {
        let metadata = fs::metadata(path).map_err(|error| {
            AppError::Message(format!("Could not read extraction source: {error}"))
        })?;
        Ok(Self {
            bytes: metadata.len(),
            modified: metadata
                .modified()
                .map_err(|error| AppError::Message(error.to_string()))?,
        })
    }
}

pub(super) struct Snapshot {
    pub summary: ExtractResult,
    pub values: Vec<ExtractValue>,
    pub matcher: CompiledFormat,
    pub path: PathBuf,
    pub revision: Revision,
    pub total_rows: usize,
    /// All-row analyses need no retained row-ID vector.
    pub ids: Option<Arc<Vec<usize>>>,
    /// Source membership is frozen above; never re-resolve after annotations change.
    pub _query: QuerySpec,
    pub bytes: usize,
}

impl Snapshot {
    pub fn validate(&self, store: &ProjectsStore) -> Result<(), AppError> {
        if store
            .project_dir(&self.summary.project_id)
            .join("data.parquet")
            != self.path
        {
            return Err(unavailable());
        }
        if Revision::read(&self.path)? != self.revision {
            return Err(AppError::Message(
                "The source data changed. Run extraction again.".into(),
            ));
        }
        Ok(())
    }
}

struct Entry {
    snapshot: Arc<Snapshot>,
    released: bool,
}
#[derive(Default)]
struct Cache {
    entries: VecDeque<Entry>,
    bytes: usize,
}

fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Cache::default()))
}

fn unavailable() -> AppError {
    AppError::Message("This extraction result was closed or evicted. Run extraction again.".into())
}

impl Cache {
    fn prune_released(&mut self) {
        for index in (0..self.entries.len()).rev() {
            if self.entries[index].released && Arc::strong_count(&self.entries[index].snapshot) == 1
            {
                self.remove(index);
            }
        }
    }
    fn remove(&mut self, index: usize) {
        if let Some(entry) = self.entries.remove(index) {
            self.bytes -= entry.snapshot.bytes;
        }
    }
}

pub(super) fn insert(snapshot: Snapshot) -> Result<ExtractResult, AppError> {
    if snapshot.bytes > MAX_CACHE_BYTES {
        return Err(AppError::Message("Extraction result exceeds the transient cache budget. Narrow the rows or columns and run again.".into()));
    }
    let mut cache = cache().lock();
    cache.prune_released();
    let mut bytes = cache.bytes;
    let mut count = cache.entries.len();
    let mut evict = Vec::new();
    for (index, entry) in cache.entries.iter().enumerate() {
        if count < MAX_RESULTS && bytes.saturating_add(snapshot.bytes) <= MAX_CACHE_BYTES {
            break;
        }
        if Arc::strong_count(&entry.snapshot) == 1 {
            bytes -= entry.snapshot.bytes;
            count -= 1;
            evict.push(index);
        }
    }
    if count >= MAX_RESULTS || bytes.saturating_add(snapshot.bytes) > MAX_CACHE_BYTES {
        return Err(AppError::Message(
            "Extraction results are in use. Close a result or wait for its export to finish."
                .into(),
        ));
    }
    // Failed admission preserves older analyses, even when an active export
    // pins the entry preventing admission of the new result altogether.
    for index in evict.into_iter().rev() {
        cache.remove(index);
    }
    let summary = snapshot.summary.clone();
    cache.bytes += snapshot.bytes;
    cache.entries.push_back(Entry {
        snapshot: Arc::new(snapshot),
        released: false,
    });
    Ok(summary)
}

pub(super) fn get(store: &ProjectsStore, id: Uuid) -> Result<Arc<Snapshot>, AppError> {
    let snapshot = {
        let mut cache = cache().lock();
        cache.prune_released();
        let index = cache
            .entries
            .iter()
            .position(|entry| !entry.released && entry.snapshot.summary.result_id == id)
            .ok_or_else(unavailable)?;
        let entry = cache.entries.remove(index).expect("located cache entry");
        let snapshot = Arc::clone(&entry.snapshot);
        cache.entries.push_back(entry);
        snapshot
    };
    snapshot.validate(store)?;
    Ok(snapshot)
}

pub fn release_extract_result(id: Uuid) -> bool {
    let mut cache = cache().lock();
    let Some(index) = cache
        .entries
        .iter()
        .position(|entry| entry.snapshot.summary.result_id == id && !entry.released)
    else {
        return false;
    };
    cache.entries[index].released = true;
    cache.prune_released();
    true
}

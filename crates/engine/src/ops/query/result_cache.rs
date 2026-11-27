//! Process-local cache of resolved row ids for repeated page requests (T19).
//!
//! Only query keys, original row ids and annotation counts are kept; page data,
//! output columns, and cell values never enter this cache. The key omits
//! offset/limit so moving between pages of the same result is a hit.

use std::{fs, mem::size_of, path::Path, sync::OnceLock, time::UNIX_EPOCH};

use parking_lot::Mutex;
use uuid::Uuid;

use super::{page::is_unrestricted_flag_filter, target::QuerySpec};

pub(crate) const MAX_RESULTS: usize = 8;
pub(crate) const MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;
// Entry metadata plus the Arc counters and Vec header. Owned buffers are
// counted by capacity; allocator bookkeeping is outside this estimate.
const ENTRY_OVERHEAD_BYTES: usize =
    size_of::<(QueryCacheKey, CachedResult)>() + size_of::<Vec<usize>>() + 2 * size_of::<usize>();

#[derive(Debug, Clone)]
pub(crate) struct CachedResult {
    pub ids: std::sync::Arc<Vec<usize>>,
    pub annotated_filtered: usize,
    pub annotated_total: usize,
    /// Annotation revision when the counts were computed. A data-only result
    /// keeps its ids across annotation edits, but its result-internal count
    /// must be refreshed when this no longer matches the project revision.
    pub annotation_revision: u64,
    /// Row-id allocation and fixed entry overhead; key buffers are added on insert.
    pub estimated_bytes: usize,
}

impl CachedResult {
    pub(crate) fn new(
        ids: Vec<usize>,
        annotated_filtered: usize,
        annotated_total: usize,
        annotation_revision: u64,
    ) -> Self {
        let estimated_bytes = ids.capacity() * size_of::<usize>() + ENTRY_OVERHEAD_BYTES;
        Self {
            ids: std::sync::Arc::new(ids),
            annotated_filtered,
            annotated_total,
            annotation_revision,
            estimated_bytes,
        }
    }
}

fn entry_bytes(key: &QueryCacheKey, result: &CachedResult) -> usize {
    result
        .estimated_bytes
        .saturating_add(key.spec.capacity())
        .saturating_add(key.annotation_token.as_ref().map_or(0, String::capacity))
}

/// Statistics for tests and the T27 measurement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultCacheStats {
    pub entries: usize,
    /// Retained buffer capacities and entry metadata, excluding allocator overhead.
    pub total_bytes: usize,
    pub max_entries: usize,
    pub max_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DataRevision {
    len: u64,
    modified_nanos: u128,
}

/// Cache key: project, normalized spec, data revision, and — only when the
/// spec reads annotations — the current annotation token.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct QueryCacheKey {
    project_id: Uuid,
    spec: String,
    data_revision: DataRevision,
    annotation_token: Option<String>,
}

pub(crate) struct ResultCache {
    /// Least recently used first, most recently used last.
    entries: Vec<(QueryCacheKey, CachedResult)>,
    total_bytes: usize,
    max_entries: usize,
    max_bytes: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl ResultCache {
    pub(crate) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: Vec::new(),
            total_bytes: 0,
            max_entries,
            max_bytes,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    pub(crate) fn get(&mut self, key: &QueryCacheKey) -> Option<CachedResult> {
        self.lookup(key, true)
    }

    fn lookup(&mut self, key: &QueryCacheKey, count_miss: bool) -> Option<CachedResult> {
        let Some(position) = self.entries.iter().position(|(known, _)| known == key) else {
            if count_miss {
                self.misses += 1;
            }
            return None;
        };
        let entry = self.entries.remove(position);
        let result = entry.1.clone();
        self.entries.push(entry);
        self.hits += 1;
        Some(result)
    }

    pub(crate) fn insert(&mut self, key: QueryCacheKey, result: CachedResult) {
        self.remove_key(&key);
        let bytes = entry_bytes(&key, &result);

        // Never hold an entry that by itself exceeds the budget: a later
        // request must not be able to report a hit for it.
        if bytes > self.max_bytes {
            return;
        }

        while !self.entries.is_empty()
            && (self.entries.len() >= self.max_entries
                || self.total_bytes.saturating_add(bytes) > self.max_bytes)
        {
            let (key, evicted) = self.entries.remove(0);
            self.total_bytes = self.total_bytes.saturating_sub(entry_bytes(&key, &evicted));
            self.evictions += 1;
        }

        if self.entries.len() >= self.max_entries
            || self.total_bytes.saturating_add(bytes) > self.max_bytes
        {
            return;
        }

        self.total_bytes += bytes;
        self.entries.push((key, result));
    }

    pub(crate) fn remove_project(&mut self, project_id: &Uuid) {
        let mut removed_bytes = 0;
        self.entries.retain(|(key, result)| {
            let keep = key.project_id != *project_id;
            if !keep {
                removed_bytes += entry_bytes(key, result);
            }
            keep
        });
        self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.total_bytes = 0;
        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
    }

    fn refresh_counts(
        &mut self,
        key: &QueryCacheKey,
        revision: u64,
        total: usize,
        filtered: usize,
    ) {
        if let Some((_, result)) = self.entries.iter_mut().find(|(known, _)| known == key) {
            if revision >= result.annotation_revision {
                result.annotation_revision = revision;
                result.annotated_total = total;
                result.annotated_filtered = filtered;
            }
        }
    }

    fn remove_key(&mut self, key: &QueryCacheKey) {
        if let Some(position) = self.entries.iter().position(|(known, _)| known == key) {
            let (key, old) = self.entries.remove(position);
            self.total_bytes = self.total_bytes.saturating_sub(entry_bytes(&key, &old));
        }
    }

    pub(crate) fn stats(&self) -> ResultCacheStats {
        ResultCacheStats {
            entries: self.entries.len(),
            total_bytes: self.total_bytes,
            max_entries: self.max_entries,
            max_bytes: self.max_bytes,
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
        }
    }
}

fn data_revision(path: &Path) -> Option<DataRevision> {
    let metadata = fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    let modified_nanos = modified.duration_since(UNIX_EPOCH).ok()?.as_nanos();
    Some(DataRevision {
        len: metadata.len(),
        modified_nanos,
    })
}

/// Normalizes spec fields whose different spellings mean the same query.
///
/// An empty `searchColumns` list is the same as omitting it (all data columns),
/// and blank search/`all` flag filters/false bookmark/`asc` are the defaults.
/// The serialized field order is fixed by `QuerySpec`, so equal specs produce
/// equal strings.
fn normalized_spec_json(spec: &QuerySpec) -> String {
    let mut normalized = spec.clone();
    if normalized
        .search
        .as_deref()
        .is_some_and(|search| search.trim().is_empty())
    {
        normalized.search = None;
    }
    if normalized
        .search_columns
        .as_ref()
        .is_some_and(|columns| columns.is_empty())
    {
        normalized.search_columns = None;
    }
    if is_unrestricted_flag_filter(normalized.flag_filter.as_deref()) {
        normalized.flag_filter = None;
    }
    if normalized.bookmarked_only == Some(false) {
        normalized.bookmarked_only = None;
    }
    if normalized.sort_key.as_deref().is_some_and(str::is_empty) {
        normalized.sort_key = None;
    }
    if normalized.sort_direction.as_deref() == Some("asc") {
        normalized.sort_direction = None;
    }
    serde_json::to_string(&normalized).unwrap_or_default()
}

/// True when the resolved ids depend on annotation state.
///
/// Data-only searches retain their ids across annotation edits. Flag,
/// bookmark and reserved tag predicates must use the current revision.
pub(crate) fn spec_depends_on_annotations(spec: &QuerySpec) -> bool {
    !is_unrestricted_flag_filter(spec.flag_filter.as_deref())
        || spec.bookmarked_only == Some(true)
        || spec
            .search
            .as_deref()
            .is_some_and(crate::search::search_reads_tags)
}

pub(crate) fn query_cache_key(
    project_id: Uuid,
    spec: &QuerySpec,
    parquet_path: &Path,
    annotation_token: Option<String>,
) -> Option<QueryCacheKey> {
    Some(QueryCacheKey {
        project_id,
        spec: normalized_spec_json(spec),
        data_revision: data_revision(parquet_path)?,
        annotation_token,
    })
}

fn global_cache() -> &'static Mutex<ResultCache> {
    static CACHE: OnceLock<Mutex<ResultCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ResultCache::new(MAX_RESULTS, MAX_RESULT_BYTES)))
}

pub(crate) fn cache_lookup(key: &QueryCacheKey) -> Option<CachedResult> {
    global_cache().lock().get(key)
}

/// Page/locator preflight counts a ready hit; a miss is counted by the shared
/// resolver exactly once, instead of again at every internal cache probe.
pub(crate) fn cache_lookup_if_present(key: &QueryCacheKey) -> Option<CachedResult> {
    global_cache().lock().lookup(key, false)
}

/// Owner-election race check is internal work, not another cache request.
pub(crate) fn cache_peek(key: &QueryCacheKey) -> Option<CachedResult> {
    global_cache()
        .lock()
        .entries
        .iter()
        .find(|(known, _)| known == key)
        .map(|(_, result)| result.clone())
}

pub(crate) fn cache_store(key: QueryCacheKey, result: CachedResult) {
    global_cache().lock().insert(key, result);
}

pub(crate) fn cache_refresh_counts(
    key: &QueryCacheKey,
    revision: u64,
    total: usize,
    filtered: usize,
) {
    global_cache()
        .lock()
        .refresh_counts(key, revision, total, filtered);
}

pub(crate) fn cache_remove_project(project_id: &Uuid) {
    global_cache().lock().remove_project(project_id);
}

/// Entry count, bytes, and hit/miss/eviction counters.
pub fn result_cache_stats() -> ResultCacheStats {
    global_cache().lock().stats()
}

/// Test hook: drops every cached result and resets the counters.
#[doc(hidden)]
pub fn clear_result_cache() {
    global_cache().lock().clear();
}

#[cfg(test)]
mod tests;

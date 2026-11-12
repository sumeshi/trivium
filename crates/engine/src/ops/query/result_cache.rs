//! Process-local cache of resolved row ids for repeated page requests (T19).
//!
//! Only the original row ids and two annotation counts are kept; page data,
//! output columns, and cell values never enter this cache. The key omits
//! offset/limit so moving between pages of the same result is a hit.

use std::{fs, mem::size_of, path::Path, sync::OnceLock, time::UNIX_EPOCH};

use parking_lot::Mutex;
use uuid::Uuid;

use super::{page::is_unrestricted_flag_filter, target::QuerySpec};

pub(crate) const MAX_RESULTS: usize = 2;
pub(crate) const MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;
const ENTRY_OVERHEAD_BYTES: usize = 64;

#[derive(Debug, Clone)]
pub(crate) struct CachedResult {
    pub ids: std::sync::Arc<Vec<usize>>,
    pub annotated_filtered: usize,
    pub annotated_total: usize,
    /// Annotation revision when the counts were computed. A data-only result
    /// keeps its ids across annotation edits, but its result-internal count
    /// must be refreshed when this no longer matches the project revision.
    pub annotation_revision: u64,
    pub estimated_bytes: usize,
}

impl CachedResult {
    pub(crate) fn new(
        ids: Vec<usize>,
        annotated_filtered: usize,
        annotated_total: usize,
        annotation_revision: u64,
    ) -> Self {
        let estimated_bytes = ids.len() * size_of::<usize>() + ENTRY_OVERHEAD_BYTES;
        Self {
            ids: std::sync::Arc::new(ids),
            annotated_filtered,
            annotated_total,
            annotation_revision,
            estimated_bytes,
        }
    }
}

/// Statistics for tests and the T27 measurement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultCacheStats {
    pub entries: usize,
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
        let bytes = result.estimated_bytes;

        // Never hold an entry that by itself exceeds the budget: a later
        // request must not be able to report a hit for it.
        if bytes > self.max_bytes {
            return;
        }

        while !self.entries.is_empty()
            && (self.entries.len() >= self.max_entries || self.total_bytes + bytes > self.max_bytes)
        {
            let (_, evicted) = self.entries.remove(0);
            self.total_bytes = self.total_bytes.saturating_sub(evicted.estimated_bytes);
            self.evictions += 1;
        }

        if self.entries.len() >= self.max_entries || self.total_bytes + bytes > self.max_bytes {
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
                removed_bytes += result.estimated_bytes;
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
            let (_, old) = self.entries.remove(position);
            self.total_bytes = self.total_bytes.saturating_sub(old.estimated_bytes);
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
/// Memo and tag edits do not change which rows match, so they must not
/// invalidate a result. A flag filter reads final flags (user flags plus IOC
/// flags) and bookmark-only reads bookmarks.
pub(crate) fn spec_depends_on_annotations(spec: &QuerySpec) -> bool {
    !is_unrestricted_flag_filter(spec.flag_filter.as_deref()) || spec.bookmarked_only == Some(true)
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
mod tests {
    use super::*;

    fn key(project_id: Uuid, spec: &str) -> QueryCacheKey {
        QueryCacheKey {
            project_id,
            spec: spec.to_string(),
            data_revision: DataRevision {
                len: 10,
                modified_nanos: 20,
            },
            annotation_token: None,
        }
    }

    fn result(rows: usize) -> CachedResult {
        CachedResult::new((0..rows).collect(), 0, 0, 0)
    }

    #[test]
    fn lru_keeps_two_results_and_reports_first_as_miss() {
        let mut cache = ResultCache::new(2, MAX_RESULT_BYTES);
        let project = Uuid::new_v4();
        cache.insert(key(project, "a"), result(4));
        cache.insert(key(project, "b"), result(4));
        assert_eq!(cache.get(&key(project, "a")).map(|c| c.ids.len()), Some(4));
        cache.insert(key(project, "c"), result(4));

        assert_eq!(cache.stats().entries, 2);
        assert!(cache.get(&key(project, "b")).is_none());
        assert!(cache.get(&key(project, "a")).is_some());
    }

    #[test]
    fn oversized_result_is_not_retained_or_reported_as_hit() {
        let mut cache = ResultCache::new(2, 1024);
        let project = Uuid::new_v4();
        let large = result(1024); // ~8 KiB, larger than the injected budget.
        assert!(large.estimated_bytes > 1024);
        cache.insert(key(project, "large"), large);

        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().total_bytes, 0);
        assert!(cache.get(&key(project, "large")).is_none());
    }

    #[test]
    fn total_budget_evicts_the_oldest_result() {
        let mut cache = ResultCache::new(2, 256);
        let project = Uuid::new_v4();
        // Two 8-row results need 2 * (64 + 64) = 256 bytes; a third must evict.
        cache.insert(key(project, "a"), result(8));
        cache.insert(key(project, "b"), result(8));
        cache.insert(key(project, "c"), result(8));

        let stats = cache.stats();
        assert_eq!(stats.entries, 2);
        assert!(stats.total_bytes <= 256);
        assert_eq!(stats.evictions, 1);
        assert!(cache.get(&key(project, "a")).is_none());
    }

    #[test]
    fn deleting_a_project_keeps_other_projects() {
        let mut cache = ResultCache::new(2, MAX_RESULT_BYTES);
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        cache.insert(key(first, "a"), result(4));
        cache.insert(key(second, "a"), result(4));
        cache.remove_project(&first);

        assert_eq!(cache.stats().entries, 1);
        assert!(cache.get(&key(first, "a")).is_none());
        assert!(cache.get(&key(second, "a")).is_some());
    }

    #[test]
    fn normalization_matches_equivalent_specs() {
        let empty_scope = QuerySpec {
            search_columns: Some(Vec::new()),
            ..Default::default()
        };
        let omitted_scope = QuerySpec::default();
        assert_eq!(
            normalized_spec_json(&empty_scope),
            normalized_spec_json(&omitted_scope)
        );

        let defaults_spelled_out = QuerySpec {
            flag_filter: Some("all".to_string()),
            bookmarked_only: Some(false),
            sort_direction: Some("asc".to_string()),
            ..Default::default()
        };
        assert_eq!(
            normalized_spec_json(&defaults_spelled_out),
            normalized_spec_json(&omitted_scope)
        );
        assert!(!spec_depends_on_annotations(&defaults_spelled_out));
        assert!(spec_depends_on_annotations(&QuerySpec {
            bookmarked_only: Some(true),
            ..Default::default()
        }));
    }

    #[test]
    fn late_annotation_count_refresh_cannot_roll_back_a_newer_snapshot_or_copy_ids() {
        let mut cache = ResultCache::new(2, MAX_RESULT_BYTES);
        let key = key(Uuid::new_v4(), "data-only");
        let entry = result(8);
        let ids = entry.ids.clone();
        let bytes = entry.estimated_bytes;
        cache.insert(key.clone(), entry);
        cache.refresh_counts(&key, 3, 2, 1);
        cache.refresh_counts(&key, 2, 99, 99);
        let refreshed = cache.get(&key).unwrap();
        assert_eq!(
            (
                refreshed.annotation_revision,
                refreshed.annotated_total,
                refreshed.annotated_filtered
            ),
            (3, 2, 1)
        );
        assert!(std::sync::Arc::ptr_eq(&ids, &refreshed.ids));
        assert_eq!(cache.stats().total_bytes, bytes);
    }
}

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
fn lru_promotes_hits_before_evicting() {
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
    // A selective result may retain much more allocation than its length.
    let mut ids = Vec::with_capacity(1024);
    ids.push(0);
    let large = CachedResult::new(ids, 0, 0, 0);
    assert_eq!(large.ids.len(), 1);
    assert!(large.estimated_bytes > 1024);
    cache.insert(key(project, "large"), large);

    assert_eq!(cache.stats().entries, 0);
    assert_eq!(cache.stats().total_bytes, 0);
    assert!(cache.get(&key(project, "large")).is_none());
}

#[test]
fn key_buffer_capacity_is_included_even_when_the_key_text_is_short() {
    let mut cache = ResultCache::new(MAX_RESULTS, 1024);
    let project = Uuid::new_v4();
    for field in ["spec", "annotation"] {
        let mut oversized = key(project, "a");
        let mut text = String::with_capacity(2048);
        text.push('a');
        if field == "spec" {
            oversized.spec = text;
        } else {
            oversized.annotation_token = Some(text);
        }
        let lookup = oversized.clone();
        let entry = result(0);
        assert!(entry_bytes(&oversized, &entry) > cache.max_bytes);
        cache.insert(oversized, entry);
        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().total_bytes, 0);
        assert!(cache.get(&lookup).is_none());
    }
}

#[test]
fn total_budget_evicts_the_oldest_result() {
    let project = Uuid::new_v4();
    let budget = 2 * entry_bytes(&key(project, "a"), &result(8));
    let mut cache = ResultCache::new(MAX_RESULTS, budget);
    cache.insert(key(project, "a"), result(8));
    cache.insert(key(project, "b"), result(8));
    assert!(cache.get(&key(project, "a")).is_some());
    cache.insert(key(project, "c"), result(8));

    let stats = cache.stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.total_bytes, budget);
    assert_eq!(stats.evictions, 1);
    assert!(cache.get(&key(project, "b")).is_none());
    assert!(cache.get(&key(project, "a")).is_some());
}

#[test]
fn deleting_a_project_keeps_other_projects() {
    let mut cache = ResultCache::new(MAX_RESULTS, MAX_RESULT_BYTES);
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    cache.insert(key(first, "a"), result(4));
    cache.insert(key(first, "b"), result(4));
    cache.insert(key(second, "a"), result(4));
    assert_eq!(cache.stats().entries, 3);
    cache.remove_project(&first);

    assert_eq!(cache.stats().entries, 1);
    assert_eq!(
        cache.stats().total_bytes,
        entry_bytes(&key(second, "a"), &result(4))
    );
    assert!(cache.get(&key(first, "a")).is_none());
    assert!(cache.get(&key(first, "b")).is_none());
    assert!(cache.get(&key(second, "a")).is_some());
}

#[test]
fn replacing_a_key_releases_its_previous_buffers_and_clear_resets_the_budget() {
    let mut cache = ResultCache::new(MAX_RESULTS, MAX_RESULT_BYTES);
    let project = Uuid::new_v4();
    let mut oversized_capacity = key(project, "a");
    oversized_capacity.spec.reserve(4096);
    cache.insert(oversized_capacity, result(8));
    let previous_bytes = cache.stats().total_bytes;
    let replacement = key(project, "a");
    let entry = result(4);
    let bytes = entry_bytes(&replacement, &entry);
    cache.insert(replacement, entry);
    assert_eq!(cache.stats().entries, 1);
    assert_eq!(cache.stats().total_bytes, bytes);
    assert!(bytes < previous_bytes);
    cache.clear();
    assert_eq!(cache.stats().entries, 0);
    assert_eq!(cache.stats().total_bytes, 0);
}

#[test]
fn retained_history_cannot_cross_data_or_annotation_revisions() {
    let mut cache = ResultCache::new(MAX_RESULTS, MAX_RESULT_BYTES);
    let mut first = key(Uuid::new_v4(), "annotation-dependent");
    first.annotation_token = Some("1".into());
    cache.insert(first.clone(), CachedResult::new(vec![3], 1, 1, 1));
    let mut next = first.clone();
    next.annotation_token = Some("2".into());
    assert!(cache.get(&next).is_none());
    cache.insert(next.clone(), CachedResult::new(vec![9], 1, 1, 2));
    assert_eq!(cache.get(&first).unwrap().ids.as_slice(), &[3]);
    assert_eq!(cache.get(&next).unwrap().ids.as_slice(), &[9]);
    next.data_revision.modified_nanos += 1;
    assert!(cache.get(&next).is_none());
    next.data_revision = first.data_revision.clone();
    next.data_revision.len += 1;
    assert!(cache.get(&next).is_none());
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
fn annotation_dependencies_follow_tag_operands_not_literal_source_text() {
    for search in [
        "@tag:review",
        "-@tag:*",
        "@TAG:\"triage: mail|review\"",
        "@tag:one|two",
    ] {
        assert!(
            spec_depends_on_annotations(&QuerySpec {
                search: Some(search.into()),
                ..Default::default()
            }),
            "{search}"
        );
    }
    for search in ["tag:review", "\"@tag:review\"", "message:@tag:review"] {
        assert!(
            !spec_depends_on_annotations(&QuerySpec {
                search: Some(search.into()),
                ..Default::default()
            }),
            "{search}"
        );
    }
}

#[test]
fn late_annotation_count_refresh_cannot_roll_back_a_newer_snapshot_or_copy_ids() {
    let mut cache = ResultCache::new(2, MAX_RESULT_BYTES);
    let key = key(Uuid::new_v4(), "data-only");
    let entry = result(8);
    let ids = entry.ids.clone();
    let bytes = entry_bytes(&key, &entry);
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

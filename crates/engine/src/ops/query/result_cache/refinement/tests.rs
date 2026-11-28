use super::super::{normalized_spec_json, CachedResult, DataRevision, MAX_RESULT_BYTES};
use super::*;
use crate::condition::{Condition, Group, GroupKind, Node, Op};
use std::sync::Arc;
use uuid::Uuid;

fn key(spec: &QuerySpec) -> QueryCacheKey {
    QueryCacheKey {
        project_id: Uuid::from_u128(42),
        spec: normalized_spec_json(spec),
        data_revision: DataRevision {
            len: 100,
            modified_nanos: 200,
        },
        annotation_token: None,
    }
}

fn search(text: &str) -> QuerySpec {
    QuerySpec {
        search: Some(text.into()),
        ..Default::default()
    }
}

fn condition(text: &str) -> Node {
    Node::Condition(Condition {
        column: "c000001".into(),
        op: Op::Contains,
        values: vec![text.into()],
        negated: false,
    })
}

fn group(kind: GroupKind, children: Vec<Node>) -> Node {
    Node::Group(Group { kind, children })
}

fn cache(spec: &QuerySpec, ids: Vec<usize>) -> ResultCache {
    let mut cache = ResultCache::new(8, MAX_RESULT_BYTES);
    cache.insert(key(spec), CachedResult::new(ids, 0, 0, 0));
    cache
}

#[test]
fn refinement_reuses_the_arc_and_preserves_the_retained_budget() {
    let mut cache = cache(&search("c000001:\"powershell.exe\""), vec![3, 17]);
    let ids = Arc::clone(&cache.entries[0].1.ids);
    let before = cache.stats();
    let found = cache
        .refinement(
            &key(&search("c000001:\"powershell.exe\" c000002:alice")),
            100,
        )
        .unwrap();
    assert!(Arc::ptr_eq(&ids, &found.ids));
    assert_eq!(
        (
            found.search_prefix,
            found.condition_prefix,
            found.search_terms,
            found.condition_terms
        ),
        (1, 0, 2, 0)
    );
    let after = cache.stats();
    assert_eq!(after.entries, before.entries);
    assert_eq!(after.total_bytes, before.total_bytes);
    assert_eq!(after.hits, before.hits + 1);
    assert_eq!(after.misses, before.misses);
}

#[test]
fn only_added_conjunction_terms_match_and_reordering_does_not() {
    let mut cache = cache(&search("alpha beta"), vec![3]);
    for text in [
        "alpha beta",
        "alpha",
        "alpha betamax gamma",
        "beta alpha gamma",
        "alpha|beta",
    ] {
        assert!(
            cache.refinement(&key(&search(text)), 100).is_none(),
            "{text}"
        );
    }
    let found = cache
        .refinement(&key(&search("alpha beta gamma")), 100)
        .unwrap();
    assert_eq!(found.search_prefix, 2);
    assert_eq!(found.search_terms, 3);
}

#[test]
fn quoted_operators_and_or_precedence_are_not_treated_as_and_prefixes() {
    let mut quoted = cache(&search("\"alpha|beta\""), vec![3]);
    assert!(quoted
        .refinement(&key(&search("\"alpha|beta\" gamma")), 100)
        .is_some());
    assert!(quoted
        .refinement(&key(&search("alpha|beta gamma")), 100)
        .is_none());

    let mut or_base = cache(&search("alpha|beta"), vec![3]);
    // AND has higher precedence, so appending gamma changes the OR subtree.
    assert!(or_base
        .refinement(&key(&search("alpha|beta gamma")), 100)
        .is_none());
    let mut atom = cache(&search("alpha"), vec![3]);
    assert!(atom.refinement(&key(&search("alpha|beta")), 100).is_none());
}

#[test]
fn structured_and_groups_flatten_but_or_subtrees_remain_opaque() {
    let disjunction = group(GroupKind::Or, vec![condition("beta"), condition("gamma")]);
    let old = QuerySpec {
        condition: Some(group(
            GroupKind::And,
            vec![condition("alpha"), disjunction.clone()],
        )),
        ..Default::default()
    };
    let mut cache = cache(&old, vec![3]);
    let new = QuerySpec {
        condition: Some(group(
            GroupKind::And,
            vec![
                group(GroupKind::And, vec![condition("alpha"), disjunction]),
                condition("delta"),
            ],
        )),
        ..Default::default()
    };
    let found = cache.refinement(&key(&new), 100).unwrap();
    assert_eq!((found.condition_prefix, found.condition_terms), (2, 3));
    let changed_or = QuerySpec {
        condition: Some(group(
            GroupKind::And,
            vec![
                condition("alpha"),
                group(
                    GroupKind::Or,
                    vec![condition("beta"), condition("gamma"), condition("delta")],
                ),
            ],
        )),
        ..Default::default()
    };
    assert!(cache.refinement(&key(&changed_or), 100).is_none());
}

#[test]
fn search_and_structured_conditions_keep_separate_origins() {
    let old = QuerySpec {
        condition: Some(condition("alpha")),
        ..Default::default()
    };
    let mut cache = cache(&old, vec![3]);
    assert!(cache.refinement(&key(&search("alpha beta")), 100).is_none());
    let new = QuerySpec {
        search: Some("beta".into()),
        condition: old.condition,
        ..Default::default()
    };
    let found = cache.refinement(&key(&new), 100).unwrap();
    assert_eq!(
        (
            found.search_prefix,
            found.condition_prefix,
            found.search_terms,
            found.condition_terms
        ),
        (0, 1, 1, 1)
    );
}

#[test]
fn tag_preparation_distinguishes_any_tag_from_a_literal_star() {
    let mut any = cache(&search("@tag:*"), vec![3]);
    assert!(any
        .refinement(&key(&search("@tag:%2A alpha")), 100)
        .is_none());
    assert!(any
        .refinement(&key(&search("@tag:\"*\" alpha")), 100)
        .is_none());
    let mut decoded = cache(&search("@tag:%41"), vec![3]);
    assert!(decoded
        .refinement(&key(&search("@tag:a alpha")), 100)
        .is_some());
    assert!(decoded
        .refinement(&key(&search("@tag:\"%41\" alpha")), 100)
        .is_none());
}

#[test]
fn project_data_and_annotation_revisions_must_be_compatible() {
    for change in 0..5 {
        let mut cache = cache(&search("alpha"), vec![3]);
        let mut new = key(&search("alpha beta"));
        match change {
            0 => new.project_id = Uuid::from_u128(99),
            1 => new.data_revision.len += 1,
            2 => new.data_revision.modified_nanos += 1,
            3 => {
                cache.entries[0].0.annotation_token = Some("7".into());
                new.annotation_token = Some("8".into());
            }
            _ => cache.entries[0].0.annotation_token = Some("7".into()),
        }
        assert!(cache.refinement(&new, 100).is_none(), "change {change}");
    }
    let mut cache = cache(&search("alpha"), vec![3]);
    let mut new = key(&search("alpha beta"));
    new.annotation_token = Some("8".into());
    assert!(
        cache.refinement(&new, 100).is_some(),
        "a data-only base survives annotation edits"
    );
    cache.entries[0].0.annotation_token = Some("8".into());
    assert!(cache.refinement(&new, 100).is_some());
}

#[test]
fn scope_order_flags_bookmarks_and_sort_must_match() {
    let old = QuerySpec {
        search: Some("alpha".into()),
        search_columns: Some(vec!["c000001".into(), "c000002".into()]),
        flag_filter: Some("critical".into()),
        bookmarked_only: Some(true),
        sort_key: Some("c000001".into()),
        sort_direction: Some("desc".into()),
        condition: None,
    };
    for change in 0..5 {
        let mut cache = cache(&old, vec![3]);
        let mut new = old.clone();
        new.search = Some("alpha beta".into());
        match change {
            0 => new.search_columns.as_mut().unwrap().reverse(),
            1 => new.flag_filter = None,
            2 => new.bookmarked_only = None,
            3 => new.sort_key = Some("c000002".into()),
            _ => new.sort_direction = Some("asc".into()),
        }
        assert!(
            cache.refinement(&key(&new), 100).is_none(),
            "change {change}"
        );
    }
    let mut cache = cache(&old, vec![17, 3]);
    let mut new = old;
    new.search = Some("alpha beta".into());
    assert_eq!(
        cache.refinement(&key(&new), 100).unwrap().ids.as_slice(),
        &[17, 3]
    );
}

#[test]
fn an_unrestricted_empty_base_is_useless_but_selective_flags_and_empty_ids_are_valid() {
    let mut full = cache(&QuerySpec::default(), vec![0, 1, 2]);
    assert!(full.refinement(&key(&search("alpha")), 3).is_none());
    let old = QuerySpec {
        flag_filter: Some("critical".into()),
        ..Default::default()
    };
    let mut selective = cache(&old, vec![1]);
    let new = QuerySpec {
        search: Some("alpha".into()),
        ..old
    };
    let found = selective.refinement(&key(&new), 3).unwrap();
    assert_eq!((found.search_prefix, found.condition_prefix), (0, 0));
    let mut empty = cache(&search("alpha"), Vec::new());
    assert!(empty
        .refinement(&key(&search("alpha beta")), 100)
        .unwrap()
        .ids
        .is_empty());
}

#[test]
fn the_smallest_candidate_wins_then_the_longest_prefix_breaks_ties() {
    let mut cache = cache(&search("alpha"), vec![1, 2, 3]);
    cache.insert(
        key(&search("alpha beta")),
        CachedResult::new(vec![1], 0, 0, 0),
    );
    cache.insert(
        key(&search("alpha beta gamma")),
        CachedResult::new(vec![1, 2], 0, 0, 0),
    );
    let new = key(&search("alpha beta gamma delta"));
    assert_eq!(cache.refinement(&new, 100).unwrap().search_prefix, 2);
    cache.insert(
        key(&search("alpha beta gamma")),
        CachedResult::new(vec![1], 0, 0, 0),
    );
    assert_eq!(cache.refinement(&new, 100).unwrap().search_prefix, 3);
}

#[test]
fn refinement_hits_promote_the_base_before_lru_eviction() {
    let mut cache = ResultCache::new(2, MAX_RESULT_BYTES);
    let oldest = key(&search("alpha"));
    let other = key(&search("unrelated"));
    cache.insert(oldest.clone(), CachedResult::new(vec![1], 0, 0, 0));
    cache.insert(other.clone(), CachedResult::new(vec![2], 0, 0, 0));
    assert!(cache.refinement(&key(&search("alpha beta")), 100).is_some());
    cache.insert(key(&search("third")), CachedResult::new(vec![3], 0, 0, 0));
    assert!(cache.get(&oldest).is_some());
    assert!(cache.get(&other).is_none());
}

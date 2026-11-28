//! Reuse completed conjunction prefixes without retaining another row-id index.

use std::sync::Arc;

use crate::{
    condition::{search_tokens_to_node, GroupKind, Node},
    search::{prepare_tag_tokens, tokenize_search_query},
};

use super::{QueryCacheKey, QuerySpec, ResultCache};

#[derive(Debug)]
pub(crate) struct CachedRefinement {
    pub ids: Arc<Vec<usize>>,
    pub search_prefix: usize,
    pub condition_prefix: usize,
    pub search_terms: usize,
    pub condition_terms: usize,
}

struct Clauses {
    search: Vec<Node>,
    condition: Vec<Node>,
}

impl Clauses {
    fn parse(spec: &QuerySpec) -> Option<Self> {
        // Keep the same tokenizer and tag decoding order as PreparedQuery.
        // Quoted literals and OR precedence must never be inferred from text.
        let tokens = tokenize_search_query(spec.search.as_deref().unwrap_or_default().trim());
        let tokens = prepare_tag_tokens(&tokens).ok()?;
        let search = search_tokens_to_node(&tokens).ok()?;
        Some(Self {
            search: conjunction(search),
            condition: conjunction(spec.condition.clone()),
        })
    }

    fn len(&self) -> usize {
        self.search.len() + self.condition.len()
    }

    fn extends(&self, prefix: &Self) -> bool {
        self.len() > prefix.len()
            && self.search.starts_with(&prefix.search)
            && self.condition.starts_with(&prefix.condition)
    }
}

/// Flatten only conjunctions at the root. An OR subtree is one opaque clause.
fn conjunction(node: Option<Node>) -> Vec<Node> {
    let mut pending: Vec<Node> = node.into_iter().collect();
    let mut clauses = Vec::new();
    while let Some(node) = pending.pop() {
        match node {
            Node::Group(group) if group.kind == GroupKind::And => {
                pending.extend(group.children.into_iter().rev());
            }
            node => clauses.push(node),
        }
    }
    clauses
}

fn same_options(base: &QuerySpec, requested: &QuerySpec) -> bool {
    base.search_columns == requested.search_columns
        && base.flag_filter == requested.flag_filter
        && base.bookmarked_only == requested.bookmarked_only
        && base.sort_key == requested.sort_key
        && base.sort_direction == requested.sort_direction
}

impl ResultCache {
    pub(super) fn refinement(
        &mut self,
        key: &QueryCacheKey,
        total_rows: usize,
    ) -> Option<CachedRefinement> {
        let requested: QuerySpec = serde_json::from_str(&key.spec).ok()?;
        let requested_clauses = Clauses::parse(&requested)?;
        let mut best: Option<(usize, CachedRefinement)> = None;
        for (position, (base_key, result)) in self.entries.iter().enumerate() {
            if base_key.project_id != key.project_id
                || base_key.data_revision != key.data_revision
                || (base_key.annotation_token.is_some()
                    && base_key.annotation_token != key.annotation_token)
            {
                continue;
            }
            let Ok(base) = serde_json::from_str::<QuerySpec>(&base_key.spec) else {
                continue;
            };
            if !same_options(&base, &requested) {
                continue;
            }
            let Some(base_clauses) = Clauses::parse(&base) else {
                continue;
            };
            if !requested_clauses.extends(&base_clauses)
                || (base_clauses.len() == 0 && result.ids.len() == total_rows)
            {
                continue;
            }
            let improves = best.as_ref().is_none_or(|(_, known)| {
                result.ids.len() < known.ids.len()
                    || (result.ids.len() == known.ids.len()
                        && base_clauses.len() > known.search_prefix + known.condition_prefix)
            });
            if improves {
                best = Some((
                    position,
                    CachedRefinement {
                        ids: Arc::clone(&result.ids),
                        search_prefix: base_clauses.search.len(),
                        condition_prefix: base_clauses.condition.len(),
                        search_terms: requested_clauses.search.len(),
                        condition_terms: requested_clauses.condition.len(),
                    },
                ));
            }
        }
        let (position, refinement) = best?;
        let entry = self.entries.remove(position);
        self.entries.push(entry);
        self.hits += 1;
        Some(refinement)
    }
}

#[cfg(test)]
mod tests;

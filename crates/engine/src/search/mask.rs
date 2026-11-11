use std::collections::HashMap;

use super::SearchToken;

#[allow(clippy::too_many_arguments)]
pub fn build_search_mask_boolean(
    rpn: &[SearchToken],
    terms: &[(Option<String>, String)],
    searchable_text: &[String],
    // Optional: per-column searchable texts; when None, falls back to row-wide text
    per_column: Option<&HashMap<String, Vec<String>>>,
) -> Vec<bool> {
    // Precompute per-(col,term) masks
    let mut key_masks: HashMap<(Option<String>, String), Vec<bool>> = HashMap::new();
    for (col_opt, term) in terms {
        let key = (col_opt.clone(), term.clone());
        if key_masks.contains_key(&key) {
            continue;
        }
        let mut mask = vec![false; searchable_text.len()];
        match (col_opt.as_ref().map(|c| c.to_lowercase()), per_column) {
            (Some(col), Some(per_col)) => {
                if let Some(col_texts) = per_col.get(&col) {
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..searchable_text.len() {
                        if let Some(t) = col_texts.get(i) {
                            if !t.is_empty() && t.contains(term) {
                                mask[i] = true;
                            }
                        }
                    }
                }
            }
            _ => {
                for i in 0..searchable_text.len() {
                    if !searchable_text[i].is_empty() && searchable_text[i].contains(term) {
                        mask[i] = true;
                    }
                }
            }
        }
        key_masks.insert(key, mask);
    }

    // Evaluate per row
    let mut mask_out = vec![false; searchable_text.len()];
    #[allow(clippy::needless_range_loop)]
    for i in 0..searchable_text.len() {
        let mut stack: Vec<bool> = Vec::new();
        for tok in rpn {
            match tok {
                SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } => {
                    let key = (col.clone(), text.clone());
                    let v = key_masks
                        .get(&key)
                        .and_then(|m| m.get(i))
                        .copied()
                        .unwrap_or(false);
                    stack.push(v);
                }
                SearchToken::Not => {
                    let a = stack.pop().unwrap_or(false);
                    stack.push(!a);
                }
                SearchToken::And => {
                    let b = stack.pop().unwrap_or(false);
                    let a = stack.pop().unwrap_or(false);
                    stack.push(a && b);
                }
                SearchToken::Or => {
                    let b = stack.pop().unwrap_or(false);
                    let a = stack.pop().unwrap_or(false);
                    stack.push(a || b);
                }
            }
        }
        mask_out[i] = stack.pop().unwrap_or(false);
    }
    mask_out
}

use std::collections::HashMap;

use polars::prelude::Series;

use crate::value_utils::anyvalue_to_search_string;

// Boolean-search support: tokens, RPN conversion, and evaluation on prebuilt per-row searchable text
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchToken {
    Term { col: Option<String>, text: String },
    QuotedTerm { col: Option<String>, text: String },
    And,
    Or,
    Not,
}

fn is_operand_token(tok: &SearchToken) -> bool {
    matches!(
        tok,
        SearchToken::Term { .. } | SearchToken::QuotedTerm { .. }
    )
}

pub fn tokenize_search_query(input: &str) -> Vec<SearchToken> {
    // Token rules:
    // - Phrases in double quotes become a single Term (without quotes)
    // - OR operator: word "OR" (upper case) or pipe character '|'
    // - AND operator: explicit "AND" allowed, but also implicit between operands (handled later)
    // - NOT operator: unary, written as leading '-' before a term, or explicit word "NOT"
    // - Case-insensitive matching overall; terms are lowercased here
    let mut raw_parts: Vec<(String, bool)> = Vec::new(); // (text, quoted)
    let mut buf = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes {
                    // end quote -> push buffer as a part
                    if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), true));
                    }
                    buf.clear();
                    in_quotes = false;
                } else {
                    // start quote -> flush current buf as part if any
                    if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), false));
                    }
                    buf.clear();
                    in_quotes = true;
                }
            }
            '|' => {
                if in_quotes {
                    buf.push(ch);
                } else {
                    if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), false));
                    }
                    buf.clear();
                    raw_parts.push(("|".to_string(), false));
                    // collapse consecutive pipes
                    while let Some('|') = chars.peek() {
                        chars.next();
                    }
                }
            }
            c if c.is_whitespace() => {
                if in_quotes {
                    buf.push(c);
                } else {
                    if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), false));
                    }
                    buf.clear();
                }
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() {
        raw_parts.push((buf.trim().to_string(), in_quotes));
    }

    // Merge pattern: col: "phrase with space" → single quoted term with column
    let mut merged: Vec<(String, bool)> = Vec::new();
    let mut i = 0usize;
    while i < raw_parts.len() {
        let (ref part, quoted) = raw_parts[i];
        if !quoted && part.ends_with(':') && i + 1 < raw_parts.len() && raw_parts[i + 1].1 {
            let col = part[..part.len() - 1].to_string();
            let phrase = raw_parts[i + 1].0.clone();
            merged.push((format!("{}:\"{}\"", col, phrase), true));
            i += 2;
            continue;
        }
        merged.push((part.clone(), quoted));
        i += 1;
    }

    // Map merged parts to tokens with unary '-' handling and column prefixes
    let mut tokens: Vec<SearchToken> = Vec::new();
    for (part, quoted) in merged {
        if part == "|" && !quoted {
            tokens.push(SearchToken::Or);
            continue;
        }
        // Do not treat words AND/OR/NOT as operators; users must use space, '|', or '-' only
        // Hyphen NOT: -term or -col:term (only when not quoted)
        if !quoted && part.starts_with('-') && part.len() > 1 {
            tokens.push(SearchToken::Not);
            let rest = &part[1..];
            if let Some(pos) = (!quoted).then(|| rest.find(':')).flatten() {
                let (c, t) = rest.split_at(pos);
                let text = t[1..].to_lowercase();
                let col = c.to_lowercase();
                tokens.push(SearchToken::Term {
                    col: Some(col),
                    text,
                });
            } else {
                tokens.push(SearchToken::Term {
                    col: None,
                    text: rest.to_lowercase(),
                });
            }
            continue;
        }
        // Column prefix: col:term (unquoted) or col:"phrase" (merged, quoted=true)
        if let Some(pos) = (!quoted).then(|| part.find(':')).flatten() {
            let (c, t) = part.split_at(pos);
            let text = t[1..].to_lowercase();
            let col = c.to_lowercase();
            tokens.push(SearchToken::Term {
                col: Some(col),
                text,
            });
            continue;
        }
        if quoted {
            // Only treat as column-scoped when pattern is col:"phrase" (merged case)
            if let Some(pos) = part.find(":\"") {
                let (c, t) = part.split_at(pos);
                let text_raw = t[1..].trim();
                let text = text_raw.trim_matches('"').to_lowercase();
                let col = c.to_lowercase();
                tokens.push(SearchToken::QuotedTerm {
                    col: Some(col),
                    text,
                });
            } else {
                tokens.push(SearchToken::QuotedTerm {
                    col: None,
                    text: part.to_lowercase(),
                });
            }
        } else {
            tokens.push(SearchToken::Term {
                col: None,
                text: part.to_lowercase(),
            });
        }
    }

    // Column carry-over across OR: if an operand with a column is followed by
    // an OR and then an operand without a column, apply the same column to the
    // following operand. This enables queries like `com:WS01|WS02|WS03` to be
    // interpreted as `com:WS01 OR com:WS02 OR com:WS03`.
    let mut adjusted: Vec<SearchToken> = Vec::with_capacity(tokens.len());
    let mut last_operand_col: Option<String> = None;
    let mut carry_col_for_next: Option<String> = None;
    for tok in tokens.into_iter() {
        match tok {
            SearchToken::Term { col, text } => {
                let new_col = if col.is_none() {
                    carry_col_for_next.take().or(col)
                } else {
                    col
                };
                last_operand_col = new_col.clone();
                adjusted.push(SearchToken::Term { col: new_col, text });
            }
            SearchToken::QuotedTerm { col, text } => {
                let new_col = if col.is_none() {
                    carry_col_for_next.take().or(col)
                } else {
                    col
                };
                last_operand_col = new_col.clone();
                adjusted.push(SearchToken::QuotedTerm { col: new_col, text });
            }
            SearchToken::Or => {
                // Set carry to the last seen operand column; it will apply to the next operand
                carry_col_for_next = last_operand_col.clone();
                adjusted.push(SearchToken::Or);
            }
            SearchToken::And => {
                // AND should not carry column context
                carry_col_for_next = None;
                adjusted.push(SearchToken::And);
            }
            SearchToken::Not => {
                // Keep carry across NOT so patterns like `com:x|-y` become `com:x OR -com:y`
                adjusted.push(SearchToken::Not);
            }
        }
    }

    // Insert implicit ANDs between adjacent operands (or operand followed by NOT)
    let mut with_and: Vec<SearchToken> = Vec::new();
    let mut i = 0usize;
    while i < adjusted.len() {
        let cur = adjusted[i].clone();
        with_and.push(cur.clone());
        if i + 1 < adjusted.len() {
            let a = &adjusted[i];
            let b = &adjusted[i + 1];
            let a_is_operand = is_operand_token(a);
            let b_starts_operand = is_operand_token(b) || matches!(b, SearchToken::Not);
            if a_is_operand && b_starts_operand {
                with_and.push(SearchToken::And);
            }
        }
        i += 1;
    }
    with_and
}

pub fn to_rpn(tokens: &[SearchToken]) -> Vec<SearchToken> {
    // Shunting-yard without parentheses. Precedence: NOT(3, right), AND(2, left), OR(1, left)
    fn precedence(tok: &SearchToken) -> (u8, bool) {
        match tok {
            SearchToken::Not => (3, true),
            SearchToken::And => (2, false),
            SearchToken::Or => (1, false),
            SearchToken::Term { .. } | SearchToken::QuotedTerm { .. } => (0, false),
        }
    }

    let mut output: Vec<SearchToken> = Vec::new();
    let mut ops: Vec<SearchToken> = Vec::new();

    for tok in tokens {
        match tok {
            SearchToken::Term { .. } | SearchToken::QuotedTerm { .. } => output.push(tok.clone()),
            SearchToken::And | SearchToken::Or | SearchToken::Not => {
                let (p_cur, right_assoc) = precedence(tok);
                while let Some(top) = ops.last() {
                    let (p_top, _) = precedence(top);
                    let should_pop = if right_assoc {
                        p_cur < p_top
                    } else {
                        p_cur <= p_top
                    };
                    if should_pop {
                        output.push(ops.pop().unwrap());
                    } else {
                        break;
                    }
                }
                ops.push(tok.clone());
            }
        }
    }
    while let Some(op) = ops.pop() {
        output.push(op);
    }
    output
}


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

pub fn build_searchable_text(
    row_count: usize,
    search_cols: &[String],
    column_series: &HashMap<&str, &Series>,
) -> Vec<String> {
    let mut searchable_text: Vec<String> = vec![String::new(); row_count];
    for col in search_cols {
        if let Some(series) = column_series.get(col.as_str()) {
            for row_idx in 0..row_count {
                if let Ok(value) = series.get(row_idx) {
                    if let Some(text) = anyvalue_to_search_string(&value) {
                        let lower = text.to_lowercase();
                        if lower.is_empty() {
                            continue;
                        }
                        let entry = &mut searchable_text[row_idx];
                        if entry.is_empty() {
                            entry.push_str(&lower);
                        } else {
                            entry.push(' ');
                            entry.push_str(&lower);
                        }
                    }
                }
            }
        }
    }
    searchable_text
}

pub fn ensure_searchable_text<'a>(
    storage: &'a mut Option<Vec<String>>,
    built_flag: &mut bool,
    row_count: usize,
    search_cols: &[String],
    column_series: &HashMap<&str, &Series>,
) -> &'a Vec<String> {
    if storage.is_none() {
        let built = build_searchable_text(row_count, search_cols, column_series);
        *storage = Some(built);
        *built_flag = true;
    }
    storage.as_ref().unwrap()
}

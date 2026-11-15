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
    // Text, quoted, and a minus directly before the opening quote.
    let mut raw_parts: Vec<(String, bool, bool)> = Vec::new();
    let mut buf = String::new();
    let mut in_quotes = false;
    let mut quoted_negated = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes {
                    if !buf.is_empty() || quoted_negated {
                        // A quoted space is a literal predicate, not an empty
                        // query. Keep the existing trim for ordinary phrases.
                        let text = if buf.trim().is_empty() {
                            &buf
                        } else {
                            buf.trim()
                        };
                        raw_parts.push((text.to_string(), true, quoted_negated));
                    }
                    buf.clear();
                    in_quotes = false;
                    quoted_negated = false;
                } else {
                    if buf == "-" {
                        quoted_negated = true;
                    } else if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), false, false));
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
                        raw_parts.push((buf.trim().to_string(), false, false));
                    }
                    buf.clear();
                    raw_parts.push(("|".to_string(), false, false));
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
                        raw_parts.push((buf.trim().to_string(), false, false));
                    }
                    buf.clear();
                }
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() || (in_quotes && (!buf.is_empty() || quoted_negated)) {
        let text = if in_quotes && buf.trim().is_empty() {
            &buf
        } else {
            buf.trim()
        };
        raw_parts.push((text.to_string(), in_quotes, quoted_negated));
    }

    // `col:"phrase with space"` arrives as two parts; merge them before scoping.
    let mut merged: Vec<(String, bool, bool)> = Vec::new();
    let mut i = 0usize;
    while i < raw_parts.len() {
        let (ref part, quoted, negated) = raw_parts[i];
        if !quoted
            && part.ends_with(':')
            && i + 1 < raw_parts.len()
            && raw_parts[i + 1].1
            && !raw_parts[i + 1].2
        {
            let col = part[..part.len() - 1].to_string();
            let phrase = raw_parts[i + 1].0.clone();
            merged.push((format!("{}:\"{}\"", col, phrase), true, negated));
            i += 2;
            continue;
        }
        merged.push((part.clone(), quoted, negated));
        i += 1;
    }

    // Words AND/OR/NOT are not operators. Only space, '|', and a leading '-' are.
    let mut tokens: Vec<SearchToken> = Vec::new();
    for (mut part, quoted, negated) in merged {
        if negated {
            tokens.push(SearchToken::Not);
        }
        // The merged `-column:"phrase"` retains its quotes, but its leading
        // minus is still the same NOT operator as in `-column:value`.
        // A quoted literal such as `"-column:value"` remains literal.
        if quoted && part.starts_with('-') && part.contains(":\"") {
            tokens.push(SearchToken::Not);
            part.remove(0);
        }
        if part == "|" && !quoted {
            tokens.push(SearchToken::Or);
            continue;
        }
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

    // `com:WS01|WS02` carries the column across OR (and across a following NOT).
    // Only the next operand can inherit it; an explicit scope consumes it too.
    let mut adjusted: Vec<SearchToken> = Vec::with_capacity(tokens.len());
    let mut last_operand_col: Option<String> = None;
    let mut carry_col_for_next: Option<String> = None;
    for tok in tokens.into_iter() {
        match tok {
            SearchToken::Term { col, text } => {
                let new_col = col.or(carry_col_for_next.take());
                last_operand_col = new_col.clone();
                adjusted.push(SearchToken::Term { col: new_col, text });
            }
            SearchToken::QuotedTerm { col, text } => {
                let new_col = col.or(carry_col_for_next.take());
                last_operand_col = new_col.clone();
                adjusted.push(SearchToken::QuotedTerm { col: new_col, text });
            }
            SearchToken::Or => {
                carry_col_for_next = last_operand_col.clone();
                adjusted.push(SearchToken::Or);
            }
            SearchToken::And => {
                carry_col_for_next = None;
                adjusted.push(SearchToken::And);
            }
            SearchToken::Not => {
                adjusted.push(SearchToken::Not);
            }
        }
    }

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

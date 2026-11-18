use super::{prepare_tag_tokens, tokenize_search_query, SearchToken};

const MAX_QUERY_BYTES: usize = 64 * 1024;
const MAX_TERMS: usize = 64;
const MAX_TOKENS: usize = MAX_TERMS * 3;

pub(crate) fn validate_search_input_size(input: &str) -> Result<(), String> {
    if input.len() > MAX_QUERY_BYTES {
        return Err("Invalid search query: text exceeds the 64 KiB limit.".into());
    }
    Ok(())
}

/// Empty text intentionally means no filter. Nonempty text must describe an
/// actual predicate, rather than silently falling back to that empty filter.
pub fn validate_search_query(input: &str) -> Result<(), String> {
    validate_search_input_size(input)?;
    if input.trim().is_empty() {
        return Ok(());
    }
    let tokens = tokenize_search_query(input);
    validate_search_tokens(&tokens)?;
    // Tag syntax is part of the same language, including percent escapes.
    prepare_tag_tokens(&tokens).map(|_| ())
}

pub(crate) fn validate_search_tokens(tokens: &[SearchToken]) -> Result<(), String> {
    if tokens.len() > MAX_TOKENS {
        return Err(format!(
            "Invalid search query: exceeds the limit of {MAX_TOKENS} tokens."
        ));
    }
    let mut terms = 0usize;
    let mut operand_expected = true;
    for token in tokens {
        match token {
            SearchToken::Term { text, .. } | SearchToken::QuotedTerm { text, .. } => {
                terms += 1;
                if terms > MAX_TERMS {
                    return Err(format!(
                        "Invalid search query: exceeds the limit of {MAX_TERMS} terms."
                    ));
                }
                if text.is_empty() {
                    return Err("Invalid search query: enter text after the column name.".into());
                }
                if !operand_expected {
                    return Err("Invalid search query: missing an operator between terms.".into());
                }
                operand_expected = false;
            }
            SearchToken::Not if operand_expected => {}
            SearchToken::And | SearchToken::Or if !operand_expected => operand_expected = true,
            _ => {
                return Err(
                    "Invalid search query: an operator needs a search term on each side.".into(),
                )
            }
        }
    }
    if operand_expected {
        return Err(
            "Invalid search query: enter at least one search term and complete each operator."
                .into(),
        );
    }
    Ok(())
}

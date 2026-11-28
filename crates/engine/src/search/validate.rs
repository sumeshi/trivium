use super::{
    prepare_tag_tokens, tokenize_search_query, SearchToken, TAG_ANY_COLUMN, TAG_EXACT_COLUMN,
};

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
    for token in tokens {
        if let SearchToken::Term { col: Some(col), .. }
        | SearchToken::QuotedTerm { col: Some(col), .. } = token
        {
            if col.eq_ignore_ascii_case(TAG_EXACT_COLUMN)
                || col.eq_ignore_ascii_case(TAG_ANY_COLUMN)
            {
                return Err("Invalid search query: internal column scopes are reserved.".into());
            }
        }
    }
    validate_prepared_search_tokens(tokens)
}

/// Grammar validation after raw scopes have been checked and ordinary @tag
/// operands have been translated to their internal column identifiers.
pub(crate) fn validate_prepared_search_tokens(tokens: &[SearchToken]) -> Result<(), String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_queries_cannot_supply_internal_tag_scopes() {
        for scope in [TAG_EXACT_COLUMN, TAG_ANY_COLUMN, "\0TRIVIUM-TAG"] {
            for query in [
                format!("{scope}:triage"),
                format!("-{scope}:*"),
                format!("{scope}:\"triage\""),
                format!("{scope}:triage|review"),
            ] {
                assert!(
                    validate_search_query(&query)
                        .unwrap_err()
                        .contains("reserved"),
                    "{query:?}"
                );
            }
        }
    }

    #[test]
    fn reserved_scopes_do_not_restrict_literal_values_or_prepared_tags() {
        for query in [
            "@tag:triage",
            "@tag:*",
            "@tag:nul\0tag",
            "\"@tag:triage\"",
            "\"\0trivium-tag:triage\"",
            "message:\0trivium-tag:triage",
            "ordinary\0value",
        ] {
            validate_search_query(query).unwrap();
            let prepared = prepare_tag_tokens(&tokenize_search_query(query)).unwrap();
            validate_prepared_search_tokens(&prepared).unwrap();
            crate::condition::search_tokens_to_node(&prepared).unwrap();
        }
    }
}

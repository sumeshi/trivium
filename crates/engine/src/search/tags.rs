use super::{tokenize_search_query, SearchToken};

pub(crate) const TAG_EXACT_COLUMN: &str = "\0trivium-tag";
pub(crate) const TAG_ANY_COLUMN: &str = "\0trivium-any-tag";

pub(crate) fn is_tag_scope(scope: &str) -> bool {
    scope.eq_ignore_ascii_case("@tag")
}

pub(crate) fn search_reads_tags(input: &str) -> bool {
    input.contains('@')
        && tokenize_search_query(input)
            .iter()
            .any(|token| match token {
                SearchToken::Term { col: Some(col), .. }
                | SearchToken::QuotedTerm { col: Some(col), .. } => is_tag_scope(col),
                _ => false,
            })
}

/// Tag escapes are decoded after tokenization, so values cannot create syntax.
pub(crate) fn prepare_tag_tokens(tokens: &[SearchToken]) -> Result<Vec<SearchToken>, String> {
    tokens
        .iter()
        .map(|token| {
            let (col, text, quoted) = match token {
                SearchToken::Term {
                    col: Some(col),
                    text,
                } => (col, text, false),
                SearchToken::QuotedTerm {
                    col: Some(col),
                    text,
                } => (col, text, true),
                _ => return Ok(token.clone()),
            };
            if !is_tag_scope(col) {
                return Ok(token.clone());
            }
            if text.is_empty() {
                return Err("Enter a tag after @tag:.".into());
            }
            let (column, value) = if !quoted && text == "*" {
                (TAG_ANY_COLUMN, text.clone())
            } else {
                let value = if quoted {
                    text.clone()
                } else {
                    decode_tag(text)?
                };
                (TAG_EXACT_COLUMN, value.to_lowercase())
            };
            let col = Some(column.to_string());
            Ok(if quoted {
                SearchToken::QuotedTerm { col, text: value }
            } else {
                SearchToken::Term { col, text: value }
            })
        })
        .collect()
}

fn decode_tag(text: &str) -> Result<String, String> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' {
            let hex = |byte: u8| (byte as char).to_digit(16).map(|value| value as u8);
            let value = bytes
                .get(at + 1..at + 3)
                .and_then(|pair| Some((hex(pair[0])? << 4) | hex(pair[1])?))
                .ok_or("Invalid @tag percent escape.")?;
            decoded.push(value);
            at += 3;
        } else {
            decoded.push(bytes[at]);
            at += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "Invalid @tag UTF-8 escape.".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(input: &str) -> Vec<SearchToken> {
        prepare_tag_tokens(&tokenize_search_query(input)).unwrap()
    }

    fn term(column: &str, text: &str) -> SearchToken {
        SearchToken::Term {
            col: Some(column.into()),
            text: text.into(),
        }
    }

    #[test]
    fn detects_only_actual_tag_scopes_including_or_carry() {
        for input in [
            "@tag:foo",
            "host:foo|@TAG:bar",
            "@tag:first|-second",
            "-@tag:\"a b\"",
        ] {
            assert!(search_reads_tags(input), "{input}");
        }
        for input in [
            "",
            "needle",
            "user@example.com",
            "\"@tag:foo\"",
            "source:@tag:foo",
            "tag:foo",
            "@tags:foo",
        ] {
            assert!(!search_reads_tags(input), "{input}");
        }
        assert!(is_tag_scope("@TaG"));
        assert!(!is_tag_scope("tag"));
    }

    #[test]
    fn scope_carry_and_boolean_operators_are_preserved() {
        assert_eq!(
            prepared("@tag:Alpha|-Bravo ordinary -@tag:Charlie"),
            vec![
                term(TAG_EXACT_COLUMN, "alpha"),
                SearchToken::Or,
                SearchToken::Not,
                term(TAG_EXACT_COLUMN, "bravo"),
                SearchToken::And,
                SearchToken::Term {
                    col: None,
                    text: "ordinary".into()
                },
                SearchToken::And,
                SearchToken::Not,
                term(TAG_EXACT_COLUMN, "charlie"),
            ],
        );
        let tokens = tokenize_search_query("host:WS01|WS02 -name:\"Jane Doe\" source:%GG");
        assert_eq!(prepare_tag_tokens(&tokens).unwrap(), tokens);
    }

    #[test]
    fn only_bare_unquoted_star_means_any_tag() {
        assert_eq!(prepared("@tag:*"), vec![term(TAG_ANY_COLUMN, "*")]);
        assert_eq!(prepared("@tag:%2A"), vec![term(TAG_EXACT_COLUMN, "*")]);
        assert_eq!(
            prepared("@tag:\"*\""),
            vec![SearchToken::QuotedTerm {
                col: Some(TAG_EXACT_COLUMN.into()),
                text: "*".into(),
            }]
        );
        assert_eq!(
            prepared("@tag:\"%20|A:B*\""),
            vec![SearchToken::QuotedTerm {
                col: Some(TAG_EXACT_COLUMN.into()),
                text: "%20|a:b*".into(),
            }]
        );
    }

    #[test]
    fn decoded_values_cannot_add_operators_and_are_decoded_exactly_once() {
        assert_eq!(
            prepared("@tag:A%20B%7CC%3AD%22E%5CF%25G%00%0A%09%F0%9F%8C%B8"),
            vec![term(TAG_EXACT_COLUMN, "a b|c:d\"e\\f%g\0\n\t🌸")],
        );
        assert_eq!(prepared("@tag:%2520"), vec![term(TAG_EXACT_COLUMN, "%20")]);
        assert_eq!(prepared("@tag:%252F"), vec![term(TAG_EXACT_COLUMN, "%2f")]);
        assert_eq!(
            prepared("@tag:%C4%B0%CE%A3"),
            vec![term(TAG_EXACT_COLUMN, "i\u{307}ς")]
        );
        assert_eq!(
            prepared("@tag:猫+%20"),
            vec![term(TAG_EXACT_COLUMN, "猫+ ")]
        );
    }

    #[test]
    fn empty_tags_and_malformed_percent_or_utf8_are_errors() {
        for input in [
            "@tag:",
            "@tag:\"\"",
            "@tag:%",
            "@tag:%0",
            "@tag:%GG",
            "@tag:%C3%28",
            "@tag:%FF",
        ] {
            assert!(
                prepare_tag_tokens(&tokenize_search_query(input)).is_err(),
                "{input}"
            );
        }
        // A malformed escape is ordinary text outside the metadata scope.
        let tokens = tokenize_search_query("source:%FF");
        assert_eq!(prepare_tag_tokens(&tokens).unwrap(), tokens);
        assert_eq!(
            prepared("@tag:\"%FF\""),
            vec![SearchToken::QuotedTerm {
                col: Some(TAG_EXACT_COLUMN.into()),
                text: "%ff".into(),
            }]
        );
    }
}

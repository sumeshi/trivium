//! Resolve search-bar column labels before projection, caching or evaluation.

use crate::{error::AppError, models::ColumnMeta};

use super::QuerySpec;

impl QuerySpec {
    /// The editor/API continue to store the user's original query. Only this
    /// request's search is normalized; condition and sort IDs stay untouched.
    /// Legacy projects without column metadata already use their source names.
    pub fn resolve_search_labels(&mut self, columns: &[ColumnMeta]) -> Result<(), AppError> {
        if columns.is_empty() {
            return Ok(());
        }
        if let Some(search) = self.search.as_deref().filter(|search| search.contains(':')) {
            self.search = Some(rewrite_scopes(search, columns)?);
        }
        Ok(())
    }
}

fn resolve_label<'a>(name: &str, columns: &'a [ColumnMeta]) -> Result<Option<&'a str>, AppError> {
    // An empty scope has the tokenizer's joined-text meaning, even when the
    // source contains an unnamed CSV header.
    if name.is_empty() {
        return Ok(None);
    }
    // Explicit IDs win even if another source happens to have that label.
    if let Some(column) = columns
        .iter()
        .find(|column| column.id.eq_ignore_ascii_case(name))
    {
        return Ok(Some(&column.id));
    }
    let folded = name.to_lowercase();
    let mut matches = columns.iter().filter(|column| {
        column.label.to_lowercase() == folded || column.source.to_lowercase() == folded
    });
    let Some(first) = matches.next() else {
        return Ok(None);
    };
    if let Some(other) = matches.find(|column| column.id != first.id) {
        return Err(AppError::Message(format!(
            "Ambiguous search column: {name}. Use an internal ID such as {} or {}.",
            first.id, other.id
        )));
    }
    Ok(Some(&first.id))
}

/// Replace only a field prefix at the beginning of an unquoted operand.
/// Values, whitespace, negation, quotes and OR carry remain byte-for-byte as
/// entered, so the existing tokenizer remains the single syntax definition.
fn rewrite_scopes(input: &str, columns: &[ColumnMeta]) -> Result<String, AppError> {
    let mut output = String::with_capacity(input.len());
    let mut copied = 0;
    let mut offset = 0;
    let mut quoted = false;
    let mut operand_start = true;
    while offset < input.len() {
        let ch = input[offset..]
            .chars()
            .next()
            .expect("valid character offset");
        if ch == '"' {
            quoted = !quoted;
            // Quotes flush a tokenizer part even without intervening space.
            operand_start = !quoted;
        } else if !quoted && (ch.is_whitespace() || ch == '|') {
            operand_start = true;
        } else if !quoted && operand_start {
            let start = offset + usize::from(ch == '-');
            for (relative, next) in input[start..].char_indices() {
                if next == ':' {
                    let end = start + relative;
                    if let Some(id) = resolve_label(&input[start..end], columns)? {
                        output.push_str(&input[copied..start]);
                        output.push_str(id);
                        copied = end;
                    }
                    break;
                }
                if next == '"' || next.is_whitespace() || next == '|' {
                    break;
                }
            }
            operand_start = false;
        }
        offset += ch.len_utf8();
    }
    output.push_str(&input[copied..]);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(id: &str, label: &str, source: &str) -> ColumnMeta {
        ColumnMeta {
            id: id.into(),
            label: label.into(),
            source: source.into(),
            order: 0,
        }
    }

    #[test]
    fn only_scopes_change_while_quotes_negation_and_or_carry_keep_their_syntax() {
        let columns = vec![
            column("c000001", "Cohort", "/cohort"),
            column("c000002", "message", "/message"),
            column("c000003", "column 3", ""),
        ];
        let input = " Cohort:keep|drop -MESSAGE:deny /cohort:\"keep value\" \"message:literal\" message:urn:value";
        assert_eq!(rewrite_scopes(input, &columns).unwrap(),
            " c000001:keep|drop -c000002:deny c000001:\"keep value\" \"message:literal\" c000002:urn:value");
        assert_eq!(
            rewrite_scopes("\"literal\"message:allow :needle", &columns).unwrap(),
            "\"literal\"c000002:allow :needle"
        );
    }

    #[test]
    fn explicit_ids_win_and_ambiguous_labels_fail_without_arbitrary_selection() {
        let columns = vec![
            column("c000001", "duplicate", "a"),
            column("c000002", "duplicate", "b"),
            column("c000003", "c000001", "c"),
        ];
        assert_eq!(
            rewrite_scopes("C000001:value", &columns).unwrap(),
            "c000001:value"
        );
        assert!(rewrite_scopes("duplicate:value", &columns)
            .unwrap_err()
            .to_string()
            .contains("Ambiguous search column: duplicate"));
        assert_eq!(
            rewrite_scopes("unknown:value \"duplicate:value\"", &columns).unwrap(),
            "unknown:value \"duplicate:value\""
        );
    }
}

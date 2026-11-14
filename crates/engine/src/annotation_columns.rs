//! CSV exports prepend this complete block before the unchanged source columns.
//! Restore uses the block's actual positions even when source-name collisions
//! cause export to suffix its headers.

pub(crate) const ANNOTATION_COLUMNS: [&str; 6] = [
    "trivium-safe",
    "trivium-suspicious",
    "trivium-critical",
    "trivium-memo",
    "trivium-tags-json",
    "trivium-bookmarked",
];

#[derive(Clone, Copy)]
pub(crate) enum TagFormat {
    Json,
    Legacy,
}

fn matches_header(actual: &str, canonical: &str) -> bool {
    actual == canonical
        || actual
            .strip_prefix(canonical)
            .and_then(|suffix| suffix.strip_prefix('_'))
            .filter(|suffix| {
                !suffix.starts_with('0') && suffix.bytes().all(|byte| byte.is_ascii_digit())
            })
            .and_then(|suffix| suffix.parse::<usize>().ok())
            .is_some_and(|index| index >= 2)
}

/// Recognize only a complete leading export block. Arbitrary suffixed source
/// columns elsewhere must not become annotations through name guessing.
pub(crate) fn leading_block(headers: &[&str]) -> Option<TagFormat> {
    if headers.len() != ANNOTATION_COLUMNS.len()
        || [0, 1, 2, 3, 5]
            .into_iter()
            .any(|index| !matches_header(headers[index], ANNOTATION_COLUMNS[index]))
    {
        return None;
    }
    if matches_header(headers[4], ANNOTATION_COLUMNS[4]) {
        Some(TagFormat::Json)
    } else if matches_header(headers[4], "trivium-tags") {
        Some(TagFormat::Legacy)
    } else {
        None
    }
}

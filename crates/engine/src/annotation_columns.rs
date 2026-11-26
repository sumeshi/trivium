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
pub(crate) fn leading_block(headers: &[&str]) -> Option<()> {
    if headers.len() != ANNOTATION_COLUMNS.len()
        || headers
            .iter()
            .zip(ANNOTATION_COLUMNS)
            .any(|(actual, canonical)| !matches_header(actual, canonical))
    {
        return None;
    }
    Some(())
}

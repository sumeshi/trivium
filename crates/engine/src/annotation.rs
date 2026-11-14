use serde::{Deserialize, Deserializer};

use crate::models::FlagEntry;

/// Partial update for one row's annotation.
///
/// A missing field keeps the stored value. An explicitly empty value clears it:
/// `Some("")` for the flag, `Some(None)` or a blank string for the memo,
/// `Some(vec![])` for tags, and `Some(false)` for the bookmark. Tags can also be
/// changed one-directionally with `add_tags` and `remove_tags`.
#[derive(Debug, Clone, Default, Deserialize, ts_rs::TS)]
#[ts(optional_fields = nullable)]
pub struct AnnotationPatch {
    /// `None` keeps the stored flag; `Some("")` clears it.
    #[serde(default)]
    pub flag: Option<String>,
    /// `None` keeps the stored memo; `Some(None)` or a blank string clears it.
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub memo: Option<Option<String>>,
    /// `None` keeps the stored tags; `Some(vec![])` clears them.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    /// Tags appended after any `tags` replacement. Existing tags are kept and
    /// duplicates are not added.
    #[serde(default)]
    pub add_tags: Vec<String>,
    /// Tags removed after any `tags` replacement and `add_tags` additions.
    #[serde(default)]
    pub remove_tags: Vec<String>,
    /// `None` keeps the stored bookmark state; `Some(false)` clears it.
    #[serde(default)]
    pub bookmarked: Option<bool>,
}

/// Distinguishes a missing field (`None`) from an explicit JSON `null`
/// (`Some(None)`). Serde applies this only when the key is present, so
/// `#[serde(default)]` supplies the missing case.
pub fn deserialize_double_option<'de, D>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

/// Applies `patch` to the stored entry and returns the entry to persist.
///
/// `None` means the annotation has no remaining content and the entry must be
/// deleted. Tags, bookmarks, and memos that the patch does not mention survive.
pub fn apply_annotation_patch(
    existing: Option<&FlagEntry>,
    patch: &AnnotationPatch,
) -> Option<FlagEntry> {
    let mut entry = existing.cloned().unwrap_or_else(|| FlagEntry {
        flag: String::new(),
        memo: None,
        tags: Vec::new(),
        bookmarked: false,
    });

    if let Some(flag) = &patch.flag {
        // The query/export paths normalize on read (`normalize_flag_value`),
        // matching what update_flag stored before this patch type existed.
        entry.flag = flag.clone();
    }
    if let Some(memo) = &patch.memo {
        entry.memo = memo
            .as_deref()
            .map(str::trim)
            .filter(|memo| !memo.is_empty())
            .map(str::to_string);
    }
    if let Some(tags) = &patch.tags {
        entry.tags = tags.clone();
    }
    for tag in &patch.add_tags {
        if !entry.tags.iter().any(|existing| existing == tag) {
            entry.tags.push(tag.clone());
        }
    }
    if !patch.remove_tags.is_empty() {
        entry.tags.retain(|tag| !patch.remove_tags.contains(tag));
    }
    if let Some(bookmarked) = patch.bookmarked {
        entry.bookmarked = bookmarked;
    }

    if entry_is_empty(&entry) {
        None
    } else {
        Some(entry)
    }
}

fn entry_is_empty(entry: &FlagEntry) -> bool {
    !entry.is_annotated()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(flag: &str, memo: Option<&str>, tags: &[&str], bookmarked: bool) -> FlagEntry {
        FlagEntry {
            flag: flag.to_string(),
            memo: memo.map(str::to_string),
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
            bookmarked,
        }
    }

    fn patch(flag: Option<&str>, memo: Option<Option<&str>>) -> AnnotationPatch {
        AnnotationPatch {
            flag: flag.map(str::to_string),
            memo: memo.map(|value| value.map(str::to_string)),
            ..AnnotationPatch::default()
        }
    }

    #[test]
    fn omitted_fields_keep_every_stored_value() {
        let existing = entry("suspicious", Some("note"), &["pc"], true);
        let next = apply_annotation_patch(Some(&existing), &AnnotationPatch::default())
            .expect("entry must survive a no-op patch");
        assert_eq!(next, existing);
    }

    #[test]
    fn empty_flag_keeps_the_other_annotations() {
        let existing = entry("critical", Some("note"), &["pc"], true);
        let next = apply_annotation_patch(Some(&existing), &patch(Some(""), None))
            .expect("memo, tags, and bookmark remain");
        assert_eq!(next.flag, "");
        assert_eq!(next.memo.as_deref(), Some("note"));
        assert_eq!(next.tags, vec!["pc".to_string()]);
        assert!(next.bookmarked);
    }

    #[test]
    fn memo_update_keeps_flag_tags_and_bookmark() {
        let existing = entry("critical", Some("old"), &["pc"], true);
        let next = apply_annotation_patch(Some(&existing), &patch(None, Some(Some(" new "))))
            .expect("entry must remain");
        assert_eq!(next.flag, "critical");
        assert_eq!(next.memo.as_deref(), Some("new"));
        assert_eq!(next.tags, vec!["pc".to_string()]);
        assert!(next.bookmarked);
    }

    #[test]
    fn clearing_the_memo_keeps_the_entry_when_other_content_remains() {
        let existing = entry("", Some("note"), &["pc"], false);
        let next =
            apply_annotation_patch(Some(&existing), &patch(None, Some(None))).expect("tags remain");
        assert_eq!(next.memo, None);
        assert_eq!(next.tags, vec!["pc".to_string()]);
    }

    #[test]
    fn entry_is_removed_only_when_every_field_is_empty() {
        let existing = entry("safe", Some("note"), &["pc"], true);
        let cleared = AnnotationPatch {
            flag: Some(String::new()),
            memo: Some(None),
            tags: Some(Vec::new()),
            bookmarked: Some(false),
            ..AnnotationPatch::default()
        };
        assert_eq!(apply_annotation_patch(Some(&existing), &cleared), None);
        assert_eq!(
            apply_annotation_patch(None, &AnnotationPatch::default()),
            None
        );
    }

    #[test]
    fn deserialization_distinguishes_missing_null_and_value() {
        let omitted: AnnotationPatch = serde_json::from_str(r#"{}"#).expect("empty patch");
        assert_eq!(omitted.flag, None);
        assert_eq!(omitted.memo, None);
        assert_eq!(omitted.tags, None);
        assert_eq!(omitted.bookmarked, None);

        let null_memo: AnnotationPatch =
            serde_json::from_str(r#"{"memo":null}"#).expect("null memo");
        assert_eq!(null_memo.memo, Some(None));

        let value: AnnotationPatch =
            serde_json::from_str(r#"{"flag":"","memo":"x","tags":[],"bookmarked":false}"#)
                .expect("explicit values");
        assert_eq!(value.flag, Some(String::new()));
        assert_eq!(value.memo, Some(Some("x".to_string())));
        assert_eq!(value.tags, Some(Vec::new()));
        assert_eq!(value.bookmarked, Some(false));
    }
}

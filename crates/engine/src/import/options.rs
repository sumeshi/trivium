use serde::{Deserialize, Serialize};

/// No encoding detection beyond an explicit Unicode byte-order mark.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
pub enum TextEncoding {
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "utf-8")]
    Utf8,
    #[serde(rename = "utf-16le")]
    Utf16Le,
    #[serde(rename = "utf-16be")]
    Utf16Be,
    #[serde(rename = "utf-32le")]
    Utf32Le,
    #[serde(rename = "utf-32be")]
    Utf32Be,
    #[serde(rename = "windows-1252")]
    Windows1252,
    #[serde(rename = "shift-jis")]
    ShiftJis,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(default, rename_all = "camelCase")]
pub struct ImportOptions {
    pub encoding: TextEncoding,
    pub has_headers: bool,
    pub recover: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            encoding: TextEncoding::Auto,
            has_headers: true,
            recover: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum ImportIssueAction {
    Skipped,
    Repaired,
    UnreadableTail,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ImportIssue {
    /// Physical CSV/JSONL line or 1-based JSON array element, when known.
    pub source_record: Option<usize>,
    pub action: ImportIssueAction,
    pub reason: String,
}

/// Persistent, bounded provenance for decoding and explicitly requested recovery.
/// Unknown records after an unreadable JSON tail are never counted as skipped.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, ts_rs::TS)]
#[serde(default, rename_all = "camelCase")]
pub struct ImportReport {
    pub encoding: String,
    pub bom: bool,
    pub has_headers: Option<bool>,
    pub source_records: usize,
    pub imported_records: usize,
    pub skipped_records: usize,
    pub repaired_records: usize,
    pub generated_columns: usize,
    pub unreadable_tail: bool,
    pub issue_count: usize,
    pub issues: Vec<ImportIssue>,
}

impl ImportReport {
    pub const MAX_ISSUES: usize = 30;

    pub fn issue(
        &mut self,
        source_record: Option<usize>,
        action: ImportIssueAction,
        reason: impl AsRef<str>,
    ) {
        self.issue_count = self.issue_count.saturating_add(1);
        if self.issues.len() < Self::MAX_ISSUES {
            self.issues.push(ImportIssue {
                source_record,
                action,
                reason: reason.as_ref().chars().take(320).collect(),
            });
        }
    }
}

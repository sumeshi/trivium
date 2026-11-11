use std::collections::{HashMap, HashSet};

use polars::prelude::{DataFrame, NamedFrom, Series};

use crate::{flags::normalize_flag_value, ioc::IocRowOutcome, models::FlagEntry};

/// Derived annotation columns in export order.
pub(crate) const ANNOTATION_COLUMNS: [&str; 6] = [
    "trivium-safe",
    "trivium-suspicious",
    "trivium-critical",
    "trivium-memo",
    "trivium-tags",
    "trivium-bookmarked",
];

pub(crate) struct PlannedColumn {
    internal: String,
    pub(crate) header: String,
}

/// Chooses unique internal series names and unique header labels for the
/// derived columns. A data column that already uses a `trivium-*` label wins;
/// the derived column is written as `trivium-*_2` (then `_3`, ...) and a
/// warning is returned so the UI can report the mapping.
pub(crate) fn plan_annotation_columns(
    data_headers: &[String],
    df: &DataFrame,
    warnings: &mut Vec<String>,
) -> Vec<PlannedColumn> {
    let mut used_headers: HashSet<String> = data_headers.iter().cloned().collect();
    let mut used_internals: HashSet<String> = df
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let mut planned = Vec::with_capacity(ANNOTATION_COLUMNS.len());

    for canonical in ANNOTATION_COLUMNS {
        let mut header = canonical.to_string();
        let mut counter = 2;
        while used_headers.contains(&header) {
            header = format!("{}_{}", canonical, counter);
            counter += 1;
        }
        used_headers.insert(header.clone());
        if header != canonical {
            warnings.push(format!(
                "Column '{}' conflicts with a data column; exported as '{}'.",
                canonical, header
            ));
        }

        let base = format!("__export_{}", canonical.replace('-', "_"));
        let mut internal = base.clone();
        let mut internal_counter = 2;
        while used_internals.contains(&internal) {
            internal = format!("{}_{}", base, internal_counter);
            internal_counter += 1;
        }
        used_internals.insert(internal.clone());
        planned.push(PlannedColumn { internal, header });
    }
    planned
}

pub(crate) struct AnnotationSeries {
    safe: Vec<i32>,
    suspicious: Vec<i32>,
    critical: Vec<i32>,
    memo: Vec<String>,
    tags: Vec<String>,
    bookmarked: Vec<i32>,
}

/// Builds the independent annotation columns. IOC tags are still appended to
/// the memo as `[tag]` tokens (the existing display rule); user tags are only
/// written to `trivium-tags` so they are never stored as memo text.
pub(crate) fn build_annotations(
    df: &DataFrame,
    flags: &HashMap<usize, FlagEntry>,
    ioc_outcomes: &[IocRowOutcome],
) -> AnnotationSeries {
    let mut safe = vec![0; df.height()];
    let mut suspicious = vec![0; df.height()];
    let mut critical = vec![0; df.height()];
    let mut memo = vec![String::new(); df.height()];
    let mut tags = vec![String::new(); df.height()];
    let mut bookmarked = vec![0; df.height()];

    for i in 0..df.height() {
        let outcome = ioc_outcomes.get(i).cloned().unwrap_or_default();
        let user_flag = flags
            .get(&i)
            .map(|entry| normalize_flag_value(entry.flag.trim()))
            .unwrap_or_default();
        let final_flag = if user_flag.is_empty() {
            outcome.flag
        } else {
            user_flag
        };
        let mut final_memo = flags
            .get(&i)
            .and_then(|entry| entry.memo.clone())
            .unwrap_or_default();
        for tag in outcome.tags {
            if !final_memo.contains(&tag) {
                if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                    final_memo.push(' ');
                }
                final_memo.push_str(&tag);
            }
        }

        match final_flag.as_str() {
            "safe" => safe[i] = 1,
            "suspicious" => suspicious[i] = 1,
            "critical" => critical[i] = 1,
            _ => {}
        }
        memo[i] = final_memo;

        if let Some(entry) = flags.get(&i) {
            tags[i] = entry.tags.join(";");
            bookmarked[i] = if entry.bookmarked { 1 } else { 0 };
        }
    }

    AnnotationSeries {
        safe,
        suspicious,
        critical,
        memo,
        tags,
        bookmarked,
    }
}

/// Renames the derived series to the planned internal names so they cannot
/// collide with a data column that uses the same internal name.
pub(crate) fn annotation_series(
    annotations: AnnotationSeries,
    planned: &[PlannedColumn],
) -> Vec<Series> {
    let series = [
        Series::new("safe", annotations.safe),
        Series::new("suspicious", annotations.suspicious),
        Series::new("critical", annotations.critical),
        Series::new("memo", annotations.memo),
        Series::new("tags", annotations.tags),
        Series::new("bookmarked", annotations.bookmarked),
    ];
    planned
        .iter()
        .zip(series)
        .map(|(planned, series)| series.with_name(&planned.internal))
        .collect()
}

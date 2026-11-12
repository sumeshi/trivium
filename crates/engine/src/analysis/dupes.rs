use std::collections::HashMap;

use polars::prelude::{AnyValue, Series};

use crate::error::AppError;
use crate::jobs::JobContext;
use crate::value_utils::anyvalue_to_search_string;

/// Row indexes whose key appears more than once (`duplicates`) or exactly once.
/// A key that appears three times contributes three indexes.
pub fn matching_row_indices(columns: &[&Series], duplicates: bool) -> Vec<usize> {
    matching_row_indices_with_job(columns, duplicates, &JobContext::none())
        .expect("an unregistered duplicate scan cannot be cancelled")
}

pub fn matching_row_indices_with_job(
    columns: &[&Series],
    duplicates: bool,
    job: &JobContext,
) -> Result<Vec<usize>, AppError> {
    let mut index = DuplicateIndex::default();
    index.push(columns, 0, job)?;
    index.finish(duplicates, job)
}

/// Exact compound-key groups; batches can be released immediately after push.
#[derive(Default)]
pub(crate) struct DuplicateIndex {
    groups: HashMap<String, Vec<usize>>,
}

impl DuplicateIndex {
    pub(crate) fn push(
        &mut self,
        columns: &[&Series],
        offset: usize,
        job: &JobContext,
    ) -> Result<(), AppError> {
        job.check_cancelled()?;
        let height = columns.first().map(|series| series.len()).unwrap_or(0);
        for idx in 0..height {
            if idx % 1024 == 0 {
                job.check_cancelled()?;
            }
            self.groups
                .entry(row_key(columns, idx))
                .or_default()
                .push(offset + idx);
        }
        Ok(())
    }

    pub(crate) fn finish(self, duplicates: bool, job: &JobContext) -> Result<Vec<usize>, AppError> {
        let mut matched = Vec::new();
        for (group, rows) in self.groups.into_values().enumerate() {
            if group % 1024 == 0 {
                job.check_cancelled()?;
            }
            if (rows.len() > 1) == duplicates {
                matched.extend(rows);
            }
        }
        job.check_cancelled()?;
        matched.sort_unstable();
        job.check_cancelled()?;
        Ok(matched)
    }
}

fn row_key(columns: &[&Series], idx: usize) -> String {
    let mut key = String::new();
    for series in columns {
        let text = cell_text(series, idx);
        key.push_str(&text.len().to_string());
        key.push(':');
        key.push_str(&text);
        key.push('\u{1e}');
    }
    key
}

fn cell_text(series: &Series, idx: usize) -> String {
    let Ok(value) = series.get(idx) else {
        return String::new();
    };
    if matches!(value, AnyValue::Null) {
        return String::new();
    }
    anyvalue_to_search_string(&value).unwrap_or_default()
}

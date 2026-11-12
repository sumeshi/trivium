use std::collections::HashMap;

use polars::prelude::{AnyValue, DataType, Series};
use serde::Serialize;

use crate::analysis::infer::{dtype_kind, ColumnKind, KindFlags};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::value_utils::anyvalue_to_search_string;

const TOP_VALUES: usize = 20;
/// The rare list is bounded too: the UI must never receive every unique value.
const RARE_VALUES: usize = 20;

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ts_rs::TS)]
pub struct ValueCount {
    pub value: String,
    #[ts(type = "number")]
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ts_rs::TS)]
pub struct ColumnStats {
    pub column: String,
    pub kind: String,
    #[ts(type = "number")]
    pub missing: u64,
    #[ts(type = "number")]
    pub unique: u64,
    pub top: Vec<ValueCount>,
    pub rare: Vec<ValueCount>,
}

/// One pass over `series`: missing count, distinct values, and the column kind.
pub fn summarize_series(column: &str, series: &Series, rare_max: u64) -> ColumnStats {
    let job = JobContext::none();
    let mut accumulator = StatsAccumulator::new(column, series.dtype());
    accumulator
        .observe(series, 0..series.len(), &job)
        .expect("uncancelled statistics");
    accumulator
        .finish(rare_max, &job)
        .expect("uncancelled statistics")
}

/// Exact statistics retain distinct values, not the source column or frame.
/// Top and rare ranking retain only their bounded output, instead of sorting
/// and allocating a second vector containing every distinct value.
pub(crate) struct StatsAccumulator {
    column: String,
    hinted: Option<ColumnKind>,
    flags: KindFlags,
    counts: HashMap<String, u64>,
    missing: u64,
}

impl StatsAccumulator {
    pub fn new(column: &str, dtype: &DataType) -> Self {
        Self {
            column: column.into(),
            hinted: dtype_kind(dtype),
            flags: KindFlags::default(),
            counts: HashMap::new(),
            missing: 0,
        }
    }

    pub fn observe(
        &mut self,
        series: &Series,
        rows: impl IntoIterator<Item = usize>,
        job: &JobContext,
    ) -> Result<(), AppError> {
        for (index, row) in rows.into_iter().enumerate() {
            if index % 1024 == 0 {
                job.check_cancelled()?;
            }
            let Ok(value) = series.get(row) else {
                self.missing += 1;
                continue;
            };
            let Some(text) = present_text(&value) else {
                self.missing += 1;
                continue;
            };
            if self.hinted.is_none() {
                self.flags.observe(&value);
            }
            *self.counts.entry(text).or_default() += 1;
        }
        job.check_cancelled()
    }

    pub fn finish(self, rare_max: u64, job: &JobContext) -> Result<ColumnStats, AppError> {
        let kind = self.hinted.unwrap_or_else(|| self.flags.finish());
        let unique = self.counts.len() as u64;
        let mut top = Vec::with_capacity(TOP_VALUES);
        let mut rare = Vec::with_capacity(RARE_VALUES);
        for (index, (value, count)) in self.counts.into_iter().enumerate() {
            if index % 1024 == 0 {
                job.check_cancelled()?;
            }
            let item = ValueCount { value, count };
            if count <= rare_max {
                if let Some(index) = insertion_index(&rare, &item, RARE_VALUES) {
                    rare.insert(index, item.clone());
                    rare.truncate(RARE_VALUES);
                }
            }
            if let Some(index) = insertion_index(&top, &item, TOP_VALUES) {
                top.insert(index, item);
                top.truncate(TOP_VALUES);
            }
        }
        job.check_cancelled()?;
        Ok(ColumnStats {
            column: self.column,
            kind: kind.as_str().into(),
            missing: self.missing,
            unique,
            top,
            rare,
        })
    }
}

fn present_text(value: &AnyValue) -> Option<String> {
    if matches!(value, AnyValue::Null) {
        return None;
    }
    anyvalue_to_search_string(value).filter(|text| !text.is_empty())
}

fn insertion_index(items: &[ValueCount], candidate: &ValueCount, limit: usize) -> Option<usize> {
    let index = items
        .binary_search_by(|item| {
            candidate
                .count
                .cmp(&item.count)
                .then_with(|| item.value.cmp(&candidate.value))
        })
        .unwrap_or_else(|index| index);
    (index < limit).then_some(index)
}

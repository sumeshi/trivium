use std::collections::HashMap;

use polars::prelude::{DataType, Series};
use serde::Serialize;

use crate::analysis::infer::{dtype_kind, ColumnKind, KindFlags};
use crate::error::AppError;
use crate::jobs::JobContext;
#[cfg(test)]
mod tests;
mod text;

const TOP_VALUES: usize = 20;
/// The rare list is bounded too: the UI must never receive every unique value.
const RARE_VALUES: usize = 20;
const MAX_DISTINCT_VALUES: usize = 100_000;
const MAX_AGGREGATION_BYTES: usize = 32 * 1024 * 1024;
// Conservative allowance for each separately allocated key/clone. Hash table
// slots and their control bytes are accounted independently below.
const ALLOCATION_OVERHEAD: usize = 32;

#[derive(Clone, Copy)]
pub(crate) struct StatsLimits {
    pub(crate) distinct: usize,
    pub(crate) bytes: usize,
}

impl Default for StatsLimits {
    fn default() -> Self {
        Self {
            distinct: MAX_DISTINCT_VALUES,
            bytes: MAX_AGGREGATION_BYTES,
        }
    }
}

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
pub fn summarize_series(
    column: &str,
    series: &Series,
    rare_max: u64,
) -> Result<ColumnStats, AppError> {
    let job = JobContext::none();
    let mut accumulator = StatsAccumulator::new(column, series.dtype());
    accumulator.observe(series, 0..series.len(), &job)?;
    accumulator.finish(rare_max, &job)
}

/// Exact statistics retain distinct values, not the source column or frame.
/// Top and rare ranking retain only their bounded output, instead of sorting
/// and allocating a second vector containing every distinct value.
pub(crate) struct StatsAccumulator {
    column: String,
    hinted: Option<ColumnKind>,
    flags: KindFlags,
    counts: HashMap<Box<str>, u64>,
    missing: u64,
    limits: StatsLimits,
    key_bytes: usize,
    failure: Option<String>,
}

impl StatsAccumulator {
    pub fn new(column: &str, dtype: &DataType) -> Self {
        Self::with_limits(column, dtype, StatsLimits::default())
    }

    pub(crate) fn with_limits(column: &str, dtype: &DataType, limits: StatsLimits) -> Self {
        Self {
            column: column.into(),
            hinted: dtype_kind(dtype),
            flags: KindFlags::default(),
            counts: HashMap::new(),
            missing: 0,
            limits,
            key_bytes: 0,
            failure: None,
        }
    }

    pub fn observe(
        &mut self,
        series: &Series,
        rows: impl IntoIterator<Item = usize>,
        job: &JobContext,
    ) -> Result<(), AppError> {
        if let Some(error) = &self.failure {
            return Err(AppError::Message(error.clone()));
        }
        let result = self.observe_rows(series, rows, job);
        if let Err(error) = &result {
            self.failure = Some(error.to_string());
            self.counts = HashMap::new();
            self.key_bytes = 0;
        }
        result
    }

    fn observe_rows(
        &mut self,
        series: &Series,
        rows: impl IntoIterator<Item = usize>,
        job: &JobContext,
    ) -> Result<(), AppError> {
        self.check_budget(self.key_bytes, table_bytes(self.counts.capacity()))?;
        for (index, row) in rows.into_iter().enumerate() {
            if index % 1024 == 0 {
                job.check_cancelled()?;
            }
            let Ok(value) = series.get(row) else {
                self.missing += 1;
                continue;
            };
            // Output clones do not exist during observation. Their reserved
            // space can format one legacy typed/list cell, including repeats
            // when steady-state accounting is exactly at the limit.
            let retained = self
                .fixed_bytes()
                .saturating_add(self.key_bytes)
                .saturating_add(table_bytes(self.counts.capacity()));
            let available = self
                .limits
                .bytes
                .saturating_sub(retained)
                .saturating_sub(ALLOCATION_OVERHEAD);
            let Some(text) =
                text::present_text(&value, available).map_err(|()| self.limit_error())?
            else {
                self.missing += 1;
                continue;
            };
            if let Some(count) = self.counts.get_mut(text.as_ref()) {
                *count += 1;
                continue;
            }
            if self.counts.len() >= self.limits.distinct {
                return Err(self.limit_error());
            }
            let key_bytes = self
                .key_bytes
                .saturating_add(text.len())
                .saturating_add(ALLOCATION_OVERHEAD);
            if self.counts.len() == self.counts.capacity() {
                // HashMap may grow geometrically. Budget both old and new
                // tables during rehash, before requesting the allocation.
                let next_capacity = self
                    .counts
                    .capacity()
                    .saturating_add(1)
                    .saturating_mul(2)
                    .max(3);
                self.check_budget(
                    key_bytes,
                    table_bytes(self.counts.capacity()).saturating_add(table_bytes(next_capacity)),
                )?;
                self.counts.try_reserve(1).map_err(|_| self.limit_error())?;
            }
            self.check_budget(key_bytes, table_bytes(self.counts.capacity()))?;
            if self.hinted.is_none() {
                self.flags.observe_text(text.as_ref());
            }
            self.counts.insert(text.into_owned().into_boxed_str(), 1);
            self.key_bytes = key_bytes;
        }
        job.check_cancelled()
    }

    pub fn finish(self, rare_max: u64, job: &JobContext) -> Result<ColumnStats, AppError> {
        if let Some(error) = &self.failure {
            return Err(AppError::Message(error.clone()));
        }
        self.check_budget(self.key_bytes, table_bytes(self.counts.capacity()))?;
        let kind = self.hinted.unwrap_or_else(|| self.flags.finish());
        let unique = self.counts.len() as u64;
        let mut top = Vec::with_capacity(TOP_VALUES);
        let mut rare = Vec::with_capacity(RARE_VALUES);
        for (index, (value, count)) in self.counts.into_iter().enumerate() {
            if index % 1024 == 0 {
                job.check_cancelled()?;
            }
            let item = ValueCount {
                value: value.into_string(),
                count,
            };
            if count <= rare_max {
                if let Some(index) = insertion_index(&rare, &item, RARE_VALUES) {
                    if rare.len() == RARE_VALUES {
                        rare.pop();
                    }
                    rare.insert(index, item.clone());
                }
            }
            if let Some(index) = insertion_index(&top, &item, TOP_VALUES) {
                if top.len() == TOP_VALUES {
                    top.pop();
                }
                top.insert(index, item);
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

    fn fixed_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.column.capacity()
            + ALLOCATION_OVERHEAD
            + (TOP_VALUES + RARE_VALUES) * std::mem::size_of::<ValueCount>()
            + 2 * ALLOCATION_OVERHEAD
    }

    #[cfg(test)]
    fn accounted_bytes(&self) -> usize {
        self.fixed_bytes()
            .saturating_add(self.key_bytes.saturating_mul(2))
            .saturating_add(table_bytes(self.counts.capacity()))
    }

    fn check_budget(&self, key_bytes: usize, table_bytes: usize) -> Result<(), AppError> {
        // Reserve a second copy of every key for temporary formatting and the
        // bounded rare/top output. This is an aggregation-allocation budget,
        // not a claim about the input batch or whole-process RSS.
        if self
            .fixed_bytes()
            .saturating_add(key_bytes.saturating_mul(2))
            .saturating_add(table_bytes)
            > self.limits.bytes
        {
            return Err(self.limit_error());
        }
        Ok(())
    }

    fn limit_error(&self) -> AppError {
        let bytes = if self.limits.bytes.is_multiple_of(1024 * 1024) {
            format!("{} MiB", self.limits.bytes / (1024 * 1024))
        } else {
            format!("{} bytes", self.limits.bytes)
        };
        AppError::Message(format!("Exact column statistics exceed the limit ({} distinct values / {} aggregation memory). Narrow the current filters and try again.", self.limits.distinct, bytes))
    }
}

fn table_bytes(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    // capacity is usable entries, not raw bucket count. At most two buckets
    // per usable entry, including each control byte and allocation padding.
    capacity
        .saturating_mul(2)
        .saturating_mul(std::mem::size_of::<(Box<str>, u64)>() + 1)
        .saturating_add(16 + ALLOCATION_OVERHEAD)
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

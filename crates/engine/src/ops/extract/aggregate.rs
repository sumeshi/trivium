use std::{collections::HashMap, mem::size_of};

use crate::{
    error::AppError,
    formats::{CompiledFormat, FormatOccurrence},
    jobs::JobContext,
};

pub(super) struct ExtractValue {
    pub value: String,
    pub count: u64,
    pub rows: u64,
    pub scope: Option<String>,
}

pub(super) const MAX_VALUE_BYTES: usize = 64 * 1024;
pub(super) const MAX_DISTINCT: usize = 100_000;
pub(super) const MAX_BYTES: usize = 32 * 1024 * 1024;
const ALLOCATION_BYTES: usize = 32;

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub distinct: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            distinct: MAX_DISTINCT,
            bytes: MAX_BYTES,
        }
    }
}

struct Count {
    occurrences: u64,
    rows: u64,
    last_row: usize,
    scope: Option<String>,
}

/// Rows arrive once in source order, with every selected column of one row
/// adjacent. A last-seen ID counts a row once without retaining hit/row sets.
pub(super) struct Accumulator {
    counts: HashMap<String, Count>,
    bytes: usize,
    limits: Limits,
}

pub(super) fn limit_error() -> AppError {
    AppError::Message("Extraction exceeds the exact aggregation limit. Narrow the rows or columns and run again; no complete result was produced.".into())
}

impl Accumulator {
    pub fn new(limits: Limits) -> Self {
        Self {
            counts: HashMap::new(),
            bytes: 0,
            limits,
        }
    }

    fn retained(&self) -> usize {
        self.bytes
            .saturating_add(table_bytes(self.counts.capacity()))
            // Reserve the final ranked vector while the map still exists.
            .saturating_add(self.counts.len() * size_of::<ExtractValue>())
    }

    pub fn finish(self, job: &JobContext) -> Result<Vec<ExtractValue>, AppError> {
        job.check_cancelled()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(self.counts.len())
            .map_err(|_| limit_error())?;
        for (index, (value, count)) in self.counts.into_iter().enumerate() {
            if index.is_multiple_of(1024) {
                job.check_cancelled()?;
            }
            values.push(ExtractValue {
                value,
                count: count.occurrences,
                rows: count.rows,
                scope: count.scope,
            });
        }
        // Sorting is bounded by the distinct limit. Stable ties use raw values.
        values.sort_unstable_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
        job.check_cancelled()?;
        Ok(values)
    }
    pub fn available(&self) -> usize {
        self.limits.bytes.saturating_sub(self.retained())
    }

    pub fn observe(
        &mut self,
        row: usize,
        found: FormatOccurrence<'_>,
        transient: usize,
        matcher: &CompiledFormat,
    ) -> Result<(), AppError> {
        let value = found.value;
        if value.len() > MAX_VALUE_BYTES {
            return Err(AppError::Message("An extracted value exceeds 64 KiB. Choose a narrower pattern; no complete result was produced.".into()));
        }
        if let Some(count) = self.counts.get_mut(value) {
            count.occurrences = count.occurrences.checked_add(1).ok_or_else(limit_error)?;
            if count.last_row != row {
                count.rows += 1;
                count.last_row = row;
            }
        } else {
            if self.counts.len() >= self.limits.distinct {
                return Err(limit_error());
            }
            let scope = matcher.classify(value);
            let added = value
                .len()
                .saturating_add(scope.map_or(0, str::len))
                .saturating_add(ALLOCATION_BYTES * (1 + usize::from(scope.is_some())));
            let next_capacity = if self.counts.len() == self.counts.capacity() {
                (self.counts.capacity().saturating_add(1) * 2).max(3)
            } else {
                self.counts.capacity()
            };
            let rehash = if next_capacity != self.counts.capacity() {
                table_bytes(self.counts.capacity())
            } else {
                0
            };
            let needed = self
                .bytes
                .saturating_add(added)
                .saturating_add(table_bytes(next_capacity))
                .saturating_add(rehash)
                .saturating_add((self.counts.len() + 1) * size_of::<ExtractValue>())
                .saturating_add(transient);
            if needed > self.limits.bytes {
                return Err(limit_error());
            }
            if next_capacity != self.counts.capacity() {
                self.counts.try_reserve(1).map_err(|_| limit_error())?;
            }
            self.bytes += added;
            self.counts.insert(
                value.into(),
                Count {
                    occurrences: 1,
                    rows: 1,
                    last_row: row,
                    scope: scope.map(str::to_owned),
                },
            );
            if self.retained().saturating_add(transient) > self.limits.bytes {
                return Err(limit_error());
            }
        }
        Ok(())
    }
}

fn table_bytes(capacity: usize) -> usize {
    // Conservative load factor, slot control bytes and allocation overhead.
    capacity
        .saturating_mul(2)
        .saturating_add(16)
        .saturating_mul(size_of::<(String, Count)>() + 1)
        .saturating_add(ALLOCATION_BYTES)
}

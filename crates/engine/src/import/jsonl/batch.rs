//! Allocations retained while constructing one dense JSON import batch.
use std::mem::{size_of, take};

use polars::prelude::*;

use crate::{
    error::AppError,
    models::ColumnMeta,
    project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD},
};

pub(super) struct Record {
    pub cells: Vec<(usize, Option<String>)>,
    pub original: String,
    pub row_id: i64,
    pub source_record: i64,
}

impl Record {
    fn text_bytes(&self) -> usize {
        self.cells
            .iter()
            .fold(self.original.capacity(), |bytes, (_, cell)| {
                bytes.saturating_add(cell.as_ref().map_or(0, String::capacity))
            })
    }
}

pub(super) struct Batch {
    cells: Vec<Vec<Option<String>>>,
    originals: Vec<String>,
    row_ids: Vec<i64>,
    source_records: Vec<i64>,
    text_bytes: usize,
}

fn next_capacity(len: usize, capacity: usize) -> usize {
    if len < capacity {
        capacity
    } else {
        capacity.saturating_mul(2).max(1)
    }
}

fn reserve_next<T>(values: &mut Vec<T>) -> Result<(), AppError> {
    if values.len() == values.capacity() {
        values
            .try_reserve_exact(next_capacity(values.len(), values.capacity()) - values.len())
            .map_err(|_| AppError::Message("Could not allocate a JSON import batch.".into()))?;
    }
    Ok(())
}

impl Batch {
    pub fn new(columns: usize) -> Self {
        Self {
            cells: vec![Vec::new(); columns],
            originals: Vec::new(),
            row_ids: Vec::new(),
            source_records: Vec::new(),
            text_bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.row_ids.len()
    }

    fn buffer_bytes(&self, grow: bool) -> usize {
        let capacity = |len, current| {
            if grow {
                next_capacity(len, current)
            } else {
                current
            }
        };
        let mut bytes = size_of::<Self>().saturating_add(
            self.cells
                .capacity()
                .saturating_mul(size_of::<Vec<Option<String>>>()),
        );
        for cells in &self.cells {
            bytes = bytes.saturating_add(
                capacity(cells.len(), cells.capacity()).saturating_mul(size_of::<Option<String>>()),
            );
        }
        for (len, reserved, item_size) in [
            (
                self.originals.len(),
                self.originals.capacity(),
                size_of::<String>(),
            ),
            (
                self.row_ids.len(),
                self.row_ids.capacity(),
                size_of::<i64>(),
            ),
            (
                self.source_records.len(),
                self.source_records.capacity(),
                size_of::<i64>(),
            ),
        ] {
            bytes = bytes.saturating_add(capacity(len, reserved).saturating_mul(item_size));
        }
        bytes.saturating_add(self.text_bytes)
    }

    /// Count missing-cell slots and reserved capacity before extending any
    /// vector, rather than discovering the over-allocation after a push.
    pub fn would_exceed(&self, record: &Record, budget: usize) -> bool {
        self.buffer_bytes(true).saturating_add(record.text_bytes()) > budget
    }

    pub fn allocated_bytes(&self) -> usize {
        self.buffer_bytes(false)
    }

    pub fn push(&mut self, record: Record) -> Result<(), AppError> {
        self.text_bytes = self.text_bytes.saturating_add(record.text_bytes());
        let position = self.len();
        for cells in &mut self.cells {
            reserve_next(cells)?;
            cells.push(None);
        }
        for (column, value) in record.cells {
            self.cells[column][position] = value;
        }
        reserve_next(&mut self.originals)?;
        reserve_next(&mut self.row_ids)?;
        reserve_next(&mut self.source_records)?;
        self.originals.push(record.original);
        self.row_ids.push(record.row_id);
        self.source_records.push(record.source_record);
        Ok(())
    }

    pub fn take_frame(&mut self, columns: &[ColumnMeta]) -> Result<DataFrame, AppError> {
        let mut series = Vec::with_capacity(columns.len() + 3);
        for (column, cells) in columns.iter().zip(&mut self.cells) {
            series.push(Series::new(column.id.as_str(), take(cells)));
        }
        series.push(Series::new(INTERNAL_ORIGINAL, take(&mut self.originals)));
        series.push(Series::new(INTERNAL_ROW_ID, take(&mut self.row_ids)));
        series.push(Series::new(
            INTERNAL_SOURCE_RECORD,
            take(&mut self.source_records),
        ));
        self.text_bytes = 0;
        DataFrame::new(series)
            .map_err(|_| AppError::Message("Failed to build the JSON dataset.".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(row: i64) -> Record {
        let mut text = String::with_capacity(512);
        text.push('x');
        Record {
            cells: vec![(0, Some(text))],
            original: "{}".into(),
            row_id: row,
            source_record: row + 1,
        }
    }

    #[test]
    fn budget_counts_missing_slots_and_string_capacity_before_growth() {
        let mut batch = Batch::new(800);
        let budget = 512 * 1024;
        let mut row = 0;
        while !batch.would_exceed(&record(row), budget) {
            batch.push(record(row)).unwrap();
            assert!(batch.allocated_bytes() <= budget);
            row += 1;
        }
        assert!(row > 1 && row < 32);
        assert_eq!(batch.cells[799].len(), row as usize);
        assert!(batch.cells[799].iter().all(Option::is_none));
        let actual = size_of::<Batch>()
            + batch.cells.capacity() * size_of::<Vec<Option<String>>>()
            + batch
                .cells
                .iter()
                .map(|cells| {
                    cells.capacity() * size_of::<Option<String>>()
                        + cells.iter().flatten().map(String::capacity).sum::<usize>()
                })
                .sum::<usize>()
            + batch.originals.capacity() * size_of::<String>()
            + batch.originals.iter().map(String::capacity).sum::<usize>()
            + (batch.row_ids.capacity() + batch.source_records.capacity()) * size_of::<i64>();
        assert_eq!(batch.allocated_bytes(), actual);
    }
}

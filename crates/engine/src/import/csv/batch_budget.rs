//! Count reserved cell slots as well as strings before a CSV batch grows.
use std::mem::size_of;

use crate::error::AppError;

fn capacity<T>(values: &Vec<T>, grow: bool) -> usize {
    if grow && values.len() == values.capacity() {
        values.capacity().saturating_mul(2).max(1)
    } else {
        values.capacity()
    }
}

fn bytes<T>(values: &Vec<T>, grow: bool) -> usize {
    capacity(values, grow).saturating_mul(size_of::<T>())
}

pub(super) fn retained_bytes(
    cells: &Vec<Vec<Option<String>>>,
    originals: &Vec<String>,
    row_ids: &Vec<i64>,
    source_records: &Vec<i64>,
    text_bytes: usize,
    grow: bool,
) -> usize {
    cells
        .iter()
        .fold(bytes(cells, false), |total, column| {
            total.saturating_add(bytes(column, grow))
        })
        .saturating_add(bytes(originals, grow))
        .saturating_add(bytes(row_ids, grow))
        .saturating_add(bytes(source_records, grow))
        .saturating_add(text_bytes)
}

pub(super) fn reserve_next<T>(values: &mut Vec<T>) -> Result<(), AppError> {
    if values.len() == values.capacity() {
        values
            .try_reserve_exact(capacity(values, true) - values.len())
            .map_err(|_| AppError::Message("Could not allocate a CSV import batch.".into()))?;
    }
    Ok(())
}

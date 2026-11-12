use std::{path::Path, sync::atomic::Ordering};

use anyhow::{anyhow, Result};
use polars::prelude::{DataFrame, NewChunkedArray, UInt32Chunked};

use crate::jobs::JobContext;

use super::{
    last_page_projection_store, record_projection, ProjectBatchReader, PROJECT_PAGE_READS,
};

/// Read only intersecting row groups and projected columns, not the file prefix.
pub fn read_page_dataframe(
    path: &Path,
    columns: &[String],
    offset: usize,
    limit: usize,
) -> Result<DataFrame> {
    read_page_dataframe_with_job(path, columns, offset, limit, &JobContext::none())
}

pub fn read_page_dataframe_with_job(
    path: &Path,
    columns: &[String],
    offset: usize,
    limit: usize,
    job: &JobContext,
) -> Result<DataFrame> {
    job.check_cancelled()?;
    if limit == 0 {
        return Ok(DataFrame::empty());
    }
    PROJECT_PAGE_READS.fetch_add(1, Ordering::Relaxed);
    record_projection(last_page_projection_store(), columns);
    let end = offset.saturating_add(limit);
    let mut reader = ProjectBatchReader::for_range(path, columns, offset, end)?;
    let mut output = reader.empty_frame()?;
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch?;
        let start = offset
            .saturating_sub(batch.offset)
            .min(batch.frame.height());
        let stop = end.saturating_sub(batch.offset).min(batch.frame.height());
        if start < stop {
            let indices =
                UInt32Chunked::from_iter_values("take", (start..stop).map(|row| row as u32));
            output.vstack_mut(&batch.frame.take(&indices)?)?;
        }
        if batch.offset.saturating_add(batch.frame.height()) >= end {
            break;
        }
    }
    job.check_cancelled()?;
    Ok(output)
}

/// Gather original IDs in caller order, preserving duplicates, while decoding
/// bounded batches only from groups containing at least one requested ID.
pub fn read_page_rows_dataframe(
    path: &Path,
    columns: &[String],
    rows: &[usize],
) -> Result<DataFrame> {
    read_page_rows_dataframe_with_job(path, columns, rows, &JobContext::none())
}

pub fn read_page_rows_dataframe_with_job(
    path: &Path,
    columns: &[String],
    rows: &[usize],
    job: &JobContext,
) -> Result<DataFrame> {
    job.check_cancelled()?;
    if rows.is_empty() {
        return Ok(DataFrame::empty());
    }
    PROJECT_PAGE_READS.fetch_add(1, Ordering::Relaxed);
    record_projection(last_page_projection_store(), columns);
    let mut reader = ProjectBatchReader::for_rows(path, columns, rows)?;
    let mut output = reader.empty_frame()?;
    if rows.iter().any(|row| *row >= reader.total_rows()) {
        return Err(anyhow!("Requested row is outside the Parquet file"));
    }
    let mut sorted = rows.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut next = 0;
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch?;
        let end = batch.offset + batch.frame.height();
        let last = next + sorted[next..].partition_point(|row| *row < end);
        if last > next {
            let indices = UInt32Chunked::from_iter_values(
                "take",
                sorted[next..last]
                    .iter()
                    .map(|row| (row - batch.offset) as u32),
            );
            output.vstack_mut(&batch.frame.take(&indices)?)?;
            next = last;
        }
        if next == sorted.len() {
            break;
        }
        if sorted[next] >= reader.current_row_group_end() {
            reader.skip_current_row_group();
        }
    }
    let order = UInt32Chunked::from_iter_values(
        "take",
        rows.iter().map(|row| {
            sorted
                .binary_search(row)
                .expect("requested ID was collected") as u32
        }),
    );
    job.check_cancelled()?;
    Ok(output.take(&order)?)
}

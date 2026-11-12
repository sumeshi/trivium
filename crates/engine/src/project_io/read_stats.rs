use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

static IO_BYTES: AtomicU64 = AtomicU64::new(0);
static DECODED_GROUPS: AtomicU64 = AtomicU64::new(0);
static DECODED_ROWS: AtomicU64 = AtomicU64::new(0);
static COLUMN_BYTES: AtomicU64 = AtomicU64::new(0);
static MAX_BATCH_ROWS: AtomicU64 = AtomicU64::new(0);

/// Actual work of the bounded Parquet reader, not just API call counts.
///
/// `io_bytes` includes footer reads. `column_bytes` is the compressed size of
/// projected column chunks loaded from the selected groups. The maximum is
/// one decoded output batch; callers may deliberately retain several batches.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ProjectReadStats {
    pub io_bytes: u64,
    pub column_bytes: u64,
    pub decoded_row_groups: u64,
    pub decoded_rows: u64,
    pub max_batch_rows: u64,
}

pub fn project_read_stats() -> ProjectReadStats {
    ProjectReadStats {
        io_bytes: IO_BYTES.load(Ordering::Relaxed),
        column_bytes: COLUMN_BYTES.load(Ordering::Relaxed),
        decoded_row_groups: DECODED_GROUPS.load(Ordering::Relaxed),
        decoded_rows: DECODED_ROWS.load(Ordering::Relaxed),
        max_batch_rows: MAX_BATCH_ROWS.load(Ordering::Relaxed),
    }
}

pub fn reset_project_read_stats() {
    for counter in [
        &IO_BYTES,
        &COLUMN_BYTES,
        &DECODED_GROUPS,
        &DECODED_ROWS,
        &MAX_BATCH_ROWS,
    ] {
        counter.store(0, Ordering::Relaxed);
    }
}

pub(super) fn record_io(bytes: usize) {
    IO_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub(super) fn record_group(bytes: u64) {
    DECODED_GROUPS.fetch_add(1, Ordering::Relaxed);
    COLUMN_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

pub(super) fn record_batch(rows: usize) {
    DECODED_ROWS.fetch_add(rows as u64, Ordering::Relaxed);
    MAX_BATCH_ROWS.fetch_max(rows as u64, Ordering::Relaxed);
}

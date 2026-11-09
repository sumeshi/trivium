pub mod dupes;
pub mod histogram;
pub mod infer;
pub mod stats;

pub use dupes::matching_row_indices;
pub use histogram::{histogram_series, Histogram, HistogramBucket, DEFAULT_BUCKETS};
pub use infer::{infer_column_kinds, ColumnKind};
pub use stats::{summarize_series, ColumnStats, ValueCount};

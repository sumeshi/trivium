pub mod dupes;
pub mod group;
pub mod histogram;
pub mod infer;
pub mod stats;
pub mod time;

pub use dupes::matching_row_indices;
pub use histogram::{histogram_series, Histogram, HistogramBucket, DEFAULT_BUCKETS};
pub use infer::{infer_column_kinds, ColumnKind};
pub use stats::{summarize_series, ColumnStats, ValueCount};
pub use time::{analyze_series, TimeAnalysis, TimeSpan};

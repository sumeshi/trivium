//! Page I/O must scale with touched row groups, not with original row offsets.

use std::{
    fs::{self, File},
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

use polars::prelude::*;
use uuid::Uuid;

use trivium_engine::{
    jobs::{request_cancel, try_begin_job, JobContext, JobKind},
    project_io::{
        project_read_stats, read_page_dataframe, read_page_dataframe_with_job,
        read_page_rows_dataframe, read_project_columns_with_job, reset_project_read_counters,
        ProjectBatchReader, PROJECT_BATCH_ROWS,
    },
};

static LOCK: Mutex<()> = Mutex::new(());

fn guard() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Fixture {
    root: PathBuf,
    path: PathBuf,
}

impl Fixture {
    fn new(rows: usize, group_size: usize) -> Self {
        let root = std::env::temp_dir().join(format!("trivium-parquet-access-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("data.parquet");
        let nanos: Vec<i64> = (0..rows)
            .map(|row| 1_700_000_000_000_000_000 + row as i64)
            .collect();
        let frame = DataFrame::new(vec![
            Series::new("seq", (0..rows as u64).collect::<Vec<_>>()),
            Series::new("text", (0..rows).map(text).collect::<Vec<_>>()),
            Series::new(
                "maybe",
                (0..rows)
                    .map(|row| (row % 3 != 0).then_some(if row % 3 == 1 { "" } else { "value" }))
                    .collect::<Vec<_>>(),
            ),
            Series::new(
                "uint",
                (0..rows)
                    .map(|row| u64::MAX - row as u64)
                    .collect::<Vec<_>>(),
            ),
            Series::new("nanos", nanos)
                .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
                .unwrap(),
            Series::new(
                "original",
                (0..rows)
                    .map(|row| {
                        format!(r#"{{"large":18446744073709551615,"row":{row},"explicit":null}}"#)
                    })
                    .collect::<Vec<_>>(),
            ),
        ])
        .unwrap();
        let mut writer = ParquetWriter::new(File::create(&path).unwrap())
            .with_statistics(false)
            .batched(&frame.schema())
            .unwrap();
        for start in (0..rows).step_by(group_size) {
            writer
                .write_batch(&frame.slice(start as i64, group_size.min(rows - start)))
                .unwrap();
        }
        writer.finish().unwrap();
        Self { root, path }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn text(row: usize) -> String {
    format!("source-{row:08}-{}", "exact-value-".repeat(20))
}

fn columns() -> Vec<String> {
    ["text", "seq", "maybe", "uint", "nanos", "original"]
        .map(String::from)
        .to_vec()
}

fn assert_rows(frame: &DataFrame, expected: &[usize]) {
    assert_eq!(frame.height(), expected.len());
    assert_eq!(
        frame.get_column_names(),
        ["text", "seq", "maybe", "uint", "nanos", "original"]
    );
    for (index, source) in expected.iter().copied().enumerate() {
        assert_eq!(
            frame.column("text").unwrap().str().unwrap().get(index),
            Some(text(source).as_str())
        );
        assert_eq!(
            frame.column("seq").unwrap().u64().unwrap().get(index),
            Some(source as u64)
        );
        let expected = match source % 3 {
            0 => None,
            1 => Some(""),
            _ => Some("value"),
        };
        assert_eq!(
            frame.column("maybe").unwrap().str().unwrap().get(index),
            expected
        );
        assert_eq!(
            frame.column("uint").unwrap().u64().unwrap().get(index),
            Some(u64::MAX - source as u64)
        );
        assert_eq!(
            frame.column("nanos").unwrap().get(index).unwrap(),
            AnyValue::Datetime(
                1_700_000_000_000_000_000 + source as i64,
                TimeUnit::Nanoseconds,
                &None
            )
        );
        assert_eq!(
            frame.column("original").unwrap().str().unwrap().get(index),
            Some(
                format!(r#"{{"large":18446744073709551615,"row":{source},"explicit":null}}"#)
                    .as_str()
            )
        );
    }
}

#[test]
fn source_start_middle_last_read_only_intersected_groups_without_row_id_or_statistics() {
    let _lock = guard();
    let fixture = Fixture::new(5199, 128);
    for offset in [0, 2600, 5189] {
        reset_project_read_counters();
        let page = read_page_dataframe(&fixture.path, &columns(), offset, 10).unwrap();
        assert_rows(&page, &(offset..offset + 10).collect::<Vec<_>>());
        let stats = project_read_stats();
        assert_eq!(stats.decoded_row_groups, 1, "{stats:?}");
        assert!(stats.decoded_rows <= 128, "{stats:?}");
        assert!(
            stats.column_bytes > 0
                && stats.io_bytes >= stats.column_bytes
                && stats.column_bytes < fs::metadata(&fixture.path).unwrap().len() / 4,
            "{stats:?}"
        );
        assert!(stats.max_batch_rows <= PROJECT_BATCH_ROWS as u64);
    }
}

#[test]
fn crossing_group_edges_and_sparse_repeated_unsorted_ids_preserve_source_values() {
    let _lock = guard();
    let fixture = Fixture::new(5199, 128);
    reset_project_read_counters();
    let page = read_page_dataframe(&fixture.path, &columns(), 124, 10).unwrap();
    assert_rows(&page, &(124..134).collect::<Vec<_>>());
    assert_eq!(project_read_stats().decoded_row_groups, 2);

    reset_project_read_counters();
    let expected = [5198, 3, 2500, 3, 5198];
    let sparse = read_page_rows_dataframe(&fixture.path, &columns(), &expected).unwrap();
    assert_rows(&sparse, &expected);
    let stats = project_read_stats();
    assert_eq!(stats.decoded_row_groups, 3, "{stats:?}");
    assert!(
        stats.decoded_rows <= 384,
        "sparse gather decoded skipped groups: {stats:?}"
    );
    assert!(
        sparse.estimated_size() < 16_384,
        "page retains unrelated source string buffers"
    );
}

#[test]
fn projection_reads_only_requested_column_chunks_and_empty_requests_do_no_io() {
    let _lock = guard();
    let fixture = Fixture::new(5199, 128);
    reset_project_read_counters();
    let projected = read_page_dataframe(&fixture.path, &["uint".into()], 5100, 5).unwrap();
    assert_eq!(projected.get_column_names(), ["uint"]);
    assert_eq!(
        projected.column("uint").unwrap().u64().unwrap().get(0),
        Some(u64::MAX - 5100)
    );
    let projected_bytes = project_read_stats().column_bytes;
    reset_project_read_counters();
    read_page_dataframe(&fixture.path, &columns(), 5100, 5).unwrap();
    assert!(project_read_stats().column_bytes > projected_bytes);

    reset_project_read_counters();
    assert!(read_page_dataframe(&fixture.path, &columns(), 0, 0)
        .unwrap()
        .is_empty());
    assert!(read_page_rows_dataframe(&fixture.path, &columns(), &[])
        .unwrap()
        .is_empty());
    assert_eq!(project_read_stats().io_bytes, 0);
    let beyond = read_page_dataframe(&fixture.path, &columns(), 8000, 5).unwrap();
    assert_eq!(beyond.width(), columns().len());
    assert_eq!(beyond.height(), 0);
    assert_eq!(project_read_stats().decoded_row_groups, 0);
    assert!(read_page_rows_dataframe(&fixture.path, &columns(), &[5199]).is_err());
}

#[test]
fn large_row_groups_stream_bounded_chunks_with_original_global_offsets() {
    let _lock = guard();
    let fixture = Fixture::new(25_000, 12_500);
    reset_project_read_counters();
    let reader = ProjectBatchReader::open(&fixture.path, &["seq".into()]).unwrap();
    let mut expected_offset = 0;
    for batch in reader {
        let batch = batch.unwrap();
        assert_eq!(batch.offset, expected_offset);
        assert!(batch.frame.height() <= PROJECT_BATCH_ROWS);
        assert_eq!(
            batch.frame.column("seq").unwrap().u64().unwrap().get(0),
            Some(batch.offset as u64)
        );
        expected_offset += batch.frame.height();
    }
    assert_eq!(expected_offset, 25_000);
    let stats = project_read_stats();
    assert_eq!(stats.decoded_row_groups, 2);
    assert_eq!(stats.decoded_rows, 25_000);
    assert_eq!(stats.max_batch_rows, PROJECT_BATCH_ROWS as u64);
}

#[test]
fn cancellation_before_or_between_batches_stops_further_parquet_io() {
    let _lock = guard();
    let fixture = Fixture::new(25_000, 12_500);
    let guard = try_begin_job(JobKind::Query, None).unwrap();
    let job = JobContext::with_job(guard, None);
    let mut reader = ProjectBatchReader::open(&fixture.path, &["seq".into()]).unwrap();
    reset_project_read_counters();
    assert_eq!(
        reader.next_with_job(&job).unwrap().unwrap().frame.height(),
        PROJECT_BATCH_ROWS
    );
    let before = project_read_stats();
    request_cancel(job.id().unwrap());
    assert!(reader
        .next_with_job(&job)
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("Cancelled"));
    assert_eq!(project_read_stats(), before);
    assert!(reader.next_with_job(&job).is_none());

    reset_project_read_counters();
    assert!(read_page_dataframe_with_job(&fixture.path, &columns(), 0, 10, &job).is_err());
    assert_eq!(project_read_stats().io_bytes, 0);
    assert!(read_project_columns_with_job(&fixture.path, &columns(), &job).is_err());
    assert_eq!(project_read_stats().io_bytes, 0);
}

#[test]
fn two_million_row_first_middle_last_pages_have_constant_group_io() {
    let _lock = guard();
    let rows = 2_000_000usize;
    let group_rows = 50_000usize;
    let root = std::env::temp_dir().join(format!("trivium-parquet-large-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let fixture = Fixture {
        path: root.join("data.parquet"),
        root,
    };
    let frame = DataFrame::new(vec![Series::new(
        "exact",
        (0..rows)
            .map(|row| u64::MAX - row as u64)
            .collect::<Vec<_>>(),
    )])
    .unwrap();
    let mut writer = ParquetWriter::new(File::create(&fixture.path).unwrap())
        .with_statistics(false)
        .batched(&frame.schema())
        .unwrap();
    for start in (0..rows).step_by(group_rows) {
        writer
            .write_batch(&frame.slice(start as i64, group_rows))
            .unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    drop(frame);
    let file_bytes = fs::metadata(&fixture.path).unwrap().len();
    for offset in [0, 1_000_000, 1_999_750] {
        reset_project_read_counters();
        let page = read_page_dataframe(&fixture.path, &["exact".into()], offset, 250).unwrap();
        assert_eq!(page.height(), 250);
        for (index, value) in page
            .column("exact")
            .unwrap()
            .u64()
            .unwrap()
            .into_iter()
            .enumerate()
        {
            assert_eq!(value, Some(u64::MAX - (offset + index) as u64));
        }
        let stats = project_read_stats();
        assert_eq!(stats.decoded_row_groups, 1, "{stats:?}");
        assert!(stats.decoded_rows <= group_rows as u64, "{stats:?}");
        assert!(
            stats.max_batch_rows <= PROJECT_BATCH_ROWS as u64,
            "{stats:?}"
        );
        assert!(stats.column_bytes < file_bytes / 10, "{stats:?}");
        assert!(stats.io_bytes < file_bytes / 4, "{stats:?}");
    }
}

#[test]
fn range_limit_is_relative_to_first_selected_group_and_stops_at_requested_end() {
    let _lock = guard();
    let fixture = Fixture::new(40_000, 12_000);
    for (offset, limit, expected_decoded) in [(0, 10, 10), (24_030, 7, 37), (23_995, 10, 12_005)] {
        reset_project_read_counters();
        let page = read_page_dataframe(&fixture.path, &columns(), offset, limit).unwrap();
        assert_rows(&page, &(offset..offset + limit).collect::<Vec<_>>());
        let stats = project_read_stats();
        assert_eq!(stats.decoded_rows, expected_decoded, "{stats:?}");
        assert!(stats.max_batch_rows <= PROJECT_BATCH_ROWS as u64);
    }
    reset_project_read_counters();
    let expected = [39_999, 3, 24_001, 3];
    let sparse = read_page_rows_dataframe(&fixture.path, &columns(), &expected).unwrap();
    assert_rows(&sparse, &expected);
    assert_eq!(project_read_stats().decoded_row_groups, 3);
}

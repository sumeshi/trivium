//! T16: CSV import parses bounded batches instead of holding the whole file.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::prelude::{DataFrame, NamedFrom, ParquetReader, SerReader, Series};
use uuid::Uuid;

use trivium_engine::import::csv::{BatchLimits, CsvImporter};
use trivium_engine::jobs::{
    request_cancel, try_begin_job, JobContext, JobKind, JobProgress, ProgressFn,
};
use trivium_engine::models::ColumnMeta;
use trivium_engine::ops::projects::{
    create_project_with_job, load_project, CreateProjectPayload, ProjectRequest,
};
use trivium_engine::storage::{
    compute_column_max_chars, load_column_metrics, COLUMN_WIDTH_SAMPLE_ROWS,
};
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-import-stream-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn payload(path: &Path) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().to_string(),
        description: None,
        delimiter: None,
        restore_annotations: None,
    }
}

fn import_with_limits(path: &Path, limits: BatchLimits) -> (Vec<ColumnMeta>, Vec<Vec<String>>) {
    let job = JobContext::none();
    let mut importer = CsvImporter::open(path, None, limits).expect("open CSV importer");
    let columns = importer.columns().to_vec();
    let mut rows = Vec::new();
    while let Some(batch) = importer.next_batch(&job).expect("read batch") {
        let height = batch.height();
        for row in 0..height {
            let mut values = Vec::with_capacity(columns.len());
            for column in &columns {
                let series = batch
                    .column(&column.id)
                    .expect("data column")
                    .str()
                    .expect("utf8 data column");
                values.push(series.get(row).expect("cell value").to_string());
            }
            rows.push(values);
        }
    }
    (columns, rows)
}

fn utf16_bytes(text: &str, big_endian: bool) -> Vec<u8> {
    let mut out = if big_endian {
        vec![0xFE, 0xFF]
    } else {
        vec![0xFF, 0xFE]
    };
    for unit in text.encode_utf16() {
        let bytes = if big_endian {
            unit.to_be_bytes()
        } else {
            unit.to_le_bytes()
        };
        out.extend_from_slice(&bytes);
    }
    out
}

#[test]
fn quoted_newlines_commas_and_quotes_survive_tiny_buffers() {
    let root = temp_root();
    let path = root.join("quoted.csv");
    let body = concat!(
        "name,note,value\n",
        "\"alice\",\"line1\nline2\",1\n",
        "\"bob\",\"has \"\"quotes\"\", and, commas\",2\n",
        "\"carol\",\"plain\",3\n",
    );
    fs::write(&path, body).expect("write fixture");

    // One row and one estimated byte per batch, with a 4-byte read buffer, so
    // every parser buffer boundary is exercised.
    let limits = BatchLimits {
        max_rows: 1,
        max_bytes: 1,
        read_buffer: 4,
    };
    let (columns, rows) = import_with_limits(&path, limits);

    assert_eq!(columns.len(), 3);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["alice", "line1\nline2", "1"]);
    assert_eq!(rows[1], vec!["bob", "has \"quotes\", and, commas", "2"]);
    assert_eq!(rows[2], vec!["carol", "plain", "3"]);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn utf16_le_and_be_match_ascii_across_tiny_buffers() {
    let root = temp_root();
    let body = concat!(
        "name,note,value\n",
        "\"alice\",\"line1\nline2\",1\n",
        "\"日本\",\"emoji 😀 and \"\"quotes\"\"\",2\n",
        "\"carol\",\"surrogate pair 😀😀\",3\n",
    );
    let ascii_path = root.join("ascii.csv");
    let le_path = root.join("utf16le.csv");
    let be_path = root.join("utf16be.csv");
    fs::write(&ascii_path, body).expect("write ascii fixture");
    fs::write(&le_path, utf16_bytes(body, false)).expect("write utf16le fixture");
    fs::write(&be_path, utf16_bytes(body, true)).expect("write utf16be fixture");

    // 3-byte reads split UTF-16 code units and surrogate pairs across reads.
    let limits = BatchLimits {
        max_rows: 2,
        max_bytes: 8,
        read_buffer: 3,
    };
    let (ascii_columns, ascii_rows) = import_with_limits(&ascii_path, limits);
    let (le_columns, le_rows) = import_with_limits(&le_path, limits);
    let (be_columns, be_rows) = import_with_limits(&be_path, limits);

    let labels = |columns: &[ColumnMeta]| {
        columns
            .iter()
            .map(|column| column.source.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(labels(&ascii_columns), labels(&le_columns));
    assert_eq!(labels(&ascii_columns), labels(&be_columns));
    assert_eq!(ascii_rows, le_rows);
    assert_eq!(ascii_rows, be_rows);
    assert_eq!(ascii_rows.len(), 3);
    assert_eq!(ascii_rows[1][1], "emoji 😀 and \"quotes\"");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn hundred_thousand_row_csv_imports_through_staged_batches() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("create store");
    let path = root.join("large.csv");
    let mut body = String::from("id,msg\n");
    for index in 0..100_000 {
        writeln!(body, "{},row {}", index, index).expect("format row");
    }
    fs::write(&path, body).expect("write large fixture");

    let summary = create_project_with_job(&store, payload(&path), &JobContext::none())
        .expect("large CSV import");
    assert_eq!(summary.meta.total_records, 100_000);

    let loaded = load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .expect("load imported project");
    assert_eq!(loaded.project.meta.total_records, 100_000);

    // 100,000 rows with the default 50,000-row batch limit are written as two
    // row groups; this is the batched writer, not one final frame write.
    let parquet = store.project_dir(&summary.meta.id).join("data.parquet");
    let mut reader = ParquetReader::new(fs::File::open(parquet).expect("open parquet"));
    let metadata = reader.get_metadata().expect("parquet metadata");
    assert!(
        metadata.row_groups.len() >= 2,
        "expected batched row groups, got {}",
        metadata.row_groups.len()
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn cancellation_between_batches_returns_cancelled() {
    let root = temp_root();
    let path = root.join("rows.csv");
    fs::write(&path, "id,msg\n1,a\n2,b\n3,c\n").expect("write fixture");

    let guard = try_begin_job(JobKind::Import, None).expect("begin job");
    let id = guard.id();
    let emit: ProgressFn = Arc::new(move |progress: JobProgress| {
        if progress.message.as_deref() == Some("Reading CSV") {
            request_cancel(id);
        }
    });
    let job = JobContext::with_job(guard, Some(emit));
    let mut importer = CsvImporter::open(
        &path,
        None,
        BatchLimits {
            max_rows: 1,
            max_bytes: usize::MAX,
            read_buffer: 8,
        },
    )
    .expect("open importer");

    let first = importer
        .next_batch(&job)
        .expect("first batch")
        .expect("one row");
    assert_eq!(first.height(), 1);

    let error = importer
        .next_batch(&job)
        .expect_err("cancel must be seen at the next batch boundary");
    assert_eq!(error.to_string(), "Cancelled.");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn cancelled_import_leaves_no_staging_or_project() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("create store");
    let path = root.join("cancel.csv");
    let mut body = String::from("id,msg\n");
    for index in 0..10 {
        writeln!(body, "{},row {}", index, index).expect("format row");
    }
    fs::write(&path, body).expect("write fixture");

    let guard = try_begin_job(JobKind::Import, None).expect("begin job");
    let id = guard.id();
    let emit: ProgressFn = Arc::new(move |_progress: JobProgress| {
        // The first progress report is the copy stage. Cancelling there makes
        // create_project_with_job fail through the staging guard.
        request_cancel(id);
    });
    let job = JobContext::with_job(guard, Some(emit));

    let error = create_project_with_job(&store, payload(&path), &job)
        .expect_err("cancelled import must fail");
    assert_eq!(error.to_string(), "Cancelled.");
    assert!(store.all().is_empty(), "no project may be registered");
    assert!(
        fs::read_dir(store.staging_root())
            .expect("staging root")
            .next()
            .is_none(),
        "staging directory must be removed"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn column_width_estimate_uses_only_the_leading_sample() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("create store");
    let path = root.join("widths.csv");
    let mut body = String::from("id,text\n");
    for index in 0..COLUMN_WIDTH_SAMPLE_ROWS + 50 {
        if index == 5 {
            writeln!(body, "{},{}", index, "x".repeat(42)).expect("format sample row");
        } else if index >= COLUMN_WIDTH_SAMPLE_ROWS {
            writeln!(body, "{},{}", index, "y".repeat(500)).expect("format late row");
        } else {
            writeln!(body, "{},short", index).expect("format row");
        }
    }
    fs::write(&path, body).expect("write fixture");

    let summary = create_project_with_job(&store, payload(&path), &JobContext::none())
        .expect("create project");
    let metrics_path = store
        .project_dir(&summary.meta.id)
        .join("column_max_chars.json");
    let metrics = load_column_metrics(&metrics_path)
        .expect("load metrics")
        .expect("metrics were written during import");

    // The sampled maximum (42) wins; the 500-character values after the sample
    // must not enlarge the estimate.
    assert_eq!(metrics.get("c000002"), Some(&42));
    assert_eq!(metrics.get("c000001"), Some(&7));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn legacy_width_estimate_scans_only_the_leading_sample() {
    let mut values: Vec<String> = (0..COLUMN_WIDTH_SAMPLE_ROWS)
        .map(|_| "short".to_string())
        .collect();
    values.push("z".repeat(500));
    let df = DataFrame::new(vec![Series::new("c000001", values)]).expect("build frame");

    let metrics = compute_column_max_chars(&df);
    assert_eq!(
        metrics.get("c000001"),
        Some(&7),
        "values after the sample must not be scanned"
    );
}

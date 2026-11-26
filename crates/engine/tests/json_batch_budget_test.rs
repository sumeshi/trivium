//! Sparse JSON must not turn a small input into an unbounded dense batch.
use std::{fmt::Write as _, fs, path::PathBuf, sync::Mutex};

use polars::prelude::AnyValue;
use serde_json::{json, Value};
use trivium_engine::{
    import::jsonl::{BatchLimits, JsonImporter, RecordFormat, MAX_JSON_COLUMNS},
    jobs::{queue_job, request_cancel, JobContext, JobKind},
    ops::projects::{create_project, CreateProjectPayload},
    project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD},
    store::ProjectsStore,
};
use uuid::Uuid;

static JOB_LOCK: Mutex<()> = Mutex::new(());

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-json-budget-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn sparse_records(count: usize) -> Vec<Value> {
    (0..count)
        .map(|row| {
            let mut record = serde_json::Map::new();
            record.insert(
                format!("k{row:05}"),
                json!(format!("value,{row} <x>日本語</x>")),
            );
            Value::Object(record)
        })
        .collect()
}

#[test]
fn sparse_batches_bound_missing_cells_and_preserve_values_ids_and_originals() {
    const RECORDS: usize = 800;
    const BUDGET: usize = 512 * 1024;
    let fixture = Fixture::new();
    let records = sparse_records(RECORDS);
    for format in [RecordFormat::Json, RecordFormat::JsonLines] {
        let source = fixture.0.join(if format == RecordFormat::Json {
            "sparse.json"
        } else {
            "sparse.jsonl"
        });
        let contents = if format == RecordFormat::Json {
            serde_json::to_string(&records).unwrap()
        } else {
            records.iter().map(|record| format!("{record}\n")).collect()
        };
        fs::write(&source, &contents).unwrap();
        let importer = JsonImporter::open(
            &source,
            format,
            BatchLimits {
                max_rows: 50_000,
                max_bytes: BUDGET,
            },
            &JobContext::none(),
        )
        .unwrap();
        let columns = importer.columns().to_vec();
        assert_eq!(columns.len(), RECORDS);
        let mut position = 0;
        let mut batches = 0;
        importer
            .write_batches(&JobContext::none(), |frame| {
                batches += 1;
                // Even the occupied lengths alone impose this necessary memory
                // bound; payload-only accounting used to put all 800 rows here.
                assert!(frame.height() * RECORDS * std::mem::size_of::<Option<String>>() <= BUDGET);
                for local in 0..frame.height() {
                    let row = position + local;
                    assert_eq!(
                        frame.column(INTERNAL_ROW_ID).unwrap().get(local).unwrap(),
                        AnyValue::Int64(row as i64)
                    );
                    assert_eq!(
                        frame
                            .column(INTERNAL_SOURCE_RECORD)
                            .unwrap()
                            .get(local)
                            .unwrap(),
                        AnyValue::Int64(row as i64 + 1)
                    );
                    let value = frame
                        .column(&columns[row].id)
                        .unwrap()
                        .str()
                        .unwrap()
                        .get(local)
                        .unwrap();
                    assert_eq!(value, records[row][format!("k{row:05}")].as_str().unwrap());
                    let absent = (row + 1) % RECORDS;
                    assert!(frame
                        .column(&columns[absent].id)
                        .unwrap()
                        .str()
                        .unwrap()
                        .get(local)
                        .is_none());
                    let original = frame
                        .column(INTERNAL_ORIGINAL)
                        .unwrap()
                        .str()
                        .unwrap()
                        .get(local)
                        .unwrap();
                    assert_eq!(
                        serde_json::from_str::<Value>(original).unwrap(),
                        records[row]
                    );
                }
                position += frame.height();
                Ok(())
            })
            .unwrap();
        assert_eq!(position, RECORDS);
        assert!(
            batches > 20,
            "missing slots must trigger many small batches"
        );
        assert_eq!(fs::read_to_string(&source).unwrap(), contents);
    }
}

#[test]
fn excessive_column_union_is_rejected_without_registering_or_leaving_staging() {
    let fixture = Fixture::new();
    let source = fixture.0.join("too-wide.jsonl");
    let mut contents = String::new();
    for row in 0..20_000 {
        writeln!(&mut contents, "{{\"root\":{{\"k/{row:05}~\":1}}}}").unwrap();
    }
    fs::write(&source, &contents).unwrap();
    let store = ProjectsStore::new(fixture.0.join("projects")).unwrap();
    let payload = || CreateProjectPayload {
        import_options: None,
        path: source.to_string_lossy().into_owned(),
        description: None,
        delimiter: None,
        restore_annotations: None,
    };
    let error = create_project(&store, payload()).unwrap_err();
    assert_eq!(
        error.to_string(),
        format!(
            "JSON record {} exceeds the limit of {} flattened columns. No columns were discarded.",
            MAX_JSON_COLUMNS + 1,
            MAX_JSON_COLUMNS
        )
    );
    assert!(store.try_all().unwrap().is_empty());
    assert!(fs::read_dir(store.staging_root()).unwrap().next().is_none());
    assert_eq!(fs::read_to_string(&source).unwrap(), contents);
    // A rejected import must not poison a subsequent import into the same store.
    fs::write(&source, "{\"ok\":1}\n").unwrap();
    assert_eq!(
        create_project(&store, payload())
            .unwrap()
            .meta
            .total_records,
        1
    );
}

#[test]
fn column_limit_accepts_exact_boundary_and_counts_nested_escaped_leaf_paths() {
    let fixture = Fixture::new();
    let source = fixture.0.join("boundary.json");
    let mut map = serde_json::Map::new();
    for index in 0..MAX_JSON_COLUMNS {
        map.insert(format!("k/{index}~"), json!(index));
    }
    fs::write(&source, json!({"nested": map}).to_string()).unwrap();
    let importer = JsonImporter::open(
        &source,
        RecordFormat::Json,
        BatchLimits::default(),
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(importer.columns().len(), MAX_JSON_COLUMNS);
    assert!(importer
        .columns()
        .iter()
        .all(|column| column.source.starts_with("/nested/k~1") && column.source.ends_with("~0")));
    let mut too_wide = json!({"nested": map});
    too_wide["one_more"] = json!({});
    fs::write(&source, too_wide.to_string()).unwrap();
    let error = JsonImporter::open(
        &source,
        RecordFormat::Json,
        BatchLimits::default(),
        &JobContext::none(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("4096 flattened columns"));
}

#[test]
fn one_oversized_record_is_alone_and_budget_flushes_observe_cancellation() {
    let fixture = Fixture::new();
    let source = fixture.0.join("large-record.jsonl");
    let large = "x".repeat(4_096);
    let records = [
        json!({"value":large}),
        json!({"value":"small"}),
        json!({"value":"next"}),
    ];
    fs::write(
        &source,
        records
            .iter()
            .map(|record| format!("{record}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let limits = BatchLimits {
        max_rows: 50_000,
        max_bytes: 1_024,
    };
    let importer = JsonImporter::open(
        &source,
        RecordFormat::JsonLines,
        limits,
        &JobContext::none(),
    )
    .unwrap();
    let mut heights = Vec::new();
    importer
        .write_batches(&JobContext::none(), |frame| {
            heights.push(frame.height());
            Ok(())
        })
        .unwrap();
    assert_eq!(heights, [1, 2]);

    let _lock = JOB_LOCK.lock().unwrap();
    let guard = queue_job(JobKind::Import, None).unwrap();
    let id = guard.id();
    let job = JobContext::with_job(guard, None);
    let importer = JsonImporter::open(&source, RecordFormat::JsonLines, limits, &job).unwrap();
    let mut written = 0;
    let error = importer
        .write_batches(&job, |frame| {
            written += frame.height();
            request_cancel(id);
            Ok(())
        })
        .err()
        .expect("a cancelled import must fail");
    assert_eq!(error.to_string(), "Cancelled.");
    assert_eq!(written, 1);
}

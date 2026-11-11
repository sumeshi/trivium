//! T17: JSON/JSONL import scans the schema first and writes fixed-schema
//! batches without keeping every record.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use polars::prelude::AnyValue;
use serde_json::json;
use uuid::Uuid;

use trivium_engine::import::jsonl::{
    BatchLimits, JsonImporter, RecordFormat,
};
use trivium_engine::import::{json, jsonl};
use trivium_engine::jobs::{
    request_cancel, try_begin_job, JobContext, JobKind, JobProgress, ProgressFn,
};
use trivium_engine::models::ProjectSummary;
use trivium_engine::ops;
use trivium_engine::ops::projects::{
    create_project_with_job, load_project, CreateProjectPayload, ProjectRequest,
};
use trivium_engine::ops::rows::{
    query_project_rows, QueryRowsPayload, QueryRowsResponse, QuerySpec,
};
use trivium_engine::project_io::read_original_records;
use trivium_engine::store::ProjectsStore;

/// The job registry allows two concurrent jobs process-wide. These tests run
/// in parallel, so serialize the ones that begin a job.
static JOB_LOCK: Mutex<()> = Mutex::new(());

fn temp_root() -> PathBuf {    let root = std::env::temp_dir().join(format!("trivium-json-stream-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn write_file(root: &Path, name: &str, body: &str) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn payload(path: &Path) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().to_string(),
        description: None,
        delimiter: None,
        restore_annotations: None,
    }
}

fn create(store: &ProjectsStore, path: &Path) -> ProjectSummary {
    ops::projects::create_project(store, payload(path)).expect("failed to create project")
}

fn query_page(
    store: &ProjectsStore,
    summary: &ProjectSummary,
    offset: usize,
    limit: usize,
) -> QueryRowsResponse {
    query_project_rows(
        store,
        QueryRowsPayload {
            project_id: summary.meta.id,
            spec: QuerySpec::default(),
            offset: Some(offset),
            limit: Some(limit),
        },
    )
    .expect("query failed")
}

fn source_ids(summary: &ProjectSummary) -> std::collections::HashMap<String, String> {
    summary
        .meta
        .columns
        .iter()
        .map(|column| (column.source.clone(), column.id.clone()))
        .collect()
}

fn cell_text<'a>(
    response: &'a QueryRowsResponse,
    row: usize,
    id: &str,
) -> Option<&'a str> {
    response.rows[row]
        .cells
        .get(id)
        .expect("missing cell")
        .text
        .as_deref()
}

#[test]
fn a_key_only_in_the_last_record_stays_a_column() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(
        &root,
        "events.json",
        r#"[
  {"ts": "t1", "user": "alice"},
  {"ts": "t2", "user": "bob"},
  {"ts": "t3", "user": "carol", "only_later": "late"}
]"#,
    );

    let summary = create(&store, &path);
    assert_eq!(summary.meta.total_records, 3);
    let sources: Vec<&str> = summary
        .meta
        .columns
        .iter()
        .map(|column| column.source.as_str())
        .collect();
    assert_eq!(sources, vec!["/ts", "/user", "/only_later"]);

    let ids = source_ids(&summary);
    let last = query_page(&store, &summary, 2, 1);
    assert_eq!(
        cell_text(&last, 0, &ids["/only_later"]),
        Some("late"),
        "the pass-1 union keeps a key that first appears in the last record"
    );
    let first = query_page(&store, &summary, 0, 1);
    assert!(
        cell_text(&first, 0, &ids["/only_later"]).is_none(),
        "an absent key stays null"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn fifty_thousand_element_json_array_imports_every_record() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = root.join("large.json");
    let mut body = String::from("[");
    for index in 0..50_000usize {
        if index > 0 {
            body.push(',');
        }
        write!(
            &mut body,
            r#"{{"id":{},"msg":"row {}"}}"#,
            index, index
        )
        .expect("format array element");
    }
    body.push(']');
    fs::write(&path, body).expect("write large JSON fixture");

    let summary = create(&store, &path);
    assert_eq!(summary.meta.total_records, 50_000);
    let ids = source_ids(&summary);

    let first = query_page(&store, &summary, 0, 1);
    assert_eq!(cell_text(&first, 0, &ids["/id"]), Some("0"));
    assert_eq!(cell_text(&first, 0, &ids["/msg"]), Some("row 0"));

    let last = query_page(&store, &summary, 49_999, 1);
    assert_eq!(cell_text(&last, 0, &ids["/id"]), Some("49999"));
    assert_eq!(cell_text(&last, 0, &ids["/msg"]), Some("row 49999"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn json_and_jsonl_produce_the_same_columns_and_values() {
    let root = temp_root();
    let json_path = write_file(
        &root,
        "same.json",
        r#"[{"ts":"t1","host":{"name":"a"},"n":1},{"ts":"t2","host":{"name":"b"},"n":2}]"#,
    );
    let jsonl_path = write_file(
        &root,
        "same.jsonl",
        "{\"ts\":\"t1\",\"host\":{\"name\":\"a\"},\"n\":1}\n\
         {\"ts\":\"t2\",\"host\":{\"name\":\"b\"},\"n\":2}\n",
    );

    let from_json = json::load(&json_path).expect("json load");
    let from_jsonl = jsonl::load(&jsonl_path).expect("jsonl load");
    let sources = |imported: &trivium_engine::models::ImportedData| {
        imported
            .columns
            .iter()
            .map(|column| column.source.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(sources(&from_json), sources(&from_jsonl));
    assert_eq!(from_json.frame.height(), from_jsonl.frame.height());

    for (column, other) in from_json.columns.iter().zip(from_jsonl.columns.iter()) {
        for row in 0..from_json.frame.height() {
            let left = from_json
                .frame
                .column(&column.id)
                .expect("json column")
                .get(row)
                .expect("json value");
            let right = from_jsonl
                .frame
                .column(&other.id)
                .expect("jsonl column")
                .get(row)
                .expect("jsonl value");
            let text = |value: AnyValue| match value {
                AnyValue::Null => None,
                other => other.get_str().map(str::to_string),
            };
            assert_eq!(text(left), text(right), "row {} column {}", row, column.source);
        }
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn exact_numbers_null_missing_and_empty_objects_survive() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_file(
        &root,
        "values.jsonl",
        concat!(
            "{\"u\":18446744073709551615,\"n\":9007199254740993,",
            "\"huge\":170141183460469231731687303715884105728,",
            "\"nil\":null,\"empty\":\"\"}\n",
            "{\"u\":1,\"n\":2,\"huge\":3}\n",
            "{}\n",
        ),
    );

    let summary = create(&store, &path);
    assert_eq!(summary.meta.total_records, 3, "an empty object is one record");
    let ids = source_ids(&summary);
    let response = query_page(&store, &summary, 0, 3);
    assert_eq!(
        cell_text(&response, 0, &ids["/u"]),
        Some("18446744073709551615")
    );
    assert_eq!(
        cell_text(&response, 0, &ids["/n"]),
        Some("9007199254740993")
    );
    assert_eq!(
        cell_text(&response, 0, &ids["/huge"]),
        Some("170141183460469231731687303715884105728")
    );
    assert!(cell_text(&response, 0, &ids["/nil"]).is_none());
    assert_eq!(cell_text(&response, 0, &ids["/empty"]), Some(""));
    assert!(
        cell_text(&response, 1, &ids["/nil"]).is_none(),
        "a missing key stays null"
    );

    let originals = read_original_records(&store.project_dir(&summary.meta.id))
        .expect("original records");
    assert_eq!(originals[0]["u"], json!(18446744073709551615_u64));
    assert_eq!(
        originals[0]["huge"].to_string(),
        "170141183460469231731687303715884105728"
    );
    assert_eq!(originals[0]["nil"], serde_json::Value::Null);
    assert!(originals[1].get("nil").is_none(), "missing stays missing");
    assert_eq!(originals[2], json!({}));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn cancellation_during_pass_one_returns_cancelled() {
    let root = temp_root();
    let path = root.join("scan.jsonl");
    let mut body = String::new();
    for index in 0..1_500usize {
        writeln!(&mut body, "{{\"id\":{}}}", index).expect("format record");
    }
    fs::write(&path, body).expect("write fixture");

    let _job_lock = JOB_LOCK.lock().expect("job test lock");
    let guard = try_begin_job(JobKind::Import, None).expect("begin job");
    let id = guard.id();
    let emit: ProgressFn = Arc::new(move |progress: JobProgress| {
        if progress.message.as_deref() == Some("Scanning JSON") {
            request_cancel(id);
        }
    });
    let job = JobContext::with_job(guard, Some(emit));

    let error = match JsonImporter::open(
        &path,
        RecordFormat::JsonLines,
        BatchLimits::default(),
        &job,
    ) {
        Ok(_) => panic!("pass 1 must observe the cancel request"),
        Err(error) => error,
    };
    assert_eq!(error.to_string(), "Cancelled.");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn cancellation_at_a_pass_two_batch_boundary_returns_cancelled() {
    let root = temp_root();
    let path = root.join("batches.jsonl");
    fs::write(
        &path,
        "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n{\"id\":4}\n",
    )
    .expect("write fixture");

    let _job_lock = JOB_LOCK.lock().expect("job test lock");
    let guard = try_begin_job(JobKind::Import, None).expect("begin job");
    let id = guard.id();
    let job = JobContext::with_job(guard, None);
    let importer = JsonImporter::open(
        &path,
        RecordFormat::JsonLines,
        BatchLimits {
            max_rows: 2,
            max_bytes: usize::MAX,
        },
        &job,
    )
    .expect("open importer");

    let error = importer
        .write_batches(&job, |_batch| {
            request_cancel(id);
            Ok(())
        })
        .err()
        .expect("the next batch boundary must observe the cancel");
    assert_eq!(error.to_string(), "Cancelled.");

    let _ = fs::remove_dir_all(root);
}

#[test]
fn cancelled_json_create_leaves_no_staging_or_project() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("create store");
    let path = write_file(
        &root,
        "cancel.json",
        r#"[{"a":1},{"a":2},{"a":3}]"#,
    );

    let _job_lock = JOB_LOCK.lock().expect("job test lock");
    let guard = try_begin_job(JobKind::Import, None).expect("begin job");
    let id = guard.id();
    let emit: ProgressFn = Arc::new(move |_progress: JobProgress| {
        request_cancel(id);
    });
    let job = JobContext::with_job(guard, Some(emit));

    let error = create_project_with_job(&store, payload(&path), &job)
        .expect_err("a cancelled import must fail");
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
fn broken_inputs_report_element_or_line_numbers() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("create store");

    let non_object = write_file(&root, "bad_element.json", r#"[{"a":1},2]"#);
    let error = ops::projects::create_project(&store, payload(&non_object))
        .expect_err("a non-object array element must fail");
    assert!(
        error
            .to_string()
            .contains("Record 2 in the JSON array is not an object."),
        "unexpected error: {}",
        error
    );

    let broken = write_file(&root, "broken.json", r#"[{"a":1},"#);
    let error = ops::projects::create_project(&store, payload(&broken))
        .expect_err("broken JSON must fail");
    assert!(
        error.to_string().starts_with("Failed to parse JSON:"),
        "unexpected error: {}",
        error
    );

    let trailing = write_file(&root, "trailing.json", r#"[{"a":1}] trailing"#);
    let error = ops::projects::create_project(&store, payload(&trailing))
        .expect_err("trailing characters must fail");
    assert!(
        error.to_string().starts_with("Failed to parse JSON:"),
        "unexpected error: {}",
        error
    );

    let broken_lines = write_file(
        &root,
        "broken.jsonl",
        "{\"a\":1}\n\n{\"a\":2,\n{\"a\":3}\n",
    );
    let error = jsonl::load(&broken_lines)
        .err()
        .expect("broken JSONL must fail");
    assert!(
        error.to_string().contains("line 3"),
        "a blank line is skipped but the error must name line 3: {}",
        error
    );

    let non_object_line = write_file(&root, "bad_line.jsonl", "{\"a\":1}\n2\n");
    let error = jsonl::load(&non_object_line)
        .err()
        .expect("a non-object line must fail");
    assert!(
        error.to_string().contains("JSONL line 2 is not a JSON object."),
        "unexpected error: {}",
        error
    );

    assert!(store.all().is_empty(), "failed imports must not register projects");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn load_project_reopens_a_streamed_json_array() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("create store");
    let path = write_file(&root, "reopen.json", r#"[{"a":1},{"a":2},{"b":3}]"#);
    let summary = create(&store, &path);

    let loaded = load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .expect("load created project");
    assert_eq!(loaded.columns.len(), 2);
    assert_eq!(loaded.initial_rows.len(), 3);

    let _ = fs::remove_dir_all(root);
}

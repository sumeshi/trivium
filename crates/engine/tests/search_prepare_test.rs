//! T18: search preparation happens once per query, and row evaluation touches
//! only the columns the query needs.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use uuid::Uuid;

use trivium_engine::condition::eval::{regex_compile_count, reset_regex_compile_count};
use trivium_engine::condition::{Condition, Node, Op};
use trivium_engine::jobs::{self, JobContext, JobKind};
use trivium_engine::ops;
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::{query_project_rows_with_job, QueryRowsPayload, QuerySpec};
use trivium_engine::search::{
    reset_searchable_text_build_count, searchable_text_build_count,
};
use trivium_engine::store::ProjectsStore;
use trivium_engine::value_utils::{reset_stringified_columns, stringified_column_count};

mod common;

/// The instrumentation counters are process-wide; tests that read them must not
/// run concurrently inside this test binary.
static HOOK_LOCK: Mutex<()> = Mutex::new(());

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    ids: HashMap<String, String>,
}

impl Fixture {
    fn create(csv: &str) -> Self {
        let root = std::env::temp_dir().join(format!("trivium-search-prep-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("failed to create temp dir");
        let path = root.join("rows.csv");
        fs::write(&path, csv).expect("failed to write fixture");

        let store = ProjectsStore::new(root.clone()).expect("failed to create store");
        let id = ops::projects::create_project(
            &store,
            CreateProjectPayload {
                path: path.to_string_lossy().to_string(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .expect("failed to create project")
        .meta
        .id;
        let ids = common::column_ids(&store, id);
        Self {
            root,
            store,
            id,
            ids,
        }
    }

    fn column(&self, source: &str) -> String {
        common::id(&self.ids, source)
    }

    fn query(&self, spec: QuerySpec) -> Result<usize, String> {
        query_project_rows_with_job(
            &self.store,
            QueryRowsPayload {
                project_id: self.id,
                spec,
                offset: None,
                limit: None,
            },
            &JobContext::none(),
        )
        .map(|response| response.total_filtered_rows)
        .map_err(|error| error.to_string())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn spec(
    search: Option<&str>,
    search_columns: Option<Vec<String>>,
    condition: Option<Node>,
) -> QuerySpec {
    QuerySpec {
        search: search.map(str::to_string),
        search_columns,
        condition,
        ..Default::default()
    }
}

fn condition(column: String, op: Op, values: &[&str]) -> Node {
    Node::Condition(Condition {
        column,
        op,
        values: values.iter().map(|value| (*value).to_string()).collect(),
        negated: false,
    })
}

fn hundred_rows() -> String {
    let mut body = String::from("name,indicator\n");
    for index in 0..100 {
        body.push_str(&format!("row{index},ABC-{index}\n"));
    }
    body
}

#[test]
fn regex_is_compiled_once_for_the_query_not_once_per_row() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::create(&hundred_rows());
    let indicator = fixture.column("indicator");

    reset_regex_compile_count();
    let matched = fixture
        .query(spec(
            None,
            None,
            Some(condition(indicator, Op::Regex, &["^ABC-"])),
        ))
        .expect("query failed");

    assert_eq!(matched, 100);
    assert_eq!(regex_compile_count(), 1);
}

#[test]
fn condition_on_one_column_does_not_stringify_unrelated_columns() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let big_value = "x".repeat(1024 * 1024);
    let mut body = String::from("small,big\n");
    for index in 0..100 {
        let small = if index % 2 == 0 { "keep" } else { "drop" };
        let big = if index == 0 { big_value.as_str() } else { "tiny" };
        body.push_str(&format!("{small},{big}\n"));
    }
    let fixture = Fixture::create(&body);
    let small = fixture.column("small");
    let big = fixture.column("big");

    reset_stringified_columns();
    let matched = fixture
        .query(spec(
            None,
            None,
            Some(condition(small.clone(), Op::Eq, &["keep"])),
        ))
        .expect("query failed");

    assert_eq!(matched, 50);
    assert_eq!(stringified_column_count(&small), 100);
    assert_eq!(
        stringified_column_count(&big),
        0,
        "a condition on `small` must not stringify the large `big` column"
    );
}

#[test]
fn unconditional_display_builds_no_full_text_cache() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::create(&hundred_rows());

    reset_searchable_text_build_count();
    let matched = fixture.query(spec(None, None, None)).expect("query failed");

    assert_eq!(matched, 100);
    assert_eq!(searchable_text_build_count(), 0);
}

#[test]
fn unknown_columns_and_invalid_regex_fail_before_evaluation() {
    let fixture = Fixture::create(&hundred_rows());

    let unknown_condition = fixture
        .query(spec(
            None,
            None,
            Some(condition("c999999".to_string(), Op::Eq, &["x"])),
        ))
        .expect_err("unknown condition column must fail");
    assert!(
        unknown_condition.contains("Unknown column: c999999"),
        "unexpected error: {unknown_condition}"
    );

    let unknown_scope = fixture
        .query(spec(
            Some("needle"),
            Some(vec!["c999999".to_string()]),
            None,
        ))
        .expect_err("unknown search column must fail");
    assert!(
        unknown_scope.contains("Unknown column: c999999"),
        "unexpected error: {unknown_scope}"
    );

    let unknown_scoped_term = fixture
        .query(spec(Some("c999999:needle"), None, None))
        .expect_err("unknown scoped search column must fail");
    assert!(
        unknown_scoped_term.contains("Unknown column: c999999"),
        "unexpected error: {unknown_scoped_term}"
    );

    let indicator = fixture.column("indicator");
    let invalid_regex = fixture
        .query(spec(
            None,
            None,
            Some(condition(indicator, Op::Regex, &["["])),
        ))
        .expect_err("invalid regex must fail");
    assert!(
        invalid_regex.contains("Invalid regex"),
        "unexpected error: {invalid_regex}"
    );
}

#[test]
fn cancelled_job_stops_the_search() {
    let _guard = HOOK_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let mut body = String::from("name,value\n");
    for index in 0..5_000 {
        body.push_str(&format!("row{index},value-{index}\n"));
    }
    let fixture = Fixture::create(&body);

    let guard = jobs::try_begin_job(JobKind::Query, Some(fixture.id))
        .expect("failed to begin query job");
    jobs::request_cancel(guard.id());
    let job = JobContext::with_job(guard, None);

    let error = query_project_rows_with_job(
        &fixture.store,
        QueryRowsPayload {
            project_id: fixture.id,
            spec: spec(Some("value"), None, None),
            offset: None,
            limit: None,
        },
        &job,
    )
    .expect_err("a cancelled job must stop the search");

    assert_eq!(error.to_string(), "Cancelled.");
}

use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use super::*;
use crate::{
    condition::{Condition, Node, Op},
    formats::FormatDefinition,
    jobs::{self, JobKind},
    models::ProjectSummary,
    ops::{
        projects::{
            create_project, delete_project, is_temporary_project, list_projects, load_project,
            CreateProjectPayload, ProjectRequest,
        },
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    store::ProjectsStore,
};
use sha2::{Digest, Sha256};

mod json;

struct Case {
    store: ProjectsStore,
    root: PathBuf,
    id: uuid::Uuid,
    columns: Vec<String>,
}
impl Case {
    fn csv() -> Self {
        Self::new("sample.csv", "CommandLine,Other,Scope\n\"日本語 10.0.0.1,10.0.0.1 and 8.8.8.8\",10.0.0.1,keep\n8.8.8.8,127.0.0.1,drop\n256.1.2.3,,keep\n")
    }
    fn new(name: &str, text: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("trivium-extraction-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join(name);
        fs::write(&path, text).unwrap();
        let store = ProjectsStore::new(root.join("store")).unwrap();
        let meta = create_project(
            &store,
            CreateProjectPayload {
                import_options: None,
                path: path.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta;
        Self {
            columns: meta
                .columns
                .iter()
                .map(|column| column.id.clone())
                .collect(),
            root,
            store,
            id: meta.id,
        }
    }
    fn payload(&self) -> ExtractPayload {
        ExtractPayload {
            project_id: self.id,
            columns: self.columns[..2].to_vec(),
            format: ipv4(),
            scope: ExtractScope::Filtered,
            spec: QuerySpec::default(),
        }
    }
    fn values(&self, result: &ProjectSummary) -> Vec<csv::StringRecord> {
        let path = result.meta.source_path.as_ref().unwrap();
        let mut csv = csv::Reader::from_path(path).unwrap();
        assert_eq!(
            csv.headers().unwrap().iter().collect::<Vec<_>>(),
            vec!["Value", "Count", "Rows"]
        );
        csv.records().collect::<Result<Vec<_>, _>>().unwrap()
    }
    fn recent(&self) -> Vec<uuid::Uuid> {
        list_projects(&self.store)
            .unwrap()
            .iter()
            .map(|project| project.meta.id)
            .collect()
    }
    fn assert_no_generation_files(&self) {
        assert_eq!(fs::read_dir(self.store.staging_root()).unwrap().count(), 0);
    }
}
impl Drop for Case {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn ipv4() -> FormatDefinition {
    serde_json::from_str(include_str!("../../../../../definitions/formats/ipv4.json")).unwrap()
}
fn record(value: &str, count: &str, rows: &str) -> csv::StringRecord {
    csv::StringRecord::from(vec![value, count, rows])
}
fn cancellable() -> jobs::JobGuard {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(guard) = jobs::queue_job(JobKind::Analysis, None) {
            if guard.is_running() {
                return guard;
            }
        }
        assert!(
            Instant::now() < deadline,
            "other test jobs did not release their slots"
        );
        std::thread::yield_now();
    }
}

#[test]
fn extracted_csv_is_an_ordinary_queryable_case_with_only_values_and_exact_counts() {
    let case = Case::csv();
    let original = fs::read(case.root.join("sample.csv")).unwrap();
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    assert_eq!(result.meta.total_records, 3);
    assert_eq!(
        case.values(&result),
        vec![
            record("10.0.0.1", "3", "1"),
            record("8.8.8.8", "2", "2"),
            record("127.0.0.1", "1", "1")
        ]
    );
    assert_eq!(
        case.recent(),
        vec![case.id],
        "generated results must not enter Recent"
    );
    assert_eq!(
        case.store.try_all().unwrap().len(),
        2,
        "ordinary addressing still discovers the case"
    );
    assert!(is_temporary_project(&case.store, &result.meta.id));
    let source = PathBuf::from(result.meta.source_path.as_ref().unwrap());
    assert_eq!(
        source,
        fs::canonicalize(case.store.project_dir(&result.meta.id))
            .unwrap()
            .join("source.csv")
    );
    let bytes = fs::read(&source).unwrap();
    assert_eq!(result.meta.source_size, Some(bytes.len() as u64));
    assert_eq!(
        result.meta.source_sha256,
        Some(format!("{:x}", Sha256::digest(&bytes)))
    );
    assert_eq!(fs::read(case.root.join("sample.csv")).unwrap(), original);
    case.assert_no_generation_files();

    let loaded = load_project(
        &case.store,
        ProjectRequest {
            project_id: result.meta.id,
        },
    )
    .unwrap();
    assert_eq!(loaded.project.meta.total_records, 3);
    let count = result
        .meta
        .columns
        .iter()
        .find(|column| column.label == "Count")
        .unwrap()
        .id
        .clone();
    let page = query_project_rows(
        &case.store,
        QueryRowsPayload {
            project_id: result.meta.id,
            spec: QuerySpec {
                sort_key: Some(count.clone()),
                sort_direction: Some("asc".into()),
                ..Default::default()
            },
            offset: Some(0),
            limit: Some(10),
        },
    )
    .unwrap();
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.cells[&count].text.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["1", "2", "3"]
    );
}

#[test]
fn filtered_and_all_scopes_create_stable_independent_csvs() {
    let case = Case::csv();
    let mut payload = case.payload();
    payload.spec.condition = Some(Node::Condition(Condition {
        column: case.columns[2].clone(),
        op: Op::Eq,
        values: vec!["keep".into()],
        negated: false,
    }));
    let filtered = extract_values(&case.store, payload.clone(), &JobContext::none()).unwrap();
    assert_eq!(
        case.values(&filtered),
        vec![record("10.0.0.1", "3", "1"), record("8.8.8.8", "1", "1")]
    );
    payload.scope = ExtractScope::All;
    let all = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!(case.values(&all).len(), 3);
    assert_ne!(all.meta.id, filtered.meta.id);
    assert_eq!(case.recent(), vec![case.id]);
    case.assert_no_generation_files();
}

#[test]
fn internal_missing_and_repeated_column_inputs_do_not_publish_partial_cases() {
    let case = Case::csv();
    for columns in [
        vec![],
        vec!["__original".into()],
        vec!["unknown".into()],
        vec![case.columns[0].clone(), case.columns[0].clone()],
    ] {
        let mut payload = case.payload();
        payload.columns = columns;
        assert!(extract_values(&case.store, payload, &JobContext::none()).is_err());
    }
    let error = scan::build(
        &case.store,
        case.payload(),
        aggregate::Limits {
            distinct: 1,
            bytes: 4096,
        },
        &JobContext::none(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("no complete result"));
    assert_eq!(case.store.try_all().unwrap().len(), 1);
    case.assert_no_generation_files();
}

#[test]
fn cancelling_before_scan_or_during_csv_creation_leaves_no_case_or_generation_files() {
    let case = Case::csv();
    let guard = cancellable();
    jobs::request_cancel(guard.id());
    let error = extract_values(
        &case.store,
        case.payload(),
        &JobContext::with_job(guard, None),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "Cancelled.");
    let table = scan::build(
        &case.store,
        case.payload(),
        aggregate::Limits::default(),
        &JobContext::none(),
    )
    .unwrap();
    let guard = cancellable();
    let id = guard.id();
    let progress: jobs::ProgressFn = Arc::new(move |progress| {
        if progress.message.as_deref() == Some("Preparing extracted CSV") {
            jobs::request_cancel(id);
        }
    });
    let error = output::open_csv(
        &case.store,
        table,
        &JobContext::with_job(guard, Some(progress)),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "Cancelled.");
    assert_eq!(case.store.try_all().unwrap().len(), 1);
    case.assert_no_generation_files();
}

#[test]
fn ordinary_export_and_delete_work_without_an_extract_specific_lifetime_or_paging_api() {
    use crate::ops::export::{export_project, ExportProjectPayload};
    let case = Case::csv();
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    let destination = case.root.join("saved.csv");
    let export = |destination: PathBuf| ExportProjectPayload {
        project_id: result.meta.id,
        destination: destination.to_string_lossy().into(),
        allow_overwrite: false,
        scope: Some("all".into()),
        spec: QuerySpec::default(),
        columns: None,
        row_indices: None,
        include_annotations: Some(false),
    };
    assert_eq!(
        export_project(&case.store, export(destination.clone()))
            .unwrap()
            .rows,
        3
    );
    let rows = csv::Reader::from_path(destination)
        .unwrap()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, case.values(&result));
    assert!(export_project(
        &case.store,
        export(PathBuf::from(result.meta.source_path.as_ref().unwrap()))
    )
    .is_err());
    delete_project(
        &case.store,
        ProjectRequest {
            project_id: result.meta.id,
        },
    )
    .unwrap();
    assert!(!case.store.project_dir(&result.meta.id).exists());
    assert_eq!(case.recent(), vec![case.id]);
}

#[test]
fn json_cells_are_not_duplicated_from_internal_original_records() {
    let case = Case::new(
        "nested.jsonl",
        "{\"details\":{\"message\":\"IP=10.1.2.3\"},\"other\":\"10.1.2.3\"}\n{\"other\":null}\n",
    );
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    assert_eq!(case.values(&result), vec![record("10.1.2.3", "2", "1")]);
    case.assert_no_generation_files();
}

#[test]
fn result_csv_remains_unchanged_when_source_annotations_change() {
    use crate::ops::flags::{update_flag, UpdateFlagPayload};
    let case = Case::csv();
    let flag = |row, value: &str| {
        update_flag(
            &case.store,
            UpdateFlagPayload {
                project_id: case.id,
                row_index: row,
                flag: Some(value.into()),
                memo: None,
                tags: None,
                bookmarked: None,
            },
        )
        .unwrap()
    };
    flag(0, "critical");
    let mut payload = case.payload();
    payload.spec.flag_filter = Some("critical".into());
    let first = extract_values(&case.store, payload.clone(), &JobContext::none()).unwrap();
    let original = case.values(&first);
    flag(0, "safe");
    flag(1, "critical");
    let second = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!(case.values(&first), original);
    assert_eq!(
        case.values(&second),
        vec![record("127.0.0.1", "1", "1"), record("8.8.8.8", "1", "1")]
    );
}

#[test]
fn byte_and_value_limits_fail_without_truncating_or_publishing_a_case() {
    let case = Case::csv();
    let error = scan::build(
        &case.store,
        case.payload(),
        aggregate::Limits {
            distinct: 100_000,
            bytes: 256,
        },
        &JobContext::none(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("no complete result"));
    let long = Case::new(
        "long.csv",
        &format!("first,second\n{},unused\n", "a".repeat(65_537)),
    );
    let mut payload = long.payload();
    payload.format = FormatDefinition {
        schema_version: 1,
        id: "long".into(),
        name: "Long".into(),
        pattern: "a+".into(),
        value_group: None,
        validation: None,
        examples: None,
    };
    assert!(extract_values(&long.store, payload, &JobContext::none())
        .unwrap_err()
        .to_string()
        .contains("64 KiB"));
    assert_eq!(long.store.try_all().unwrap().len(), 1);
}

#[test]
fn a_normal_case_named_like_an_extraction_stays_in_recent_and_temporary_cases_survive_store_reopen()
{
    let case = Case::new("extract-ipv4-user.csv", "first,second\n10.0.0.1,none\n");
    assert!(!is_temporary_project(&case.store, &case.id));
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    let reopened = ProjectsStore::new(case.root.join("store")).unwrap();
    assert!(reopened.try_find(&result.meta.id).unwrap().is_some());
    assert_eq!(
        list_projects(&reopened)
            .unwrap()
            .iter()
            .map(|summary| summary.meta.id)
            .collect::<Vec<_>>(),
        vec![case.id]
    );
    assert!(PathBuf::from(result.meta.source_path.unwrap()).is_file());
}

#[test]
fn no_matches_opens_a_valid_header_only_csv() {
    let case = Case::new("empty.csv", "first,second\nno-address,none\n");
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    assert_eq!(result.meta.total_records, 0);
    assert!(case.values(&result).is_empty());
    assert_eq!(
        load_project(
            &case.store,
            ProjectRequest {
                project_id: result.meta.id
            }
        )
        .unwrap()
        .columns
        .len(),
        3
    );
}

#[test]
fn a_non_ip_format_uses_the_same_three_column_csv_and_distinct_row_counts() {
    let case = Case::new(
        "email.csv",
        "first,second\n\"a@example.com,a@example.com\",a@example.com\na@example.com,\n",
    );
    let mut payload = case.payload();
    payload.format = serde_json::from_str(include_str!(
        "../../../../../definitions/formats/email.json"
    ))
    .unwrap();
    let result = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!(
        case.values(&result),
        vec![record("a@example.com", "4", "2")]
    );
    assert_eq!(result.meta.columns.len(), 3);
    case.assert_no_generation_files();
}

#[test]
fn merged_network_csv_keeps_ports_prefixes_and_defanged_spelling_as_distinct_values() {
    let case = Case::new(
        "network.csv",
        "first,second\n\"10.0.0.1,10.0.0.1:443,10.0.0.1/24,10[.]0[.]0[.]1\",10.0.0.1:443\n10.0.0.1/24,10[.]0[.]0[.]1\n",
    );
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    assert_eq!(
        case.values(&result),
        vec![
            record("10.0.0.1/24", "2", "2"),
            record("10.0.0.1:443", "2", "1"),
            record("10[.]0[.]0[.]1", "2", "2"),
            record("10.0.0.1", "1", "1"),
        ]
    );
    case.assert_no_generation_files();
}

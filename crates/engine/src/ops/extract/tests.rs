use std::{fs, path::PathBuf, sync::OnceLock};

use parking_lot::{Mutex, MutexGuard};

use super::*;
use crate::{
    condition::{Condition, Node, Op},
    formats::FormatDefinition,
    jobs::{self, JobKind},
    ops::{
        projects::{create_project, CreateProjectPayload},
        query::QuerySpec,
    },
    store::ProjectsStore,
};

// Every extraction test uses the same bounded process cache, just as the UI.
// Serializing this module avoids asserting residency while another test
// deliberately evicts/release entries; other engine tests remain parallel.
pub(super) fn serial() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock()
}

pub(super) struct Case {
    pub store: ProjectsStore,
    root: PathBuf,
    id: uuid::Uuid,
    columns: Vec<String>,
}
impl Case {
    pub fn csv() -> Self {
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
    pub fn payload(&self) -> ExtractPayload {
        ExtractPayload {
            project_id: self.id,
            columns: self.columns[..2].to_vec(),
            format: ipv4(),
            scope: ExtractScope::Filtered,
            spec: QuerySpec::default(),
        }
    }
    pub fn values(&self, id: uuid::Uuid) -> Vec<ExtractValue> {
        extract_values_page(
            &self.store,
            ExtractPagePayload {
                result_id: id,
                offset: 0,
                limit: 100,
            },
        )
        .unwrap()
        .rows
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

#[test]
fn adjacent_values_unicode_counts_rows_and_provenance_are_exact() {
    let _lock = serial();
    let case = Case::csv();
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    assert_eq!(
        (
            result.source_rows,
            result.total_occurrences,
            result.matched_rows,
            result.unique_values
        ),
        (3, 6, 2, 3)
    );
    let rows = case.values(result.result_id);
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.value.as_str(),
                row.count,
                row.rows,
                row.scope.as_deref()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("10.0.0.1", 3, 1, Some("Private")),
            ("8.8.8.8", 2, 2, Some("Public")),
            ("127.0.0.1", 1, 1, Some("Special"))
        ]
    );
    let first = extract_value_sources(
        &case.store,
        ExtractSourcesPayload {
            result_id: result.result_id,
            value: "10.0.0.1".into(),
            offset: 0,
            limit: 2,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert!(first.has_more);
    assert_eq!(
        first
            .rows
            .iter()
            .map(|row| (row.row_index, row.column.as_str(), row.start, row.end))
            .collect::<Vec<_>>(),
        vec![
            (0, case.columns[0].as_str(), 10, 18),
            (0, case.columns[0].as_str(), 19, 27)
        ]
    );
    let last = extract_value_sources(
        &case.store,
        ExtractSourcesPayload {
            result_id: result.result_id,
            value: "10.0.0.1".into(),
            offset: 2,
            limit: 2,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert!(!last.has_more);
    assert_eq!(last.rows[0].column, case.columns[1]);
    assert!(release_extract_result(result.result_id));
    assert!(!release_extract_result(result.result_id));
    assert!(extract_values_page(
        &case.store,
        ExtractPagePayload {
            result_id: result.result_id,
            offset: 0,
            limit: 10
        }
    )
    .is_err());
}

#[test]
fn filtered_scope_and_all_scope_share_definitions_without_changing_source() {
    let _lock = serial();
    let case = Case::csv();
    let before = fs::read(case.root.join("sample.csv")).unwrap();
    let mut payload = case.payload();
    payload.spec.condition = Some(Node::Condition(Condition {
        column: case.columns[2].clone(),
        op: Op::Eq,
        values: vec!["keep".into()],
        negated: false,
    }));
    let filtered = extract_values(&case.store, payload.clone(), &JobContext::none()).unwrap();
    assert_eq!((filtered.source_rows, filtered.total_occurrences), (2, 4));
    assert_eq!(case.values(filtered.result_id).len(), 2);
    payload.scope = ExtractScope::All;
    let all = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!((all.source_rows, all.total_occurrences), (3, 6));
    assert_eq!(fs::read(case.root.join("sample.csv")).unwrap(), before);
    release_extract_result(filtered.result_id);
    release_extract_result(all.result_id);
}

#[test]
fn metadata_and_missing_columns_are_rejected_without_partial_result() {
    let _lock = serial();
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
    let payload = case.payload();
    let error = scan::build(
        &case.store,
        payload,
        aggregate::Limits {
            distinct: 1,
            bytes: 4096,
        },
        &JobContext::none(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("no complete result"));
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    assert_eq!(result.unique_values, 3);
    release_extract_result(result.result_id);
}

#[test]
fn cancellation_does_not_publish_a_result() {
    let _lock = serial();
    let case = Case::csv();
    let guard = jobs::try_begin_job(JobKind::Analysis, Some(case.id)).unwrap();
    let id = guard.id();
    jobs::request_cancel(id);
    let error = extract_values(
        &case.store,
        case.payload(),
        &JobContext::with_job(guard, None),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "Cancelled.");
}

#[test]
fn csv_export_is_complete_atomic_and_requires_overwrite_consent() {
    let _lock = serial();
    let case = Case::csv();
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    let path = case.root.join("extract.csv");
    fs::write(&path, "keep me").unwrap();
    let payload = |overwrite| ExportExtractPayload {
        result_id: result.result_id,
        destination: path.to_string_lossy().into(),
        allow_overwrite: overwrite,
    };
    let error = export_extract(&case.store, payload(false), &JobContext::none()).unwrap_err();
    assert!(error
        .to_string()
        .starts_with("Overwrite confirmation required:"));
    assert_eq!(fs::read_to_string(&path).unwrap(), "keep me");
    assert_eq!(
        export_extract(&case.store, payload(true), &JobContext::none())
            .unwrap()
            .rows,
        3
    );
    let mut reader = csv::Reader::from_path(path).unwrap();
    assert_eq!(
        reader.headers().unwrap().iter().collect::<Vec<_>>(),
        vec!["Value", "Count", "Rows", "Scope"]
    );
    assert_eq!(reader.records().count(), 3);
    let managed = case.store.project_dir(&case.id).join("result.csv");
    assert!(export_extract(
        &case.store,
        ExportExtractPayload {
            result_id: result.result_id,
            destination: managed.to_string_lossy().into(),
            allow_overwrite: true
        },
        &JobContext::none()
    )
    .is_err());
    release_extract_result(result.result_id);
}

#[test]
fn json_cells_are_not_duplicated_from_internal_original_records() {
    let _lock = serial();
    let case = Case::new(
        "nested.jsonl",
        "{\"details\":{\"message\":\"IP=10.1.2.3\"},\"other\":\"10.1.2.3\"}\n{\"other\":null}\n",
    );
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    assert_eq!(
        (
            result.total_occurrences,
            result.matched_rows,
            result.unique_values
        ),
        (2, 1, 1)
    );
    assert_eq!(case.values(result.result_id)[0].rows, 1);
    release_extract_result(result.result_id);
}

#[test]
fn closed_or_changed_sources_cannot_be_rescanned() {
    let _lock = serial();
    let case = Case::csv();
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    fs::write(
        case.store.project_dir(&case.id).join("data.parquet"),
        "changed",
    )
    .unwrap();
    let error = extract_values_page(
        &case.store,
        ExtractPagePayload {
            result_id: result.result_id,
            offset: 0,
            limit: 10,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("source data changed"));
    release_extract_result(result.result_id);
}

#[test]
fn a_filtered_result_keeps_its_original_membership_after_flags_change() {
    use crate::ops::flags::{update_flag, UpdateFlagPayload};
    let _lock = serial();
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
    flag(0, "safe");
    flag(1, "critical");
    let sources = extract_value_sources(
        &case.store,
        ExtractSourcesPayload {
            result_id: first.result_id,
            value: "8.8.8.8".into(),
            offset: 0,
            limit: 10,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(sources.rows.len(), 1);
    assert_eq!(
        sources.rows[0].row_index, 0,
        "an old result must not adopt newly matching rows"
    );
    let second = extract_values(&case.store, payload, &JobContext::none()).unwrap();
    assert_eq!(second.total_occurrences, 2);
    assert_eq!(first.total_occurrences, 4);
    release_extract_result(first.result_id);
    release_extract_result(second.result_id);
}

#[test]
fn byte_and_value_limits_fail_without_truncation() {
    let _lock = serial();
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
        classifier: None,
        examples: None,
    };
    let error = extract_values(&long.store, payload, &JobContext::none()).unwrap_err();
    assert!(error.to_string().contains("64 KiB"));
}

#[test]
fn transient_cache_evicts_old_results_and_pages_stay_bounded() {
    let _lock = serial();
    let case = Case::csv();
    let mut results = Vec::new();
    for _ in 0..5 {
        results.push(
            extract_values(&case.store, case.payload(), &JobContext::none())
                .unwrap()
                .result_id,
        );
    }
    assert!(extract_values_page(
        &case.store,
        ExtractPagePayload {
            result_id: results[0],
            offset: 0,
            limit: 10
        }
    )
    .unwrap_err()
    .to_string()
    .contains("evicted"));
    let last = *results.last().unwrap();
    let page = extract_values_page(
        &case.store,
        ExtractPagePayload {
            result_id: last,
            offset: 1,
            limit: 1,
        },
    )
    .unwrap();
    assert_eq!((page.rows.len(), page.total, page.offset), (1, 3, 1));
    assert_eq!(page.rows[0].value, "8.8.8.8");
    for id in results {
        release_extract_result(id);
    }
}

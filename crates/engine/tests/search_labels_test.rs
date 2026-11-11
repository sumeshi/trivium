//! Scoped display labels and JSON Pointers must identify the same population
//! in the table, navigation, export, annotations and auxiliary panels.

use std::{collections::HashMap, fs, path::PathBuf, sync::Mutex};

use serde::de::DeserializeOwned;
use serde_json::json;
use uuid::Uuid;

use trivium_engine::{
    annotation::AnnotationPatch,
    ops::{
        bulk::{bulk_preview, bulk_update, BulkAnnotatePayload},
        export::{export_project, ExportProjectPayload},
        group::{group_tree, GroupTreePayload},
        histogram::{histogram, HistogramPayload},
        projects::{create_project, CreateProjectPayload},
        report::{build_report, BuildReportPayload},
        rows::{
            locate_project_row, query_project_rows, reset_resolve_count, resolve_count,
            LocateRowPayload, QueryRowsPayload, QuerySpec,
        },
        stats::{column_stats, ColumnStatsPayload},
        time::{time_analysis, TimeAnalysisPayload},
    },
    project_io::{
        last_column_read_projection, project_data_reads, project_full_reads,
        reset_project_read_counters,
    },
    storage::load_flags,
    store::ProjectsStore,
};

mod common;
static TEST_LOCK: Mutex<()> = Mutex::new(());

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    columns: HashMap<String, String>,
}

impl Fixture {
    fn new(name: &str, content: &str) -> Self {
        let root = std::env::temp_dir().join(format!("trivium-search-label-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let source = root.join(name);
        fs::write(&source, content).unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let id = create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        let columns = common::column_ids(&store, id);
        Self {
            root,
            store,
            id,
            columns,
        }
    }

    fn csv() -> Self {
        Self::new(
            "labels.csv",
            concat!(
                "row,cohort,message,score,unused,time\n",
                "r0,keep,allow,0,unrelated,2026-11-01T00:00:00.000000001\n",
                "r1,drop,deny,1,unrelated,2026-11-01T00:00:01.000000002\n",
                "r2,keep,alpha beta,2,unrelated,2026-11-01T00:00:02.000000003\n",
                "r3,other,literal cohort:keep,3,unrelated,2026-11-01T00:00:03.000000004\n",
                "r4,DROP,alpha gamma,4,unrelated,2026-11-01T00:00:04.000000005\n",
                "r5,keep,deny,5,unrelated,2026-11-01T00:00:05.000000006\n",
            ),
        )
    }

    fn query(&self, spec: QuerySpec) -> Vec<usize> {
        query_project_rows(
            &self.store,
            QueryRowsPayload {
                project_id: self.id,
                spec,
                offset: Some(0),
                limit: Some(100),
            },
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.row_index)
        .collect()
    }

    fn search(&self, search: &str) -> Vec<usize> {
        self.query(QuerySpec {
            search: Some(search.into()),
            ..Default::default()
        })
    }

    fn payload<T: DeserializeOwned>(&self, value: serde_json::Value) -> T {
        let mut value = value;
        value["projectId"] = json!(self.id);
        serde_json::from_value(value).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn csv_scopes_preserve_quoted_literals_negation_case_and_or_carry() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::csv();
    assert_eq!(fixture.search("cohort:keep|drop"), [0, 1, 2, 4, 5]);
    assert_eq!(fixture.search("-cohort:drop"), [0, 2, 3, 5]);
    assert_eq!(fixture.search("message:\"alpha beta\""), [2]);
    assert_eq!(fixture.search("-message:\"alpha beta\""), [0, 1, 3, 4, 5]);
    assert_eq!(fixture.search("message:\"alpha beta\"|\"alpha gamma\""), [2, 4]);
    assert!(fixture.search("\"-cohort:drop\"").is_empty());
    assert_eq!(fixture.search("\"cohort:keep\""), [3]);
    assert_eq!(fixture.search("COHORT:keep -message:deny"), [0, 2]);
    assert_eq!(
        fixture.search(&format!("{}:keep", common::id(&fixture.columns, "cohort"))),
        [0, 2, 5]
    );
}

#[test]
fn jsonl_nested_display_labels_and_original_pointers_work_with_stable_ids() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new(
        "nested.jsonl",
        concat!(
            "{\"actor\":{\"name\":\"alice smith\"},\"kind\":\"keep\"}\n",
            "{\"actor\":{\"name\":\"bob\"},\"kind\":\"drop\"}\n",
            "{\"actor\":{\"name\":\"alice smith\"},\"kind\":\"drop\"}\n",
        ),
    );
    assert_eq!(fixture.search("actor/name:\"alice smith\" -kind:drop"), [0]);
    assert_eq!(fixture.search("/actor/name:bob|alice"), [0, 1, 2]);
    assert_eq!(
        fixture.search(&format!(
            "{}:bob",
            common::id(&fixture.columns, "/actor/name")
        )),
        [1]
    );
}

#[test]
fn duplicate_display_labels_fail_explicitly_while_internal_ids_remain_usable() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new(
        "duplicates.csv",
        "duplicate,duplicate,c000001\nleft,right,third\n",
    );
    let error = query_project_rows(
        &fixture.store,
        QueryRowsPayload {
            project_id: fixture.id,
            spec: QuerySpec {
                search: Some("duplicate:left".into()),
                ..Default::default()
            },
            offset: None,
            limit: None,
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("Ambiguous search column: duplicate"));
    assert_eq!(fixture.search("c000001:left"), [0]);
    assert!(
        fixture.search("c000001:third").is_empty(),
        "Explicit ID must win over another source's label"
    );
}

#[test]
fn label_and_id_queries_share_projected_resolution_cache_and_navigation() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::csv();
    reset_resolve_count();
    reset_project_read_counters();
    assert_eq!(fixture.search("cohort:keep"), [0, 2, 5]);
    assert_eq!(resolve_count(), 1);
    let projection = last_column_read_projection();
    assert!(projection.contains(&common::id(&fixture.columns, "cohort")));
    assert!(!projection.contains(&common::id(&fixture.columns, "unused")));
    assert!(!projection.contains(&common::id(&fixture.columns, "message")));
    assert_eq!(project_full_reads(), 0);
    reset_project_read_counters();
    let located = locate_project_row(
        &fixture.store,
        LocateRowPayload {
            project_id: fixture.id,
            row_index: 5,
            spec: QuerySpec {
                search: Some("cohort:keep".into()),
                ..Default::default()
            },
        },
    )
    .unwrap();
    assert_eq!(located.position, Some(2));
    assert_eq!(
        project_data_reads(),
        0,
        "Lookup must reuse the table's resolved IDs"
    );
    assert_eq!(
        fixture.search(&format!("{}:keep", common::id(&fixture.columns, "cohort"))),
        [0, 2, 5]
    );
    assert_eq!(
        resolve_count(),
        1,
        "Label and explicit ID normalize to the same cache key"
    );
    assert_eq!(project_full_reads(), 0);
}

#[test]
fn export_bulk_and_panels_apply_the_same_label_search_as_the_table() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::csv();
    let search = "cohort:keep -message:deny";
    let score = common::id(&fixture.columns, "score");
    let spec = QuerySpec {
        search: Some(search.into()),
        sort_key: Some(score.clone()),
        sort_direction: Some("desc".into()),
        ..Default::default()
    };
    assert_eq!(fixture.query(spec.clone()), [2, 0]);
    let destination = fixture.root.join("export.csv");
    let exported = export_project(
        &fixture.store,
        ExportProjectPayload {
            project_id: fixture.id,
            destination: destination.to_string_lossy().into_owned(),
            scope: Some("filtered".into()),
            spec: spec.clone(),
            columns: Some(vec![common::id(&fixture.columns, "row")]),
            row_indices: None,
            include_annotations: Some(false),
        },
    )
    .unwrap();
    assert_eq!(exported.rows, 2);
    let mut reader = csv::Reader::from_path(&destination).unwrap();
    let rows: Vec<String> = reader
        .records()
        .map(|row| row.unwrap()[0].to_string())
        .collect();
    assert_eq!(rows, ["r2", "r0"]);

    let stats = column_stats(
        &fixture.store,
        ColumnStatsPayload {
            project_id: fixture.id,
            column: score.clone(),
            rare_max: 1,
            spec: spec.clone(),
        },
    )
    .unwrap();
    assert_eq!(stats.top.iter().map(|entry| entry.count).sum::<u64>(), 2);
    let histogram_payload: HistogramPayload =
        fixture.payload(json!({"column":score,"search":search,"buckets":4}));
    let distribution = histogram(&fixture.store, histogram_payload).unwrap();
    assert_eq!(
        distribution
            .buckets
            .iter()
            .map(|bucket| bucket.count)
            .sum::<u64>(),
        2
    );
    let group_payload: GroupTreePayload = fixture
        .payload(json!({"groupColumns":[common::id(&fixture.columns,"cohort")],"search":search}));
    assert_eq!(group_tree(&fixture.store, group_payload).unwrap().total, 2);
    let time_payload: TimeAnalysisPayload =
        fixture.payload(json!({"column":common::id(&fixture.columns,"time"),"search":search}));
    assert_eq!(
        time_analysis(&fixture.store, time_payload)
            .unwrap()
            .sessions
            .iter()
            .map(|span| span.count)
            .sum::<u64>(),
        2
    );
    let report_payload: BuildReportPayload =
        fixture.payload(json!({"destination":fixture.root.join("report.html"),"search":search}));
    assert_eq!(
        build_report(&fixture.store, report_payload)
            .unwrap()
            .hit_rows,
        2
    );

    let bulk = |token| BulkAnnotatePayload {
        project_id: fixture.id,
        target: "filtered".into(),
        row_indices: None,
        spec: spec.clone(),
        patch: AnnotationPatch {
            memo: Some(Some("matching original IDs".into())),
            ..Default::default()
        },
        preview_token: token,
    };
    let preview = bulk_preview(&fixture.store, bulk(None)).unwrap();
    assert_eq!(preview.count, 2);
    assert_eq!(
        bulk_update(&fixture.store, bulk(Some(preview.preview_token)))
            .unwrap()
            .updated,
        2
    );
    let flags = load_flags(&fixture.store.flags_path(&fixture.id)).unwrap();
    let mut annotated: Vec<_> = flags.keys().copied().collect();
    annotated.sort_unstable();
    assert_eq!(annotated, [0, 2]);
}

//! Every flag selection must identify the same original rows for review,
//! aggregation, export and bulk annotations, including the legacy API values.

use std::{collections::HashMap, fs, path::PathBuf, sync::Mutex};

use uuid::Uuid;

use trivium_engine::{
    annotation::AnnotationPatch,
    condition::{Condition, Node, Op},
    ops::{
        bulk::{bulk_preview, bulk_update, BulkAnnotatePayload},
        export::{export_project, ExportProjectPayload},
        flags::{update_flag, UpdateFlagPayload},
        histogram::{histogram, HistogramPayload},
        projects::{create_project, CreateProjectPayload},
        rows::{
            locate_project_row, query_project_rows, reset_resolve_count, resolve_count,
            LocateRowPayload, QueryRowsPayload, QuerySpec,
        },
        stats::{column_stats, ColumnStatsPayload},
    },
    project_io::{project_full_reads, project_page_reads, reset_project_read_counters},
    storage::load_flags,
    store::ProjectsStore,
};

mod common;

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    project: Uuid,
    columns: HashMap<String, String>,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-flag-filter-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let source = root.join("flags.csv");
        let mut csv = String::from("id,search_scope,eligible,score\n");
        for row in 0..24 {
            csv.push_str(&format!(
                "r{row:02},{},{},{row}\n",
                if row < 16 { "keep" } else { "drop" },
                if row < 12 { "yes" } else { "no" },
            ));
        }
        fs::write(&source, csv).unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let project = create_project(
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
        for row in 0..24 {
            update_flag(
                &store,
                UpdateFlagPayload {
                    project_id: project,
                    row_index: row,
                    flag: Some(["safe", "suspicious", "critical", ""][row % 4].into()),
                    memo: None,
                    tags: None,
                    bookmarked: Some(row < 8),
                },
            )
            .unwrap();
        }
        let columns = common::column_ids(&store, project);
        Self {
            root,
            store,
            project,
            columns,
        }
    }

    fn spec(&self, filter: &str) -> QuerySpec {
        QuerySpec {
            search: Some("keep".into()),
            search_columns: Some(vec![common::id(&self.columns, "search_scope")]),
            condition: Some(Node::Condition(Condition {
                column: common::id(&self.columns, "eligible"),
                op: Op::Eq,
                values: vec!["yes".into()],
                negated: false,
            })),
            flag_filter: Some(filter.into()),
            bookmarked_only: Some(true),
            sort_key: Some(common::id(&self.columns, "score")),
            sort_direction: Some("desc".into()),
        }
    }

    fn assert_population(&self, filter: &str, expected: &[usize]) {
        let spec = self.spec(filter);
        let response = query_project_rows(
            &self.store,
            QueryRowsPayload {
                project_id: self.project,
                spec: spec.clone(),
                offset: Some(0),
                limit: Some(100),
            },
        )
        .unwrap();
        let ids: Vec<usize> = response.rows.iter().map(|row| row.row_index).collect();
        assert_eq!(ids, expected, "query population for {filter}");
        assert_eq!(response.total_rows, 24);
        assert_eq!(response.total_filtered_rows, expected.len());
        assert_eq!(response.annotated_total, 18);
        assert_eq!(
            response.annotated_filtered,
            expected.iter().filter(|row| **row % 4 != 3).count(),
            "Unflagged must not count as Safe or as an annotation"
        );

        let stats = column_stats(
            &self.store,
            ColumnStatsPayload {
                project_id: self.project,
                column: common::id(&self.columns, "id"),
                rare_max: 1,
                spec: spec.clone(),
            },
        )
        .unwrap();
        assert_eq!(stats.unique, expected.len() as u64, "stats for {filter}");
        assert_eq!(
            stats.top.iter().map(|value| value.count).sum::<u64>(),
            expected.len() as u64
        );

        // Deserialize the wire shape so additive histogram response/request
        // fields do not couple this population test to its visualization.
        let histogram_payload: HistogramPayload = serde_json::from_value(serde_json::json!({
            "projectId": self.project,
            "column": common::id(&self.columns, "score"),
            "buckets": 4,
            "search": spec.search,
            "searchColumns": spec.search_columns,
            "condition": spec.condition,
            "flagFilter": filter,
            "bookmarkedOnly": true
        }))
        .unwrap();
        let histogram = histogram(&self.store, histogram_payload).unwrap();
        assert_eq!(
            histogram
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            expected.len() as u64
        );
        assert_eq!(histogram.missing, 0);

        let destination = self.root.join("filtered.csv");
        export_project(
            &self.store,
            ExportProjectPayload {
                project_id: self.project,
                destination: destination.to_string_lossy().into_owned(),
                scope: Some("filtered".into()),
                spec: spec.clone(),
                columns: Some(vec![common::id(&self.columns, "id")]),
                row_indices: None,
                include_annotations: Some(false),
            },
        )
        .unwrap();
        let exported: Vec<String> = csv::Reader::from_path(destination)
            .unwrap()
            .records()
            .map(|row| row.unwrap()[0].to_string())
            .collect();
        let expected_export: Vec<String> =
            expected.iter().map(|row| format!("r{row:02}")).collect();
        assert_eq!(exported, expected_export, "export population for {filter}");

        let marker = format!("selection-{}", filter.trim());
        let payload = || BulkAnnotatePayload {
            project_id: self.project,
            target: "filtered".into(),
            row_indices: None,
            spec: spec.clone(),
            patch: AnnotationPatch {
                memo: Some(Some(marker.clone())),
                ..Default::default()
            },
            preview_token: None,
        };
        let preview = bulk_preview(&self.store, payload()).unwrap();
        assert_eq!(preview.count, expected.len(), "bulk preview for {filter}");
        let mut apply = payload();
        apply.preview_token = Some(preview.preview_token);
        assert_eq!(
            bulk_update(&self.store, apply).unwrap().updated,
            expected.len()
        );
        let flags = load_flags(&self.store.flags_path(&self.project)).unwrap();
        let mut annotated: Vec<usize> = flags
            .iter()
            .filter(|(_, annotation)| annotation.memo.as_deref() == Some(marker.as_str()))
            .map(|(row, _)| *row)
            .collect();
        annotated.sort_unstable_by(|left, right| right.cmp(left));
        assert_eq!(
            annotated, expected,
            "bulk must annotate exactly the reviewed IDs"
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn all_sixteen_flag_selections_share_query_stats_histogram_export_and_bulk_targets() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new();
    let tokens = ["safe", "suspicious", "critical", "none"];
    for selection in 0..16 {
        let selected: Vec<&str> = tokens
            .iter()
            .enumerate()
            .filter(|(index, _)| selection & (1 << index) != 0)
            .map(|(_, token)| *token)
            .collect();
        let filter = if selected.is_empty() {
            "all".into()
        } else {
            selected.join("|")
        };
        let expected: Vec<usize> = (0..8)
            .rev()
            .filter(|row| selection == 0 || selection & (1 << (row % 4)) != 0)
            .collect();
        fixture.assert_population(&filter, &expected);
    }
}

#[test]
fn legacy_and_malformed_filters_keep_their_compatible_meaning() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new();
    fixture.assert_population("priority", &[6, 5, 2, 1]);
    fixture.assert_population("none", &[7, 3]);
    fixture.assert_population(" critical | none | critical ", &[7, 6, 3, 2]);
    for filter in [
        "",
        "all",
        "future",
        "safe|future",
        "safe|",
        "safe|all",
        "safe|priority",
    ] {
        fixture.assert_population(filter, &[7, 6, 5, 4, 3, 2, 1, 0]);
    }
}

#[test]
fn selecting_every_flag_keeps_the_source_order_page_and_lookup_fast_paths() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new();
    for filter in [
        "all",
        "safe|suspicious|critical|none",
        "none|critical|safe|suspicious",
    ] {
        reset_project_read_counters();
        reset_resolve_count();
        let spec = QuerySpec {
            flag_filter: Some(filter.into()),
            ..Default::default()
        };
        let page = query_project_rows(
            &fixture.store,
            QueryRowsPayload {
                project_id: fixture.project,
                spec: spec.clone(),
                offset: Some(18),
                limit: Some(3),
            },
        )
        .unwrap();
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>(),
            vec![18, 19, 20]
        );
        assert_eq!(page.total_filtered_rows, 24);
        assert_eq!(page.annotated_filtered, 18);
        assert_eq!(project_page_reads(), 1, "read only the requested page");
        let located = locate_project_row(
            &fixture.store,
            LocateRowPayload {
                project_id: fixture.project,
                row_index: 23,
                spec,
            },
        )
        .unwrap();
        assert_eq!(located.position, Some(23));
        assert_eq!(located.total_filtered_rows, 24);
        assert_eq!(
            resolve_count(),
            0,
            "all selected must not build resolved IDs"
        );
        assert_eq!(project_full_reads(), 0);
        assert_eq!(project_page_reads(), 1, "lookup must not read another page");
    }
}

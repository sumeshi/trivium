//! Scope parity and actual projected decoding through the public engine APIs.
use std::{collections::HashMap, fs, path::PathBuf};
use trivium_engine::{
    jobs::JobContext,
    ops::{
        self,
        export::ExportProjectPayload,
        flags::{update_flag, UpdateFlagPayload},
        projects::CreateProjectPayload,
        rows::{QueryRowsPayload, QuerySpec},
        stats::ColumnStatsPayload,
    },
    project_io::{
        project_data_reads, project_read_stats, reset_project_read_counters,
        reset_project_read_stats, PROJECT_BATCH_ROWS,
    },
    store::ProjectsStore,
};
use uuid::Uuid;
mod common;
struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn analyses_and_sorted_export_share_cached_scope_and_decode_only_requested_columns() {
    let root = Fixture(std::env::temp_dir().join(format!("trivium-stream-{}", Uuid::new_v4())));
    fs::create_dir_all(&root.0).unwrap();
    let path = root.0.join("events.csv");
    let mut csv = csv::Writer::from_path(&path).unwrap();
    csv.write_record(["id", "cohort", "rank", "ts", "unused"])
        .unwrap();
    const ROWS: usize = 20001;
    for row in 0..ROWS {
        csv.write_record([
            format!("row-{row}"),
            if row % 3 == 0 {
                "keep".into()
            } else {
                "drop".into()
            },
            format!("{:05}", (row * 3571) % ROWS),
            format!(
                "2024-01-01T00:{:02}:{:02}.{:09}",
                (row / 60) % 60,
                row % 60,
                row
            ),
            format!("irrelevant-{}-{}", row, "wide".repeat(200)),
        ])
        .unwrap();
    }
    csv.flush().unwrap();
    let store = ProjectsStore::new(root.0.clone()).unwrap();
    let id = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap()
    .meta
    .id;
    let ids = common::column_ids(&store, id);
    for row in [3, 9999, 19998] {
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index: row,
                flag: Some("critical".into()),
                memo: Some(Some(format!("memo {row}\n\"quoted\""))),
                tags: None,
                bookmarked: None,
            },
        )
        .unwrap();
    }
    let spec = QuerySpec {
        search: Some("keep".into()),
        search_columns: Some(vec![common::id(&ids, "cohort")]),
        sort_key: Some(common::id(&ids, "rank")),
        sort_direction: Some("desc".into()),
        ..Default::default()
    };
    let query = ops::rows::query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: spec.clone(),
            offset: Some(0),
            limit: Some(10),
        },
    )
    .unwrap();
    assert_eq!(query.total_filtered_rows, 6667);
    reset_project_read_stats();
    reset_project_read_counters();
    let stats = ops::stats::column_stats_with_job(
        &store,
        ColumnStatsPayload {
            project_id: id,
            column: common::id(&ids, "cohort"),
            rare_max: 1,
            spec: spec.clone(),
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(stats.unique, 1);
    assert_eq!(stats.top[0].count, 6667);
    let stats_reads = project_read_stats();
    assert_eq!(
        stats_reads.decoded_rows, ROWS as u64,
        "cache hit needs only the statistics target scan"
    );
    assert!(stats_reads.max_batch_rows <= PROJECT_BATCH_ROWS as u64);
    assert_eq!(project_data_reads(), 0, "no all-column DataFrame reads");
    reset_project_read_stats();
    let destination = root.0.join("sorted.csv");
    let result = ops::export::export_project_with_job(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().into(),
            scope: None,
            spec: spec.clone(),
            columns: Some(vec![common::id(&ids, "id"), common::id(&ids, "rank")]),
            row_indices: None,
            include_annotations: None,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(result.rows, 6667);
    let reads = project_read_stats();
    assert_eq!(
        reads.decoded_rows, ROWS as u64,
        "arbitrary sorted export scans projected source once"
    );
    assert!(reads.max_batch_rows <= PROJECT_BATCH_ROWS as u64);
    let mut output = csv::Reader::from_path(&destination).unwrap();
    let mut ranks = Vec::new();
    let mut memos = HashMap::new();
    for row in output.records() {
        let row = row.unwrap();
        ranks.push(row[7].parse::<usize>().unwrap());
        if !row[3].is_empty() {
            memos.insert(row[6].to_string(), row[3].to_string());
        }
    }
    assert!(ranks.windows(2).all(|pair| pair[0] > pair[1]));
    for row in [3, 9999, 19998] {
        assert_eq!(
            memos[&format!("row-{row}")],
            format!("memo {row}\n\"quoted\"")
        );
    }
    // Narrow output excludes the deliberately wide unrelated source field.
    assert!(
        reads.column_bytes < 2_000_000,
        "only id/rank chunks should decode: {reads:?}"
    );
    reset_project_read_stats();
    let report = root.0.join("report.html");
    let report_payload = |destination: &std::path::Path| ops::report::BuildReportPayload {
        project_id: id,
        destination: destination.to_string_lossy().into(),
        search: spec.search.clone(),
        columns: Some(vec![common::id(&ids, "id")]),
        search_columns: spec.search_columns.clone(),
        flag_filter: spec.flag_filter.clone(),
        bookmarked_only: spec.bookmarked_only,
        sort_key: spec.sort_key.clone(),
        sort_direction: spec.sort_direction.clone(),
        condition: spec.condition.clone(),
    };
    let metadata =
        ops::report::build_report_with_job(&store, report_payload(&report), &JobContext::none())
            .unwrap();
    assert_eq!(metadata.hit_rows, 6667);
    assert_eq!(
        project_read_stats().decoded_rows,
        ROWS as u64,
        "report shares cached query and scans displayed source columns once"
    );
    let html = fs::read_to_string(&report).unwrap();
    let mut expected = [3usize, 9999, 19998];
    expected.sort_by_key(|row| std::cmp::Reverse((row * 3571) % ROWS));
    let positions = expected.map(|row| html.find(&format!("<tr><td>{row}</td>")).unwrap());
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "report annotation order matches query"
    );
    assert!(html.contains("&quot;quoted&quot;"));
    let source_size = fs::metadata(&path).unwrap().len();
    assert!(ops::report::build_report(&store, report_payload(&path))
        .unwrap_err()
        .to_string()
        .contains("source evidence"));
    assert_eq!(fs::metadata(&path).unwrap().len(), source_size);
    reset_project_read_stats();
    let selected = root.0.join("selected.csv");
    let first = query.rows[0].row_index;
    let result = ops::export::export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: selected.to_string_lossy().into(),
            scope: Some("selected".into()),
            spec,
            columns: Some(Vec::new()),
            row_indices: Some(vec![first, first]),
            include_annotations: Some(true),
        },
    )
    .unwrap();
    assert_eq!(
        result.rows, 1,
        "selection deduplicates source IDs and supports annotation-only output"
    );
}

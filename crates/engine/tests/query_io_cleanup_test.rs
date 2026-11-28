//! Query-local metadata reuse must preserve saved annotations and row ordering.

use std::{
    fs,
    fs::File,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use polars::prelude::*;
use uuid::Uuid;

use trivium_engine::{
    jobs::{queue_job, request_cancel, JobContext, JobKind, ProgressFn},
    models::UpdateFlagResponse,
    ops::{
        flags::{update_flag, UpdateFlagPayload},
        projects::{create_project, CreateProjectPayload},
        rows::{
            locate_project_row, locate_project_row_with_job, query_project_rows,
            reset_resolve_count, resolve_count, LocateRowPayload, QueryRowsPayload,
            QueryRowsResponse, QuerySpec,
        },
    },
    project_io::{read_page_rows_dataframe, read_page_rows_dataframe_with_job},
    store::ProjectsStore,
};

mod common;

static LOCK: Mutex<()> = Mutex::new(());

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("trivium-query-io-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    root: TempRoot,
    store: ProjectsStore,
    id: Uuid,
}

impl Fixture {
    fn new() -> Self {
        let root = TempRoot::new();
        let source = root.0.join("rows.csv");
        fs::write(
            &source,
            "name,key\nkeep,30\ndrop,1\nkeep,10\ndrop,3\nkeep,40\nkeep,20\n",
        )
        .unwrap();
        let store = ProjectsStore::new(root.0.clone()).unwrap();
        let id = create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
                import_options: None,
            },
        )
        .unwrap()
        .meta
        .id;
        Self { root, store, id }
    }

    fn query(&self, spec: QuerySpec) -> QueryRowsResponse {
        query_project_rows(
            &self.store,
            QueryRowsPayload {
                project_id: self.id,
                spec,
                offset: Some(0),
                limit: Some(10),
            },
        )
        .unwrap()
    }

    fn annotate(
        &self,
        row: usize,
        flag: &str,
        bookmarked: bool,
        tags: &[&str],
    ) -> UpdateFlagResponse {
        update_flag(
            &self.store,
            UpdateFlagPayload {
                project_id: self.id,
                row_index: row,
                flag: Some(flag.into()),
                memo: Some(Some(format!("Note <row-{row}>"))),
                tags: Some(tags.iter().map(|tag| (*tag).into()).collect()),
                bookmarked: Some(bookmarked),
            },
        )
        .unwrap()
    }
}

fn row_ids(response: &QueryRowsResponse) -> Vec<usize> {
    response.rows.iter().map(|row| row.row_index).collect()
}

#[test]
fn data_only_cached_pages_keep_current_annotations_and_distinct_counts() {
    let _lock = LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new();
    let spec = QuerySpec {
        search: Some("NAME:keep".into()),
        ..Default::default()
    };
    reset_resolve_count();
    assert_eq!(row_ids(&fixture.query(spec.clone())), [0, 2, 4, 5]);
    assert_eq!(resolve_count(), 1);
    fixture.annotate(0, "safe", true, &["review"]);
    let outside = fixture.annotate(1, "critical", false, &[]);
    for response in [fixture.query(spec.clone()), fixture.query(spec.clone())] {
        assert_eq!(row_ids(&response), [0, 2, 4, 5]);
        assert_eq!(response.annotation_revision, outside.revision);
        assert_eq!(
            (response.annotated_total, response.annotated_filtered),
            (2, 1)
        );
        assert_eq!(response.rows[0].flag, "safe");
        assert_eq!(response.rows[0].memo.as_deref(), Some("Note <row-0>"));
        assert_eq!(response.rows[0].tags, ["review"]);
        assert!(response.rows[0].bookmarked);
    }
    assert_eq!(
        resolve_count(),
        1,
        "annotation recount reuses data-only membership"
    );
    let reopened = ProjectsStore::new(fixture.root.0.clone()).unwrap();
    let response = query_project_rows(
        &reopened,
        QueryRowsPayload {
            project_id: fixture.id,
            spec,
            offset: Some(0),
            limit: Some(1),
        },
    )
    .unwrap();
    assert_eq!(response.annotation_revision, outside.revision);
    assert_eq!(response.rows[0].memo.as_deref(), Some("Note <row-0>"));
}

#[test]
fn annotation_dependent_membership_changes_after_each_cached_snapshot() {
    let _lock = LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new();
    fixture.annotate(0, "critical", true, &["review"]);
    for (row, spec) in [
        (
            2,
            QuerySpec {
                flag_filter: Some("critical".into()),
                ..Default::default()
            },
        ),
        (
            3,
            QuerySpec {
                bookmarked_only: Some(true),
                ..Default::default()
            },
        ),
        (
            4,
            QuerySpec {
                search: Some("@tag:review".into()),
                ..Default::default()
            },
        ),
    ] {
        let before = fixture.query(spec.clone());
        assert!(!row_ids(&before).contains(&row));
        let update = fixture.annotate(row, "critical", true, &["review"]);
        let after = fixture.query(spec.clone());
        assert_eq!(after.annotation_revision, update.revision);
        assert_eq!(after.total_filtered_rows, before.total_filtered_rows + 1);
        let ordered = row_ids(&after);
        assert!(ordered.contains(&row));
        let located = locate_project_row(
            &fixture.store,
            LocateRowPayload {
                project_id: fixture.id,
                row_index: row,
                spec,
            },
        )
        .unwrap();
        assert_eq!(located.position, ordered.iter().position(|id| *id == row));
    }
}

#[test]
fn filtered_location_matches_source_and_user_sort_orders_and_observes_cancellation() {
    let _lock = LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = Fixture::new();
    let key = common::id(&common::column_ids(&fixture.store, fixture.id), "key");
    for (sort_key, direction, expected) in [
        (None, None, vec![0, 2, 4, 5]),
        (Some(String::new()), None, vec![0, 2, 4, 5]),
        (Some(key.clone()), Some("asc".into()), vec![2, 5, 0, 4]),
        (Some(key), Some("desc".into()), vec![4, 0, 5, 2]),
    ] {
        let spec = QuerySpec {
            search: Some("keep".into()),
            sort_key,
            sort_direction: direction,
            ..Default::default()
        };
        assert_eq!(row_ids(&fixture.query(spec.clone())), expected);
        for row in [0, 1, 2, 4, 5, 6, usize::MAX] {
            let found = locate_project_row(
                &fixture.store,
                LocateRowPayload {
                    project_id: fixture.id,
                    row_index: row,
                    spec: spec.clone(),
                },
            )
            .unwrap();
            assert_eq!(found.position, expected.iter().position(|id| *id == row));
            assert_eq!((found.total_rows, found.total_filtered_rows), (6, 4));
        }
        let guard = queue_job(JobKind::Query, Some(fixture.id)).unwrap();
        let callback: ProgressFn = Arc::new(|progress| {
            request_cancel(progress.id);
        });
        let job = JobContext::with_job(guard, Some(callback));
        let error = locate_project_row_with_job(
            &fixture.store,
            LocateRowPayload {
                project_id: fixture.id,
                row_index: 4,
                spec,
            },
            &job,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "Cancelled.");
    }
}

#[test]
fn ascending_sparse_pages_and_arbitrary_duplicate_pages_keep_exact_values() {
    let _lock = LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let root = TempRoot::new();
    let path = root.0.join("data.parquet");
    let values = [
        Some("0007"),
        None,
        Some(""),
        Some("ΟΣ"),
        Some("18446744073709551615"),
        Some("<alice@example.org>"),
        Some("last"),
    ];
    let frame = DataFrame::new_infer_height(vec![
        Series::new("value".into(), values).into(),
        Series::new("seq".into(), (0..values.len() as u64).collect::<Vec<_>>()).into(),
    ])
    .unwrap();
    let mut writer = ParquetWriter::new(File::create(&path).unwrap())
        .batched(frame.schema())
        .unwrap();
    for offset in (0..frame.height()).step_by(2) {
        writer
            .write_batch(&frame.slice(offset as i64, 2.min(frame.height() - offset)))
            .unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    let columns = vec!["value".into(), "seq".into()];
    for rows in [vec![0, 1, 2, 4, 6], vec![6, 0, 6, 3, 1], vec![2], vec![]] {
        let page = read_page_rows_dataframe(&path, &columns, &rows).unwrap();
        assert_eq!(page.height(), rows.len());
        if !rows.is_empty() {
            assert_eq!(page.get_column_names(), ["value", "seq"]);
            for (index, row) in rows.into_iter().enumerate() {
                assert_eq!(
                    page.column("value").unwrap().str().unwrap().get(index),
                    values[row]
                );
                assert_eq!(
                    page.column("seq").unwrap().u64().unwrap().get(index),
                    Some(row as u64)
                );
            }
        }
    }
    assert!(read_page_rows_dataframe(&path, &columns, &[0, values.len()]).is_err());
    let guard = queue_job(JobKind::Query, None).unwrap();
    request_cancel(guard.id());
    let job = JobContext::with_job(guard, None);
    assert!(read_page_rows_dataframe_with_job(&path, &columns, &[0, 2, 4], &job).is_err());
}

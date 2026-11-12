//! Import profiling is reusable metadata, never an eager frame on case switch.

use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use chrono::Utc;
use polars::prelude::*;
use uuid::Uuid;

use trivium_engine::{
    jobs::JobContext,
    models::ProjectMeta,
    ops::projects::{
        create_project, load_column_kinds_with_job, load_project, load_project_metadata_with_job,
        CreateProjectPayload, ProjectRequest,
    },
    project_io::{project_full_reads, project_read_stats, reset_project_read_counters},
    storage::{load_setting, remove_setting, save_setting_bytes},
    store::ProjectsStore,
};

static LOCK: Mutex<()> = Mutex::new(());
fn guard() -> MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|error| error.into_inner())
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-profile-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn store(&self) -> ProjectsStore {
        ProjectsStore::new(self.0.clone()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn import(store: &ProjectsStore, path: &Path) -> ProjectMeta {
    create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap()
    .meta
}

#[test]
fn fresh_import_metadata_and_kinds_never_decode_rows() {
    let _guard = guard();
    let fixture = Fixture::new();
    let store = fixture.store();
    let path = fixture.0.join("evidence.csv");
    fs::write(
        &path,
        "message,id,time\nhello,18446744073709551615,2024-01-01T00:00:00.123456789Z\n",
    )
    .unwrap();
    let meta = import(&store, &path);
    let profile = load_setting(&store.project_dir(&meta.id), "column_profile")
        .unwrap()
        .unwrap();
    assert!(profile.get("signature").is_some());
    for _ in 0..2 {
        reset_project_read_counters();
        let loaded = load_project_metadata_with_job(
            &store,
            ProjectRequest {
                project_id: meta.id,
            },
            &JobContext::none(),
        )
        .unwrap();
        assert!(loaded.initial_rows.is_empty());
        assert_eq!(
            loaded.columns,
            meta.columns
                .iter()
                .map(|col| col.id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(loaded.column_kinds[&meta.columns[0].id], "string");
        assert_eq!(loaded.column_kinds[&meta.columns[1].id], "int");
        assert_eq!(loaded.column_kinds[&meta.columns[2].id], "datetime");
        assert_eq!(project_read_stats().decoded_rows, 0);
        assert_eq!(project_full_reads(), 0);
        assert_eq!(
            load_column_kinds_with_job(&store, meta.id, &JobContext::none()).unwrap(),
            loaded.column_kinds
        );
        assert_eq!(project_read_stats().decoded_rows, 0);
    }
}

#[test]
fn classifications_merge_across_real_import_batches() {
    let _guard = guard();
    let fixture = Fixture::new();
    let store = fixture.store();
    let path = fixture.0.join("mixed.csv");
    let mut writer = BufWriter::new(File::create(&path).unwrap());
    writeln!(writer, "numeric,mixed,empty,text").unwrap();
    for _ in 0..50_000 {
        writeln!(writer, "1,2024-01-01,,alpha").unwrap();
    }
    writeln!(writer, "1.5,2,42,3").unwrap();
    writer.flush().unwrap();
    drop(writer);
    let meta = import(&store, &path);
    reset_project_read_counters();
    let kinds = load_column_kinds_with_job(&store, meta.id, &JobContext::none()).unwrap();
    for (column, expected) in meta
        .columns
        .iter()
        .zip(["float", "string", "int", "string"])
    {
        assert_eq!(kinds[&column.id], expected);
    }
    assert_eq!(project_read_stats().decoded_rows, 0);
}

#[test]
fn legacy_profile_is_built_once_then_invalidated_by_data_signature() {
    let _guard = guard();
    let fixture = Fixture::new();
    let store = fixture.store();
    let path = fixture.0.join("legacy.csv");
    fs::write(&path, "value\n1\n2\n3\n").unwrap();
    let meta = import(&store, &path);
    let dir = store.project_dir(&meta.id);
    remove_setting(&dir, "column_profile").unwrap();
    reset_project_read_counters();
    let first = load_project_metadata_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(first.column_kinds[&meta.columns[0].id], "int");
    assert_eq!(project_read_stats().decoded_rows, 3);
    assert_eq!(project_full_reads(), 0);
    reset_project_read_counters();
    load_project_metadata_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(project_read_stats().decoded_rows, 0);

    // Different bytes, row count and schema content force the derived profile
    // to change even when the saved project id and column id stay unchanged.
    let mut replacement = DataFrame::new(vec![Series::new(
        &meta.columns[0].id,
        ["changed", "text", "new", "rows"],
    )])
    .unwrap();
    ParquetWriter::new(File::create(dir.join("data.parquet")).unwrap())
        .finish(&mut replacement)
        .unwrap();
    reset_project_read_counters();
    let changed = load_project_metadata_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(changed.column_kinds[&meta.columns[0].id], "string");
    assert_eq!(project_read_stats().decoded_rows, 4);
    reset_project_read_counters();
    load_project_metadata_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(project_read_stats().decoded_rows, 0);
}

#[test]
fn legacy_typed_initial_page_preserves_original_ids_nulls_and_nanoseconds() {
    let _guard = guard();
    let fixture = Fixture::new();
    let store = fixture.store();
    let id = Uuid::new_v4();
    let dir = store.project_dir(&id);
    fs::create_dir_all(&dir).unwrap();
    let mut frame = DataFrame::new(vec![
        Series::new(
            "number",
            (0..600).map(|row| u64::MAX - row).collect::<Vec<_>>(),
        ),
        Series::new(
            "time",
            (0..600)
                .map(|row| 1_700_000_000_123_456_789i64 + row)
                .collect::<Vec<_>>(),
        )
        .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
        .unwrap(),
        Series::new(
            "message",
            (0..600)
                .map(|row| {
                    if row == 500 {
                        "w".repeat(80)
                    } else {
                        "x".into()
                    }
                })
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "nullable",
            (0..600)
                .map(|row| if row % 2 == 0 { None } else { Some("") })
                .collect::<Vec<_>>(),
        ),
    ])
    .unwrap();
    ParquetWriter::new(File::create(dir.join("data.parquet")).unwrap())
        .with_row_group_size(Some(100))
        .finish(&mut frame)
        .unwrap();
    store
        .insert(ProjectMeta {
            id,
            name: "typed".into(),
            description: None,
            created_at: Utc::now(),
            total_records: 600,
            flagged_records: 0,
            ioc_applied_records: 0,
            source_sha256: None,
            source_file_name: None,
            source_format: None,
            source_size: None,
            source_path: None,
            hidden_columns: vec![],
            storage_version: 0,
            columns: vec![],
        })
        .unwrap();
    // The one-time migration is separate from the bounded compatibility page.
    load_project_metadata_with_job(
        &store,
        ProjectRequest { project_id: id },
        &JobContext::none(),
    )
    .unwrap();
    reset_project_read_counters();
    let loaded = load_project(&store, ProjectRequest { project_id: id }).unwrap();
    assert_eq!(loaded.initial_rows.len(), 250);
    assert_eq!(loaded.column_max_chars["message"], 80);
    for (index, row) in loaded.initial_rows.iter().enumerate() {
        assert_eq!(row.row_index, index);
        assert_eq!(
            row.cells["number"].text.as_deref(),
            Some((u64::MAX - index as u64).to_string().as_str())
        );
        if index % 2 == 0 {
            assert!(row.cells["nullable"].text.is_none());
        } else {
            assert_eq!(row.cells["nullable"].text.as_deref(), Some(""));
        }
        let expected = trivium_engine::value_utils::anyvalue_to_search_string(&AnyValue::Datetime(
            1_700_000_000_123_456_789 + index as i64,
            TimeUnit::Nanoseconds,
            &None,
        ))
        .unwrap();
        assert_eq!(row.cells["time"].text.as_deref(), Some(expected.as_str()));
    }
    assert_eq!(project_full_reads(), 0);
    assert_eq!(project_read_stats().decoded_row_groups, 3);
    assert!(project_read_stats().decoded_rows <= 300);
}

#[test]
fn malformed_derived_profile_or_width_json_rebuilds_without_blocking_evidence() {
    let _guard = guard();
    let fixture = Fixture::new();
    let store = fixture.store();
    let path = fixture.0.join("corrupt-cache.csv");
    fs::write(&path, "value\n1\n2\n").unwrap();
    let meta = import(&store, &path);
    let dir = store.project_dir(&meta.id);
    save_setting_bytes(&dir, "column_profile", b"{broken").unwrap();
    reset_project_read_counters();
    let loaded = load_project_metadata_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(loaded.column_kinds[&meta.columns[0].id], "int");
    assert_eq!(project_read_stats().decoded_rows, 2);
    assert!(load_setting(&dir, "column_profile").unwrap().is_some());
    remove_setting(&dir, "column_profile").unwrap();
    save_setting_bytes(&dir, "column_max_chars", b"{broken").unwrap();
    reset_project_read_counters();
    load_project_metadata_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(project_read_stats().decoded_rows, 2);
    reset_project_read_counters();
    load_project_metadata_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(project_read_stats().decoded_rows, 0);
}

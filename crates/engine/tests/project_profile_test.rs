//! Import profiling is reusable metadata, never an eager frame on case switch.

use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use polars::prelude::*;
use uuid::Uuid;

use trivium_engine::{
    jobs::JobContext,
    models::ProjectMeta,
    ops::projects::{
        create_project, load_column_kinds_with_job, load_project_with_job, CreateProjectPayload,
        ProjectRequest,
    },
    project_io::{project_full_reads, project_read_stats, reset_project_read_counters},
    storage::{
        load_flags, load_setting, load_setting_bytes, remove_setting, save_setting,
        save_setting_bytes,
    },
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
            import_options: None,
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
        let loaded = load_project_with_job(
            &store,
            ProjectRequest {
                project_id: meta.id,
            },
            &JobContext::none(),
        )
        .unwrap();
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
fn missing_profile_is_built_once_then_invalidated_by_data_signature() {
    let _guard = guard();
    let fixture = Fixture::new();
    let store = fixture.store();
    let path = fixture.0.join("profile.csv");
    fs::write(&path, "value\n1\n2\n3\n").unwrap();
    let meta = import(&store, &path);
    let dir = store.project_dir(&meta.id);
    remove_setting(&dir, "column_profile").unwrap();
    reset_project_read_counters();
    let first = load_project_with_job(
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
    load_project_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(project_read_stats().decoded_rows, 0);

    // Changed data invalidates the cached profile for the same column IDs.
    let values = ["changed", "text", "rows"];
    let mut replacement = DataFrame::new_infer_height(vec![
        Series::new(meta.columns[0].id.as_str().into(), values).into(),
        Series::new(
            "__original".into(),
            values.map(|value| serde_json::json!([value]).to_string()),
        )
        .into(),
        Series::new("__rowid".into(), [0_i64, 1, 2]).into(),
        Series::new("__trivium_source_record".into(), [2_i64, 3, 4]).into(),
    ])
    .unwrap();
    ParquetWriter::new(File::create(dir.join("data.parquet")).unwrap())
        .finish(&mut replacement)
        .unwrap();
    reset_project_read_counters();
    let changed = load_project_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(changed.column_kinds[&meta.columns[0].id], "string");
    assert_eq!(project_read_stats().decoded_rows, 3);
    reset_project_read_counters();
    load_project_with_job(
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
fn malformed_derived_profile_rebuilds_without_blocking_evidence() {
    let _guard = guard();
    let fixture = Fixture::new();
    let store = fixture.store();
    let path = fixture.0.join("corrupt-cache.csv");
    fs::write(&path, "value\n1\n2\n").unwrap();
    let meta = import(&store, &path);
    let dir = store.project_dir(&meta.id);
    save_setting_bytes(&dir, "column_profile", b"{broken").unwrap();
    reset_project_read_counters();
    let loaded = load_project_with_job(
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
    reset_project_read_counters();
    load_project_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(project_read_stats().decoded_rows, 2);
    reset_project_read_counters();
    load_project_with_job(
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
fn stale_parser_profile_refreshes_once_without_changing_widths_or_investigation() {
    let _guard = guard();
    let fixture = Fixture::new();
    let store = fixture.store();
    let path = fixture.0.join("newly-recognized.csv");
    let source = "Timestamp,message\n2026/1/2 3:04:05,keep\n2026/1/2 4:05:06,later\n";
    fs::write(&path, source).unwrap();
    let meta = import(&store, &path);
    let dir = store.project_dir(&meta.id);
    let timestamp = &meta.columns[0].id;
    let message = &meta.columns[1].id;

    let mut old = load_setting(&dir, "column_profile").unwrap().unwrap();
    let current_version = old["inference_version"].as_u64().unwrap();
    assert!(current_version > 0);
    old["inference_version"] = serde_json::json!(0);
    old["kinds"][timestamp] = serde_json::json!("string");
    old["widths"][timestamp] = serde_json::json!(123);
    old["widths"][message] = serde_json::json!(77);
    save_setting(&dir, "column_profile", &old).unwrap();

    trivium_engine::ops::flags::update_flag(
        &store,
        serde_json::from_value(serde_json::json!({
            "projectId": meta.id, "row_index": 0, "flag": "critical",
            "memo": "<CommandLine>whoami</CommandLine>",
            "tags": ["review"], "bookmarked": true,
        }))
        .unwrap(),
    )
    .unwrap();
    let annotations = load_flags(&store.database_path(&meta.id)).unwrap();
    let view = trivium_engine::models::ViewState {
        search: "keep".into(),
        column_widths: std::collections::HashMap::from([(timestamp.clone(), 321.0)]),
        ..Default::default()
    };
    save_setting(&dir, "view_state", &serde_json::to_value(view).unwrap()).unwrap();
    let view_bytes = load_setting_bytes(&dir, "view_state").unwrap();
    let parquet_before = fs::read(dir.join("data.parquet")).unwrap();

    reset_project_read_counters();
    let refreshed = load_project_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(refreshed.column_kinds[timestamp], "datetime");
    assert_eq!(refreshed.column_max_chars[timestamp], 123);
    assert_eq!(refreshed.column_max_chars[message], 77);
    assert_eq!(project_read_stats().decoded_rows, 2);
    assert_eq!(project_full_reads(), 0);
    let saved = load_setting(&dir, "column_profile").unwrap().unwrap();
    assert_eq!(saved["inference_version"].as_u64(), Some(current_version));
    assert_eq!(saved["kinds"][timestamp], "datetime");
    assert_eq!(
        load_flags(&store.database_path(&meta.id)).unwrap(),
        annotations
    );
    assert_eq!(load_setting_bytes(&dir, "view_state").unwrap(), view_bytes);
    assert_eq!(fs::read(dir.join("data.parquet")).unwrap(), parquet_before);
    assert_eq!(fs::read_to_string(&path).unwrap(), source);

    reset_project_read_counters();
    let cached = load_project_with_job(
        &store,
        ProjectRequest {
            project_id: meta.id,
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(cached.column_kinds, refreshed.column_kinds);
    assert_eq!(cached.column_max_chars, refreshed.column_max_chars);
    assert_eq!(project_read_stats().decoded_rows, 0);
    assert_eq!(project_full_reads(), 0);
}

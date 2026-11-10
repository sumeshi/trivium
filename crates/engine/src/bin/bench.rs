//! I4 engine benchmark. Generates no data itself; run against fixtures built
//! with `scripts/gen_fixture.py`:
//!
//! ```text
//! python3 scripts/gen_fixture.py --rows 100000 --seed 42 --out fixtures/bench-100k.csv
//! cargo run --release -p trivium-engine --bin bench -- fixtures/bench-100k.csv ...
//! ```
//!
//! Measures per fixture: import (create), first sort, full export, search
//! alternation (two conditions, cold then warm cache), IOC application with
//! 100 entries plus one more entry, and a single-row flag update.

use std::time::Instant;

use trivium_engine::error::AppError;
use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::iocs::{save_iocs, SaveIocsPayload};
use trivium_engine::ops::projects::{create_project, CreateProjectPayload};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload};
use trivium_engine::store::ProjectsStore;

/// Runs `job` once, prints the elapsed time, and returns its result.
fn timed<T, F: FnOnce() -> Result<T, AppError>>(label: &str, job: F) -> Result<T, AppError> {
    let started = Instant::now();
    let result = job()?;
    println!("  {:<22}: {:?}", label, started.elapsed());
    Ok(result)
}

fn run_fixture(fixture: &str) -> Result<(), AppError> {
    let root = std::env::temp_dir().join(format!("trivium-bench-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root)
        .map_err(|e| AppError::Message(format!("failed to create bench dir: {}", e)))?;
    let store = ProjectsStore::new(root.clone()).map_err(AppError::from)?;

    println!("fixture={}", fixture);

    let build_create = || CreateProjectPayload {
        path: fixture.to_string(),
        description: None,
        delimiter: None,
        restore_annotations: None,
    };
    let id = timed("create", || create_project(&store, build_create()))?.meta.id;
    println!(
        "  {:<22}: {} rows",
        "rows_imported",
        store.find(&id).map(|meta| meta.total_records).unwrap_or(0)
    );

    // Fixture headers are not data ids in the new storage format. Resolve the
    // ids from the project metadata before building queries.
    let columns = store.find(&id).map(|meta| meta.columns).unwrap_or_default();
    let column_id = |source: &str| -> String {
        columns
            .iter()
            .find(|column| column.source == source)
            .map(|column| column.id.clone())
            .unwrap_or_else(|| source.to_string())
    };
    let ts_id = column_id("ts");
    let user_id = column_id("user");
    let host_id = column_id("host");

    let build_query = |search: Option<String>, sort: Option<String>| QueryRowsPayload {
        project_id: id,
        spec: trivium_engine::ops::rows::QuerySpec {
            search,
            search_columns: None,
            flag_filter: None,
            bookmarked_only: None,
            sort_key: sort,
            sort_direction: Some("asc".to_string()),
            condition: None,
        },
        offset: None,
        limit: Some(100),
    };

    timed("sort_first", || {
        query_project_rows(&store, build_query(None, Some(ts_id.clone()))).map(|_| ())
    })?;

    timed("export_all", || {
        export_project(
            &store,
            ExportProjectPayload {
                project_id: id,
                destination: root.join("bench-export.csv").to_string_lossy().to_string(),
                scope: None,
                spec: Default::default(),
                columns: None,
            },
        )
    })?;

    timed("search_A_cold", || {
        query_project_rows(&store, build_query(Some(format!("{}:alice", user_id)), None)).map(|_| ())
    })?;
    timed("search_B_cold", || {
        query_project_rows(
            &store,
            build_query(Some("powershell.exe error|warn".to_string()), None),
        )
        .map(|_| ())
    })?;
    timed("search_A_warm", || {
        query_project_rows(&store, build_query(Some(format!("{}:alice", user_id)), None)).map(|_| ())
    })?;
    timed("search_B_warm", || {
        query_project_rows(
            &store,
            build_query(Some("powershell.exe error|warn".to_string()), None),
        )
        .map(|_| ())
    })?;

    // save_iocs replaces the whole set, so the "+1" run rebuilds the same 100
    // entries plus one new query rather than mutating the saved list.
    // `prepare_ioc_entries` sorts by tag, so the extra tag must sort after
    // every `iocN` tag for the "+1" run to be a real appended IOC.
    let build_entries = |extra: bool| {
        let mut entries: Vec<trivium_engine::models::IocEntry> = (0..100)
            .map(|index| trivium_engine::models::IocEntry {
                flag: "critical".to_string(),
                tag: format!("ioc{}", index),
                query: format!("{}:WS{:02} {}:alice", host_id, index % 50 + 1, user_id),
            })
            .collect();
        if extra {
            entries.push(trivium_engine::models::IocEntry {
                flag: "suspicious".to_string(),
                tag: "ioc_extra".to_string(),
                query: format!("{}:frank", user_id),
            });
        }
        entries
    };

    timed("ioc_100_save", || {
        save_iocs(&store, SaveIocsPayload {
            project_id: id,
            entries: build_entries(false),
        })
    })?;
    timed("ioc_100_query", || {
        query_project_rows(&store, build_query(None, None)).map(|_| ())
    })?;
    timed("ioc_1_more_save", || {
        save_iocs(&store, SaveIocsPayload {
            project_id: id,
            entries: build_entries(true),
        })
    })?;
    timed("ioc_1_more_query", || {
        query_project_rows(&store, build_query(None, None)).map(|_| ())
    })?;

    timed("flag_update", || {
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index: 0,
                flag: Some("safe".to_string()),
                memo: Some(Some("bench".to_string())),
                tags: None,
                bookmarked: None,
            },
        )
        .map(|_| ())
    })?;

    std::fs::remove_dir_all(&root).ok();
    Ok(())
}

fn main() {
    let fixtures: Vec<String> = std::env::args().skip(1).collect();
    if fixtures.is_empty() {
        eprintln!("usage: bench FIXTURE...");
        std::process::exit(2);
    }
    for fixture in &fixtures {
        if let Err(error) = run_fixture(fixture) {
            eprintln!("fixture {} failed: {}", fixture, error);
            std::process::exit(1);
        }
    }
}

//! T27 engine benchmark: import, reopen, page, search, annotation, export,
//! cancellation, cache, and peak-RSS measurements.
//!
//! Fixtures come from `scripts/gen_fixture.py`; run with release builds:
//!
//! ```text
//! python3 scripts/gen_fixture.py --rows 100000 --seed 42 --out fixtures/bench-100k.csv
//! python3 scripts/gen_fixture.py --rows 100000 --seed 42 --variant long \
//!     --out fixtures/bench-long-100k.csv
//! cargo run --release -p trivium-engine --bin bench -- fixtures/bench-100k.csv ...
//! ```
//!
//! Each fixture runs in a child process of the same binary so `VmHWM` belongs
//! to that fixture. "cold" means the first request in that process with an
//! empty result cache; the OS page cache is not dropped. Repeated short
//! operations report p50/p95 over at least 20 samples. Import and small export
//! report min/median/max only; p95 is not derived from 3 trials.

use std::fs;
use std::process::Command;
use std::time::Instant;

use trivium_engine::error::AppError;
use trivium_engine::jobs::{JobContext, JobKind};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::projects::{
    create_project, delete_project, load_project, CreateProjectPayload, ProjectRequest,
};
use trivium_engine::ops::rows::{
    clear_result_cache, query_project_rows_with_job, reset_resolve_count, resolve_count,
    result_cache_stats, QuerySpec,
};
use trivium_engine::project_io::{project_data_reads, reset_project_data_reads};
use trivium_engine::store::ProjectsStore;

#[path = "bench/support.rs"]
mod support;

use support::*;

fn run_fixture(fixture: &str) -> Result<(), AppError> {
    let root = std::env::temp_dir().join(format!("trivium-bench-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root)
        .map_err(|e| AppError::Message(format!("failed to create bench dir: {}", e)))?;
    let store = ProjectsStore::new(root.clone()).map_err(AppError::from)?;

    println!("fixture={}", fixture);
    println!(
        "  cold=first request in this process with an empty result cache; OS cache not dropped"
    );

    let build_create = || CreateProjectPayload {
        path: fixture.to_string(),
        description: None,
        delimiter: None,
        restore_annotations: None,
    };
    let mut imports = Samples::new("import");
    let mut last_id = None;
    for attempt in 0..IMPORT_RUNS {
        let summary = imports.record(|| create_project(&store, build_create()))?;
        if attempt + 1 < IMPORT_RUNS {
            delete_project(
                &store,
                ProjectRequest {
                    project_id: summary.meta.id,
                },
            )?;
        } else {
            last_id = Some(summary.meta.id);
        }
    }
    let id = last_id.ok_or_else(|| AppError::Message("import produced no project".into()))?;
    imports.report_low_trials();

    let meta = store
        .find(&id)
        .ok_or_else(|| AppError::Message("project missing after import".into()))?;
    let input_bytes = fs::metadata(fixture)
        .map_err(|e| AppError::Message(format!("failed to stat fixture: {}", e)))?
        .len();
    let parquet_bytes = fs::metadata(store.project_dir(&id).join("data.parquet"))
        .map(|m| m.len())
        .unwrap_or(0);
    out(
        "fixture_shape",
        format!(
            "rows={} columns={} input={} parquet={} avg_record_bytes={:.1}",
            meta.total_records,
            meta.columns.len(),
            mib(input_bytes as usize),
            mib(parquet_bytes as usize),
            input_bytes as f64 / meta.total_records.max(1) as f64
        ),
    );
    rss_line("rss_after_import");

    let column_id = |source: &str| -> String {
        meta.columns
            .iter()
            .find(|column| column.source == source)
            .map(|column| column.id.clone())
            .unwrap_or_else(|| source.to_string())
    };
    let ts_id = column_id("ts");
    let user_id = column_id("user");
    let host_id = column_id("host");

    // Reopening builds a new store over the same on-disk project, like an app
    // restart. The first iteration is the in-process cold one.
    let mut reopen = Samples::new("reopen");
    for _ in 0..ITERATIONS {
        let started = Instant::now();
        let fresh = ProjectsStore::new(root.clone()).map_err(AppError::from)?;
        load_project(&fresh, ProjectRequest { project_id: id })?;
        reopen.push(started.elapsed());
    }
    reopen.report();
    rss_line("rss_after_reopen");

    let store = ProjectsStore::new(root.clone()).map_err(AppError::from)?;
    clear_result_cache();
    once("page_first_cold", || {
        page(&store, id, None, None, 0).map(|_| ())
    })?;
    let mut page_first_warm = Samples::new("page_first_warm");
    measure(&mut page_first_warm, |_| {
        page(&store, id, None, None, 0).map(|_| ())
    })?;
    page_first_warm.report();

    reset_resolve_count();
    let mut page_move = Samples::new("page_move_warm");
    measure(&mut page_move, |index| {
        page(&store, id, None, None, PAGE_SIZE * (index + 1)).map(|_| ())
    })?;
    page_move.report();
    out(
        "page_move_check",
        format!("resolve_target_calls={}", resolve_count()),
    );

    clear_result_cache();
    reset_resolve_count();
    let sorted_cold = once("sort_page_cold", || page(&store, id, None, Some(&ts_id), 0))?;
    out(
        "sort_page_check",
        format!(
            "resolve_target_calls={} filtered_rows={}",
            resolve_count(),
            sorted_cold.total_filtered_rows
        ),
    );
    reset_resolve_count();
    let mut sort_warm = Samples::new("sort_page_warm");
    measure(&mut sort_warm, |index| {
        page(&store, id, None, Some(&ts_id), PAGE_SIZE * index).map(|_| ())
    })?;
    sort_warm.report();
    out(
        "sort_page_check_warm",
        format!(
            "resolve_target_calls={} (0 = search/sort not re-run)",
            resolve_count()
        ),
    );

    clear_result_cache();
    reset_resolve_count();
    let all_search = once("search_all_cold", || {
        page(&store, id, Some("powershell"), None, 0)
    })?;
    out(
        "search_all_check",
        format!(
            "filtered_rows={} resolve_target_calls={}",
            all_search.total_filtered_rows,
            resolve_count()
        ),
    );
    rss_line("rss_after_search_all");
    reset_resolve_count();
    let mut search_warm = Samples::new("search_same_warm");
    measure(&mut search_warm, |index| {
        page(&store, id, Some("powershell"), None, PAGE_SIZE * index).map(|_| ())
    })?;
    search_warm.report();
    out(
        "search_same_check",
        format!(
            "resolve_target_calls={} (0 = search not re-run)",
            resolve_count()
        ),
    );

    clear_result_cache();
    once("search_scoped_cold", || {
        page(&store, id, Some(&format!("{}:alice", user_id)), None, 0).map(|_| ())
    })?;

    reset_project_data_reads();
    let mut flag_save = Samples::new("flag_save");
    measure(&mut flag_save, |index| {
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index: index,
                flag: Some("safe".to_string()),
                memo: Some(Some(format!("bench-{}", index))),
                tags: None,
                bookmarked: None,
            },
        )
        .map(|_| ())
    })?;
    flag_save.report();
    out(
        "flag_save_check",
        format!(
            "project_data_reads={} (0 = no full original read)",
            project_data_reads()
        ),
    );

    let small_search = format!("{}:alice {}:WS01", user_id, host_id);
    let mut export_filtered = Samples::new("export_filtered");
    for _ in 0..EXPORT_RUNS {
        export_filtered.record(|| {
            export(
                &store,
                id,
                &root.join("bench-filtered.csv"),
                "filtered",
                QuerySpec {
                    search: Some(small_search.clone()),
                    ..QuerySpec::default()
                },
                None,
                &JobContext::none(),
            )
        })?;
    }
    export_filtered.report_low_trials();
    rss_line("rss_after_export");

    let row_indices: Vec<usize> = (0..100)
        .map(|index| (index * 1000) % meta.total_records.max(1))
        .collect();
    let mut export_selected = Samples::new("export_selected");
    for _ in 0..EXPORT_RUNS {
        export_selected.record(|| {
            export(
                &store,
                id,
                &root.join("bench-selected.csv"),
                "selected",
                QuerySpec::default(),
                Some(row_indices.clone()),
                &JobContext::none(),
            )
        })?;
    }
    export_selected.report_low_trials();

    // A pre-cancelled job returns at the first checkpoint; this is dispatch and
    // checkpoint latency, not a cancel that arrives mid-batch.
    let mut cancel_query = Samples::new("cancel_query_pre");
    for _ in 0..ITERATIONS {
        timed_cancel(&mut cancel_query, JobKind::Query, id, |context| {
            query_project_rows_with_job(&store, build_query(id, None, None, 0), context).map(|_| ())
        })?;
    }
    cancel_query.report();

    let mut cancel_export = Samples::new("cancel_export_pre");
    for _ in 0..ITERATIONS {
        timed_cancel(&mut cancel_export, JobKind::Export, id, |context| {
            export(
                &store,
                id,
                &root.join("bench-cancelled.csv"),
                "selected",
                QuerySpec::default(),
                Some(row_indices.clone()),
                context,
            )
        })?;
    }
    cancel_export.report();

    // Same sorted spec, three offsets: one resolve, then two cache hits.
    clear_result_cache();
    reset_resolve_count();
    for offset in [0, PAGE_SIZE, PAGE_SIZE * 2] {
        page(&store, id, None, Some(&ts_id), offset)?;
    }
    let stats = result_cache_stats();
    out(
        "result_cache_check",
        format!(
            "resolve_target_calls={} entries={} bytes={} max_entries={} max_bytes={}",
            resolve_count(),
            stats.entries,
            mib(stats.total_bytes),
            stats.max_entries,
            mib(stats.max_bytes)
        ),
    );

    let stats_before = result_cache_stats();
    delete_project(&store, ProjectRequest { project_id: id })?;
    let stats_after = result_cache_stats();
    out(
        "cache_release_after_close",
        format!(
            "cache_entries {} -> {} cache_bytes {} -> {}",
            stats_before.entries,
            stats_after.entries,
            mib(stats_before.total_bytes),
            mib(stats_after.total_bytes)
        ),
    );
    out("rss_before_close", format!("VmHWM={}", hwm_text()));
    rss_line("rss_after_close");

    fs::remove_dir_all(&root).ok();
    Ok(())
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--single") {
        args.remove(0);
        let Some(fixture) = args.first() else {
            eprintln!("usage: bench --single FIXTURE");
            std::process::exit(2);
        };
        if let Err(error) = run_fixture(fixture) {
            eprintln!("fixture {} failed: {}", fixture, error);
            std::process::exit(1);
        }
        return;
    }
    if args.is_empty() {
        eprintln!("usage: bench FIXTURE...");
        std::process::exit(2);
    }
    // One child per fixture, so VmHWM belongs to that fixture's import rather
    // than accumulating across the whole run.
    let executable = std::env::current_exe().unwrap_or_else(|_| "bench".into());
    for fixture in &args {
        let status = Command::new(&executable)
            .arg("--single")
            .arg(fixture)
            .status()
            .unwrap_or_else(|error| {
                eprintln!("failed to spawn bench child: {}", error);
                std::process::exit(1);
            });
        if !status.success() {
            std::process::exit(status.code().unwrap_or(1));
        }
    }
}

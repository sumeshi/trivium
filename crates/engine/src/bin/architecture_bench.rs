//! Reproducible before/after engine benchmark. Output is JSONL; fixture bytes
//! are shared between builds. Run each build serially after compilation.
use std::{fs, path::Path, process::Command, time::Instant};

use anyhow::{Context, Result};
use serde_json::json;
use trivium_engine::{
    ops::{
        export::{export_project, ExportProjectPayload},
        projects::{
            create_project, delete_project, load_project, CreateProjectPayload, ProjectRequest,
        },
        rows::{clear_result_cache, query_project_rows, QueryRowsPayload, QuerySpec},
        stats::{column_stats, ColumnStatsPayload},
    },
    project_io::{read_page_dataframe, read_page_rows_dataframe, read_project_schema_columns},
    store::ProjectsStore,
};
use uuid::Uuid;

#[path = "architecture_bench/instrumentation.rs"]
mod instrumentation;

const TRIALS: usize = 3;
const PAGE: usize = 250;

fn status_kib(name: &str) -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix(name)?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

fn measure<T>(label: &str, runs: usize, mut operation: impl FnMut() -> Result<T>) -> Result<T> {
    let mut samples = Vec::new();
    let mut counters = Vec::new();
    let mut result = None;
    for _ in 0..runs {
        instrumentation::reset();
        let start = Instant::now();
        result = Some(operation()?);
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        counters.push(instrumentation::snapshot());
    }
    let mut sorted = samples.clone();
    sorted.sort_by(f64::total_cmp);
    println!(
        "{}",
        json!({"operation": label, "n": runs, "milliseconds": samples,
        "min_ms": sorted[0], "median_ms": sorted[sorted.len()/2], "max_ms": sorted[sorted.len()-1],
        "peak_rss_kib": status_kib("VmHWM:"), "rss_kib": status_kib("VmRSS:"), "io": counters})
    );
    result.context("benchmark did not run")
}

fn query(store: &ProjectsStore, id: Uuid, spec: QuerySpec, offset: usize) -> Result<()> {
    let result = query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec,
            offset: Some(offset),
            limit: Some(PAGE),
        },
    )?;
    assert!(result.rows.len() <= PAGE);
    Ok(())
}

fn run_fixture(fixture: &str) -> Result<()> {
    let root = std::env::temp_dir().join(format!("trivium-architecture-bench-{}", Uuid::new_v4()));
    fs::create_dir_all(&root)?;
    let store = ProjectsStore::new(root.clone())?;
    println!(
        "{}",
        json!({"build_label":std::env::var("TRIVIUM_BENCH_LABEL").unwrap_or_else(|_| "current".into()),
        "fixture":fixture,"input_bytes":fs::metadata(fixture)?.len(),
        "os_cache":"not dropped; generated/warm", "trials":TRIALS,
        "rss":"cumulative process VmHWM per fixture, engine only", "io_kind":instrumentation::kind()})
    );
    let mut imported_ids = Vec::new();
    let meta = measure("import", TRIALS, || {
        let summary = create_project(
            &store,
            CreateProjectPayload {
                path: fixture.to_string(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )?;
        imported_ids.push(summary.meta.id);
        Ok(summary.meta)
    })?;
    let id = meta.id;
    for imported_id in imported_ids.into_iter().filter(|saved| *saved != id) {
        delete_project(
            &store,
            ProjectRequest {
                project_id: imported_id,
            },
        )?;
    }
    let path = store.project_dir(&id).join("data.parquet");
    let schema = read_project_schema_columns(&path)?;
    println!(
        "{}",
        json!({"rows":meta.total_records,"columns":meta.columns.len(),
        "parquet_bytes":fs::metadata(&path)?.len()})
    );
    let column = |source: &str| -> String {
        meta.columns
            .iter()
            .find(|column| column.source == source)
            .unwrap()
            .id
            .clone()
    };
    measure("reopen_compat_250", TRIALS, || {
        let fresh = ProjectsStore::new(root.clone())?;
        let response = load_project(&fresh, ProjectRequest { project_id: id })?;
        assert_eq!(response.initial_rows.len(), PAGE.min(meta.total_records));
        Ok(())
    })?;
    measure("reopen_metadata", TRIALS, || {
        let fresh = ProjectsStore::new(root.clone())?;
        instrumentation::metadata_open(&fresh, id)
    })?;
    for (name, offset) in [
        ("page_first", 0),
        ("page_middle", meta.total_records / 2),
        ("page_last", meta.total_records.saturating_sub(PAGE)),
    ] {
        measure(name, TRIALS, || {
            let frame = read_page_dataframe(&path, &schema, offset, PAGE)?;
            assert_eq!(frame.height(), PAGE.min(meta.total_records));
            Ok(())
        })?;
    }
    let sparse = [
        meta.total_records - 1,
        0,
        meta.total_records / 2,
        meta.total_records - 1,
    ];
    measure("page_sparse", TRIALS, || {
        let frame = read_page_rows_dataframe(&path, &schema, &sparse)?;
        assert_eq!(frame.height(), sparse.len());
        Ok(())
    })?;
    let spec = QuerySpec {
        search: Some(format!("{}:alice", column("user"))),
        ..QuerySpec::default()
    };
    measure("query_scoped_cold", TRIALS, || {
        clear_result_cache();
        query(&store, id, spec.clone(), 0)
    })?;
    measure("query_scoped_cached", TRIALS, || {
        query(&store, id, spec.clone(), 0)
    })?;
    measure("stats_filtered", TRIALS, || {
        let result = column_stats(
            &store,
            ColumnStatsPayload {
                project_id: id,
                column: column("host"),
                rare_max: 1,
                spec: spec.clone(),
            },
        )?;
        Ok(result)
    })?;
    let sort = QuerySpec {
        sort_key: Some(column("ts")),
        sort_direction: Some("desc".into()),
        ..QuerySpec::default()
    };
    measure("export_sorted_all", TRIALS, || {
        clear_result_cache();
        let result = export_project(
            &store,
            ExportProjectPayload {
                project_id: id,
                destination: root.join("sorted.csv").to_string_lossy().to_string(),
                scope: Some("filtered".into()),
                spec: sort.clone(),
                columns: None,
                row_indices: None,
                include_annotations: Some(true),
            },
        )?;
        Ok(result)
    })?;
    println!(
        "{}",
        json!({"operation":"complete", "peak_rss_kib":status_kib("VmHWM:")})
    );
    delete_project(&store, ProjectRequest { project_id: id })?;
    fs::remove_dir_all(root)?;
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--single") {
        return run_fixture(args.get(1).context("missing fixture")?);
    }
    anyhow::ensure!(!args.is_empty(), "usage: architecture_bench FIXTURE...");
    for fixture in args {
        let status = Command::new(std::env::current_exe()?)
            .arg("--single")
            .arg(Path::new(&fixture))
            .status()?;
        anyhow::ensure!(status.success(), "fixture child failed: {status}");
    }
    Ok(())
}

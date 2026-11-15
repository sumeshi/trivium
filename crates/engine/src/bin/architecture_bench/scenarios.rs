//! Extra workloads share the existing timer and reader counters. Search children
//! open an already imported project so their RSS high-water mark excludes import.
use std::{
    path::Path,
    process::Command,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use serde_json::json;
use trivium_engine::{
    annotation::AnnotationPatch,
    jobs::{request_cancel, try_begin_job, JobContext, JobKind},
    models::ProjectMeta,
    ops::{
        bulk::{bulk_preview, bulk_update, BulkAnnotatePayload},
        flags::{update_flag, UpdateFlagPayload},
        projects::{load_project_metadata_with_job, ProjectRequest},
        rows::{
            clear_result_cache, query_project_rows, query_project_rows_with_job, QueryRowsPayload,
            QueryRowsResponse, QuerySpec,
        },
    },
    store::ProjectsStore,
};
use uuid::Uuid;

use super::{measure, status_kib, trials, PAGE};

pub(super) fn source_column(meta: &ProjectMeta, source: &str) -> Result<String> {
    let pointer = format!("/{source}");
    meta.columns
        .iter()
        .find(|column| column.source == source || column.source == pointer)
        .or_else(|| meta.columns.iter().find(|column| column.label == source))
        .map(|column| column.id.clone())
        .with_context(|| format!("fixture has no {source} column"))
}

fn payload(id: Uuid, spec: QuerySpec, offset: usize) -> QueryRowsPayload {
    QueryRowsPayload {
        project_id: id,
        spec,
        offset: Some(offset),
        limit: Some(PAGE),
    }
}

fn page_details(label: &str, response: &QueryRowsResponse) -> Result<()> {
    // Encoding is outside the engine operation timer. This is the real reply
    // shape, including annotations, rather than an estimate from Parquet size.
    let start = Instant::now();
    let bytes = serde_json::to_vec(response)?.len();
    println!(
        "{}",
        json!({"page_details":label, "returned_rows":response.rows.len(),
        "total_filtered_rows":response.total_filtered_rows, "offset":response.offset,
        "json_bytes":bytes, "encode_ms":start.elapsed().as_secs_f64()*1000.0})
    );
    Ok(())
}

pub fn isolated_searches(root: &Path, id: Uuid) -> Result<()> {
    for mode in ["scoped", "all", "cancel"] {
        let status = Command::new(std::env::current_exe()?)
            .arg("--search")
            .arg(root)
            .arg(id.to_string())
            .arg(mode)
            .status()?;
        anyhow::ensure!(status.success(), "isolated search child failed: {status}");
    }
    Ok(())
}

pub fn search_child(root: &Path, id: Uuid, mode: &str) -> Result<()> {
    let store = ProjectsStore::new(root.to_path_buf())?;
    let meta = store.try_find(&id)?.context("project not found")?;
    let mut spec = QuerySpec {
        search: Some("alice".into()),
        ..QuerySpec::default()
    };
    if mode == "scoped" {
        spec.search_columns = Some(vec![source_column(&meta, "user")?]);
    }
    anyhow::ensure!(
        ["scoped", "all", "cancel"].contains(&mode),
        "unknown search mode"
    );
    println!(
        "{}",
        json!({"isolated_search":mode, "rss_before_kib":status_kib("VmRSS:"),
        "rss_scope":"fresh process, metadata + cold query + first page; import excluded"})
    );
    if mode == "cancel" {
        return cancel_search(&store, id, spec);
    }
    let label = format!("query_{mode}_cold_isolated");
    let response = measure(&label, trials(), || {
        clear_result_cache();
        query_project_rows(&store, payload(id, spec.clone(), 0)).map_err(Into::into)
    })?;
    page_details(&label, &response)
}

fn cancel_search(store: &ProjectsStore, id: Uuid, spec: QuerySpec) -> Result<()> {
    let delay = std::env::var("TRIVIUM_BENCH_CANCEL_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20);
    let mut outcomes = Vec::new();
    measure("query_cancel_delayed", trials(), || {
        clear_result_cache();
        let guard = try_begin_job(JobKind::Query, Some(id))?;
        let job_id = guard.id();
        let context = JobContext::with_job(guard, None);
        let timer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(delay));
            let requested_at = Instant::now();
            (requested_at, request_cancel(job_id))
        });
        let result = query_project_rows_with_job(store, payload(id, spec.clone(), 0), &context);
        let completed_at = Instant::now();
        let (requested_at, accepted) = timer.join().expect("cancellation timer panicked");
        let cancelled = result
            .as_ref()
            .is_err_and(|error| error.to_string() == "Cancelled.");
        if let Err(error) = result {
            anyhow::ensure!(cancelled, "query failed: {error}");
        }
        outcomes.push(json!({"cancelled":cancelled, "request_accepted":accepted,
            "completed_before_request":completed_at < requested_at,
            "cancel_latency_ms":completed_at.checked_duration_since(requested_at)
                .map(|duration| duration.as_secs_f64()*1000.0)}));
        Ok(())
    })?;
    println!(
        "{}",
        json!({"cancellation_details":outcomes, "request_after_ms":delay,
        "note":"a query finishing before the request is recorded as completed, not cancelled"})
    );
    Ok(())
}

pub fn sorted_pages(store: &ProjectsStore, id: Uuid, meta: &ProjectMeta) -> Result<()> {
    let (source, distribution) = if source_column(meta, "sort_key").is_ok() {
        (
            "sort_key",
            "deterministic unique permutation, unrelated to source order",
        )
    } else {
        (
            "EventID",
            "event-id groups, source-row ties; use --shuffle-sort for scattered pages",
        )
    };
    let spec = QuerySpec {
        sort_key: Some(source_column(meta, source)?),
        sort_direction: Some("asc".into()),
        ..QuerySpec::default()
    };
    println!(
        "{}",
        json!({"sorted_page_workload":distribution, "sort_source":source,
        "result_cache":"primed once; each timed page uses a new reader"})
    );
    clear_result_cache();
    query_project_rows(store, payload(id, spec.clone(), 0))?;
    for (label, offset) in [
        ("page_sorted_first", 0),
        ("page_sorted_middle", meta.total_records / 2),
        ("page_sorted_last", meta.total_records.saturating_sub(PAGE)),
    ] {
        let response = measure(label, trials(), || {
            query_project_rows(store, payload(id, spec.clone(), offset)).map_err(Into::into)
        })?;
        page_details(label, &response)?;
    }
    Ok(())
}

pub fn annotated_project(
    store: &ProjectsStore,
    id: Uuid,
    meta: &ProjectMeta,
    root: &Path,
) -> Result<()> {
    let count = std::env::var("TRIVIUM_BENCH_ANNOTATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000usize)
        .min(meta.total_records);
    if count == 0 {
        return Ok(());
    }
    let ids: Vec<usize> = (0..count)
        .map(|index| index * meta.total_records / count)
        .collect();
    // Set up in one native bulk transaction, outside update/recount timings.
    let mut bulk = BulkAnnotatePayload {
        project_id: id,
        target: "selection".into(),
        row_indices: Some(ids),
        spec: QuerySpec::default(),
        patch: AnnotationPatch {
            flag: Some("suspicious".into()),
            memo: Some(Some("Synthetic benchmark note".into())),
            tags: Some(vec!["benchmark".into(), "review".into()]),
            ..AnnotationPatch::default()
        },
        preview_token: None,
    };
    bulk.preview_token = Some(bulk_preview(store, bulk.clone())?.preview_token);
    bulk_update(store, bulk)?;
    println!(
        "{}",
        json!({"annotation_workload":count,
        "note":"one bulk setup outside timers; flags, notes and two tags per row"})
    );
    let mut critical = false;
    measure("annotation_update_dense", trials(), || {
        critical = !critical;
        let result = update_flag(
            store,
            UpdateFlagPayload {
                project_id: id,
                row_index: 0,
                flag: Some(if critical { "critical" } else { "suspicious" }.into()),
                memo: None,
                tags: None,
                bookmarked: None,
            },
        )?;
        anyhow::ensure!(result.flagged_records == count, "annotation count drift");
        Ok(())
    })?;
    let spec = QuerySpec {
        search: Some("alice".into()),
        ..QuerySpec::default()
    };
    // Prime a data-only result, then change annotations. A cached page has to
    // refresh counts at the new revision without changing membership.
    query_project_rows(store, payload(id, spec.clone(), 0))?;
    let response = measure("query_annotation_recount", trials(), || {
        critical = !critical;
        update_flag(
            store,
            UpdateFlagPayload {
                project_id: id,
                row_index: 0,
                flag: Some(if critical { "critical" } else { "" }.into()),
                memo: None,
                tags: None,
                bookmarked: None,
            },
        )?;
        query_project_rows(store, payload(id, spec.clone(), 0)).map_err(Into::into)
    })?;
    page_details("query_annotation_recount", &response)?;
    measure("reopen_annotated_metadata", trials(), || {
        let fresh = ProjectsStore::new(root.to_path_buf())?;
        load_project_metadata_with_job(
            &fresh,
            ProjectRequest { project_id: id },
            &JobContext::none(),
        )
        .map_err(Into::into)
    })?;
    // The engine has no tab UI; this measures the metadata operation used on a
    // switch, while real WebView tab timing remains the native UI benchmark.
    Ok(())
}

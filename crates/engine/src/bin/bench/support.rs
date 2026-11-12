//! Shared helpers for the T27 benchmark binary. Split out so each file stays
//! under the 500-line Rust size guard.

use std::fs;
use std::time::{Duration, Instant};

use trivium_engine::error::AppError;
use trivium_engine::jobs::{request_cancel, try_begin_job, JobContext, JobKind};
use trivium_engine::ops::export::{export_project_with_job, ExportProjectPayload};
use trivium_engine::ops::rows::{
    query_project_rows, QueryRowsPayload, QueryRowsResponse, QuerySpec,
};
use trivium_engine::store::ProjectsStore;

pub const ITERATIONS: usize = 20;
pub const IMPORT_RUNS: usize = 3;
pub const EXPORT_RUNS: usize = 3;
pub const PAGE_SIZE: usize = 250;

pub struct Samples {
    label: &'static str,
    values: Vec<Duration>,
}

impl Samples {
    pub fn new(label: &'static str) -> Self {
        Self {
            label,
            values: Vec::new(),
        }
    }

    pub fn record<T>(&mut self, job: impl FnOnce() -> Result<T, AppError>) -> Result<T, AppError> {
        let started = Instant::now();
        let result = job()?;
        self.values.push(started.elapsed());
        Ok(result)
    }

    pub fn push(&mut self, value: Duration) {
        self.values.push(value);
    }

    fn sorted(&self) -> Vec<Duration> {
        let mut values = self.values.clone();
        values.sort_unstable();
        values
    }

    /// Nearest-rank percentile; only meaningful with enough samples.
    fn percentile(&self, fraction: f64) -> Duration {
        let sorted = self.sorted();
        let rank = (((fraction * sorted.len() as f64).ceil() as usize).saturating_sub(1))
            .min(sorted.len().saturating_sub(1));
        sorted[rank]
    }

    pub fn report(&self) {
        let sorted = self.sorted();
        out(
            self.label,
            format!(
                "n={} min={} p50={} p95={} max={}",
                sorted.len(),
                ms(sorted[0]),
                ms(self.percentile(0.50)),
                ms(self.percentile(0.95)),
                ms(sorted[sorted.len() - 1])
            ),
        );
    }

    /// High-cost operations: trial count and min/median/max, never a p95.
    pub fn report_low_trials(&self) {
        let sorted = self.sorted();
        out(
            self.label,
            format!(
                "n={} min={} median={} max={} (p95 not reported)",
                sorted.len(),
                ms(sorted[0]),
                ms(sorted[sorted.len() / 2]),
                ms(sorted[sorted.len() - 1])
            ),
        );
    }
}

pub fn out(label: &str, text: String) {
    println!("  {:<24} {}", label, text);
}

pub fn ms(duration: Duration) -> String {
    let micros = duration.as_secs_f64() * 1_000_000.0;
    if micros < 1_000.0 {
        format!("{:.3}us", micros)
    } else {
        format!("{:.3}ms", micros / 1_000.0)
    }
}

pub fn once<T>(label: &str, job: impl FnOnce() -> Result<T, AppError>) -> Result<T, AppError> {
    let started = Instant::now();
    let result = job()?;
    out(label, format!("n=1 {}", ms(started.elapsed())));
    Ok(result)
}

fn status_kib(key: &str) -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix(key)
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|value| value.parse().ok())
    })
}

pub fn rss_line(label: &str) {
    let mib = |kib: Option<u64>| {
        kib.map(|value| format!("{:.1}MiB", value as f64 / 1024.0))
            .unwrap_or_else(|| "n/a".to_string())
    };
    out(
        label,
        format!(
            "VmHWM={} VmRSS={}",
            mib(status_kib("VmHWM:")),
            mib(status_kib("VmRSS:"))
        ),
    );
}

pub fn mib(bytes: usize) -> String {
    format!("{:.3}MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Current process peak (VmHWM) as MiB text.
pub fn hwm_text() -> String {
    status_kib("VmHWM:")
        .map(|kib| format!("{:.1}MiB", kib as f64 / 1024.0))
        .unwrap_or_else(|| "n/a".to_string())
}

pub fn build_query(
    project_id: uuid::Uuid,
    search: Option<String>,
    sort: Option<String>,
    offset: usize,
) -> QueryRowsPayload {
    QueryRowsPayload {
        project_id,
        spec: QuerySpec {
            search,
            search_columns: None,
            condition: None,
            flag_filter: None,
            bookmarked_only: None,
            sort_key: sort,
            sort_direction: Some("asc".to_string()),
        },
        offset: Some(offset),
        limit: Some(PAGE_SIZE),
    }
}

pub fn page(
    store: &ProjectsStore,
    id: uuid::Uuid,
    search: Option<&str>,
    sort: Option<&str>,
    offset: usize,
) -> Result<QueryRowsResponse, AppError> {
    query_project_rows(
        store,
        build_query(
            id,
            search.map(str::to_string),
            sort.map(str::to_string),
            offset,
        ),
    )
}

pub fn measure(
    samples: &mut Samples,
    mut job: impl FnMut(usize) -> Result<(), AppError>,
) -> Result<(), AppError> {
    for index in 0..ITERATIONS {
        samples.record(|| job(index))?;
    }
    Ok(())
}

pub fn export(
    store: &ProjectsStore,
    id: uuid::Uuid,
    destination: &std::path::Path,
    scope: &str,
    spec: QuerySpec,
    rows: Option<Vec<usize>>,
    context: &JobContext,
) -> Result<(), AppError> {
    export_project_with_job(
        store,
        ExportProjectPayload {
            project_id: id,
            destination: destination.to_string_lossy().to_string(),
            scope: Some(scope.to_string()),
            spec,
            columns: None,
            row_indices: rows,
            include_annotations: Some(true),
        },
        context,
    )
    .map(|_| ())
}

pub fn timed_cancel(
    samples: &mut Samples,
    kind: JobKind,
    id: uuid::Uuid,
    mut job: impl FnMut(&JobContext) -> Result<(), AppError>,
) -> Result<(), AppError> {
    let guard = try_begin_job(kind, Some(id))?;
    request_cancel(guard.id());
    let context = JobContext::with_job(guard, None);
    let started = Instant::now();
    if job(&context).is_ok() {
        return Err(AppError::Message("cancelled job returned Ok".into()));
    }
    samples.push(started.elapsed());
    Ok(())
}

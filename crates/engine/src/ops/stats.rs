use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::stats::{ColumnStats, StatsAccumulator};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::store::ProjectsStore;

use super::analysis_data::{AnalysisSource, SelectedRows};
use super::query::QuerySpec;

#[derive(Debug, Deserialize)]
pub struct ColumnStatsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub column: String,
    #[serde(default = "default_rare_max", rename = "rareMax")]
    pub rare_max: u64,
    /// Applied QuerySpec. Stats describe the current result, not the whole
    /// project, when a search/filter is active.
    #[serde(flatten)]
    pub spec: QuerySpec,
}

fn default_rare_max() -> u64 {
    1
}

/// Column kind, missing count, distinct count, top 20 values, and up to 20 rare values.
pub fn column_stats(
    store: &ProjectsStore,
    payload: ColumnStatsPayload,
) -> Result<ColumnStats, AppError> {
    column_stats_with_job(store, payload, &JobContext::none())
}

pub fn column_stats_with_job(
    store: &ProjectsStore,
    payload: ColumnStatsPayload,
    job: &JobContext,
) -> Result<ColumnStats, AppError> {
    let source = AnalysisSource::open(store, payload.project_id, &payload.spec, job)?;
    let projection = source.columns(&[payload.column], "statistics column")?;
    let mut reader = source.selected_reader(&projection)?;
    let empty = reader.empty_frame().map_err(AppError::from)?;
    let dtype = empty
        .column(&projection[0])
        .map_err(|error| AppError::Other(error.into()))?
        .dtype();
    let mut stats = StatsAccumulator::new(&projection[0], dtype);
    let selected = SelectedRows::new(source.target.total_rows, &source.target.ids);
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        let series = batch
            .frame
            .column(&projection[0])
            .map_err(|error| AppError::Other(error.into()))?;
        stats.observe(
            series,
            (0..batch.frame.height()).filter(|row| selected.contains(batch.offset + row)),
            job,
        )?;
    }
    stats.finish(payload.rare_max, job)
}

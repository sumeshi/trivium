use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::stats::{ColumnStats, StatsAccumulator, StatsLimits};
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
    column_stats_with_limits(store, payload, job, StatsLimits::default())
}

fn column_stats_with_limits(
    store: &ProjectsStore,
    payload: ColumnStatsPayload,
    job: &JobContext,
    limits: StatsLimits,
) -> Result<ColumnStats, AppError> {
    let source = AnalysisSource::open(store, payload.project_id, &payload.spec, job)?;
    let projection = source.columns(&[payload.column], "statistics column")?;
    let mut reader = source.selected_reader(&projection)?;
    let empty = reader.empty_frame().map_err(AppError::from)?;
    let dtype = empty
        .column(&projection[0])
        .map_err(|error| AppError::Other(error.into()))?
        .dtype();
    let mut stats = StatsAccumulator::with_limits(&projection[0], dtype, limits);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::projects::{create_project, CreateProjectPayload};
    #[test]
    fn exact_stats_fail_as_a_whole_and_narrowing_filters_recovers() {
        let root = std::env::temp_dir().join(format!("trivium-stats-budget-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("values.csv");
        std::fs::write(&path, "value,scope\na,keep\nb,keep\nc,drop\nd,drop\n").unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let meta = create_project(
            &store,
            CreateProjectPayload {
                import_options: None,
                path: path.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta;
        let value = meta
            .columns
            .iter()
            .find(|column| column.source == "value")
            .unwrap()
            .id
            .clone();
        let scope = meta
            .columns
            .iter()
            .find(|column| column.source == "scope")
            .unwrap()
            .id
            .clone();
        let payload = |search| ColumnStatsPayload {
            project_id: meta.id,
            column: value.clone(),
            rare_max: 1,
            spec: QuerySpec {
                search,
                search_columns: Some(vec![scope.clone()]),
                ..Default::default()
            },
        };
        let limits = StatsLimits {
            distinct: 2,
            bytes: 4096,
        };
        let error = column_stats_with_limits(&store, payload(None), &JobContext::none(), limits)
            .unwrap_err();
        assert!(error.to_string().contains("Narrow the current filters"));
        let stats = column_stats_with_limits(
            &store,
            payload(Some("keep".into())),
            &JobContext::none(),
            limits,
        )
        .unwrap();
        assert_eq!(stats.unique, 2);
        assert_eq!(stats.top.iter().map(|item| item.count).sum::<u64>(), 2);
        assert_eq!(
            stats
                .top
                .iter()
                .map(|item| item.value.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}

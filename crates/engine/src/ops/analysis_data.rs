//! Shared immutable-data/query snapshot for analysis and export entry points.

use std::path::PathBuf;

use polars::prelude::DataFrame;
use uuid::Uuid;

use crate::{
    error::AppError,
    jobs::JobContext,
    models::ProjectMeta,
    project_io::{read_project_columns_with_job, ProjectBatchReader},
    store::ProjectsStore,
};

use super::query::{resolve_project_target, QuerySpec, ResolvedProjectTarget};

pub(crate) struct AnalysisSource {
    pub meta: ProjectMeta,
    pub path: PathBuf,
    pub target: ResolvedProjectTarget,
}

impl AnalysisSource {
    pub fn open(
        store: &ProjectsStore,
        project_id: Uuid,
        spec: &QuerySpec,
        job: &JobContext,
    ) -> Result<Self, AppError> {
        job.check_cancelled()?;
        let meta = store
            .try_find(&project_id)
            .map_err(AppError::from)?
            .ok_or_else(|| AppError::Message("Project not found.".into()))?;
        let target = resolve_project_target(store, project_id, spec, job)?;
        Ok(Self {
            path: store.project_dir(&project_id).join("data.parquet"),
            meta,
            target,
        })
    }

    pub fn columns(&self, requested: &[String], label: &str) -> Result<Vec<String>, AppError> {
        requested
            .iter()
            .map(|requested| {
                self.target
                    .columns
                    .iter()
                    .find(|column| column.eq_ignore_ascii_case(requested))
                    .cloned()
                    .ok_or_else(|| {
                        AppError::Message(format!(
                            "Column '{}' not found in project {}.",
                            requested, label
                        ))
                    })
            })
            .collect()
    }

    /// Materialize only the analysis inputs, never unrelated data columns.
    pub fn read_columns(
        &self,
        columns: &[String],
        job: &JobContext,
    ) -> Result<DataFrame, AppError> {
        job.check_cancelled()?;
        let frame =
            read_project_columns_with_job(&self.path, columns, job).map_err(AppError::from)?;
        job.check_cancelled()?;
        Ok(frame)
    }

    pub fn selected_reader(&self, columns: &[String]) -> Result<ProjectBatchReader, AppError> {
        let mut projection = Vec::new();
        for column in columns {
            if !projection.contains(column) {
                projection.push(column.clone());
            }
        }
        let columns = &projection;
        let reader = if self.target.ids.len() == self.target.total_rows {
            ProjectBatchReader::open(&self.path, columns)
        } else {
            ProjectBatchReader::for_rows(&self.path, columns, &self.target.ids)
        };
        reader.map_err(AppError::from)
    }
}

/// A compact membership bitmap, independent of the result's display order.
/// No bitmap is allocated when the result includes every source row.
pub(crate) struct SelectedRows {
    words: Option<Vec<u64>>,
    total: usize,
}

impl SelectedRows {
    pub fn new(total: usize, ids: &[usize]) -> Self {
        let words = (ids.len() != total).then(|| {
            let mut words = vec![0u64; total.div_ceil(64)];
            for &id in ids {
                if id < total {
                    words[id / 64] |= 1 << (id % 64);
                }
            }
            words
        });
        Self { words, total }
    }

    pub fn contains(&self, id: usize) -> bool {
        id < self.total
            && self
                .words
                .as_ref()
                .is_none_or(|words| words[id / 64] & (1 << (id % 64)) != 0)
    }
}

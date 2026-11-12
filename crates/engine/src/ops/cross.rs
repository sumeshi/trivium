use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::ProjectRow;
use crate::store::ProjectsStore;

use super::rows::{query_project_rows_with_job, QueryRowsPayload, QuerySpec};

#[derive(Debug, Deserialize)]
pub struct CrossSearchPayload {
    #[serde(rename = "projectIds")]
    pub project_ids: Vec<Uuid>,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(rename = "sortKey", default)]
    pub sort_key: Option<String>,
    #[serde(rename = "sortDirection", default)]
    pub sort_direction: Option<String>,
    #[serde(default)]
    pub condition: Option<crate::condition::Node>,
}

#[derive(Debug, Serialize)]
pub struct CrossSearchHit {
    pub project_id: Uuid,
    pub total_filtered_rows: usize,
    pub rows: Vec<ProjectRow>,
}

/// Runs one query against each project. Hit counts match `query_project_rows`.
pub fn search_projects(
    store: &ProjectsStore,
    payload: CrossSearchPayload,
) -> Result<Vec<CrossSearchHit>, AppError> {
    search_projects_with_job(store, payload, &JobContext::none())
}

pub fn search_projects_with_job(
    store: &ProjectsStore,
    payload: CrossSearchPayload,
    job: &JobContext,
) -> Result<Vec<CrossSearchHit>, AppError> {
    job.check_cancelled()?;
    let mut hits = Vec::with_capacity(payload.project_ids.len());
    for project_id in payload.project_ids {
        job.check_cancelled()?;
        let response = query_project_rows_with_job(
            store,
            QueryRowsPayload {
                project_id,
                spec: QuerySpec {
                    search: payload.search.clone(),
                    search_columns: payload.columns.clone(),
                    flag_filter: payload.flag_filter.clone(),
                    bookmarked_only: None,
                    sort_key: payload.sort_key.clone(),
                    sort_direction: payload.sort_direction.clone(),
                    condition: payload.condition.clone(),
                },
                offset: payload.offset,
                limit: payload.limit,
            },
            job,
        )?;
        hits.push(CrossSearchHit {
            project_id,
            total_filtered_rows: response.total_filtered_rows,
            rows: response.rows,
        });
    }
    Ok(hits)
}

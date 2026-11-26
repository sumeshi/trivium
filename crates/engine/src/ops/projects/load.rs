use std::collections::HashMap;

use uuid::Uuid;

use crate::{
    error::AppError,
    ioc::load_ioc_entries,
    jobs::JobContext,
    models::{LoadProjectResponse, ProjectSummary},
    project_io::{read_project_schema_and_row_count, ProjectColumns},
    storage::load_color_rules,
    store::ProjectsStore,
};

use super::{profile::load_profile, ProjectRequest};

/// Loads case metadata and its cached column profile.
pub fn load_project(
    store: &ProjectsStore,
    request: ProjectRequest,
) -> Result<LoadProjectResponse, AppError> {
    load_project_with_job(store, request, &JobContext::none())
}

pub fn load_column_kinds_with_job(
    store: &ProjectsStore,
    project_id: Uuid,
    job: &JobContext,
) -> Result<HashMap<String, String>, AppError> {
    job.check_cancelled()?;
    let meta = store
        .try_find(&project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let dir = store.project_dir(&meta.id);
    let path = dir.join("data.parquet");
    let (schema, rows) = read_project_schema_and_row_count(&path).map_err(AppError::from)?;
    let columns = ProjectColumns::from_meta(&meta);
    Ok(load_profile(&dir, rows, &schema, &columns, job)?.kinds)
}

pub fn load_project_with_job(
    store: &ProjectsStore,
    request: ProjectRequest,
    job: &JobContext,
) -> Result<LoadProjectResponse, AppError> {
    job.check_cancelled()?;
    let meta = store
        .try_find(&request.project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let dir = store.project_dir(&meta.id);
    let path = dir.join("data.parquet");
    if !path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let (schema, rows) = read_project_schema_and_row_count(&path).map_err(AppError::from)?;
    let columns = ProjectColumns::from_meta(&meta);
    let profile = load_profile(&dir, rows, &schema, &columns, job)?;
    job.check_cancelled()?;
    let iocs = load_ioc_entries(&dir).map_err(AppError::from)?;
    // A broken optional color rule must not block investigation of the case.
    let color_rules = load_color_rules(&dir)
        .unwrap_or_else(|err| {
            eprintln!("[colors] failed to load color rules for {dir:?}: {err:?}");
            None
        })
        .unwrap_or_default();
    job.check_cancelled()?;
    Ok(LoadProjectResponse {
        project: ProjectSummary { meta: meta.clone() },
        columns: columns.ids(),
        columns_meta: columns.columns().to_vec(),
        hidden_columns: meta.hidden_columns,
        column_max_chars: profile.widths,
        column_kinds: profile.kinds,
        iocs,
        color_rules,
    })
}

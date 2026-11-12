use std::collections::HashMap;

use uuid::Uuid;

use crate::{
    error::AppError,
    ioc::load_ioc_entries,
    jobs::JobContext,
    models::{LoadProjectResponse, ProjectSummary},
    project_io::{
        read_page_dataframe_with_job, read_project_row_count, read_project_schema_columns,
        ProjectColumns,
    },
    storage::{load_color_rules, load_flags_for_rows},
    store::ProjectsStore,
};

use super::{profile::load_profile, ProjectRequest};
use crate::ops::util::{materialize_rows, DEFAULT_PAGE_SIZE};

/// Compatibility entry point: includes the first 250 original rows.
pub fn load_project(
    store: &ProjectsStore,
    request: ProjectRequest,
) -> Result<LoadProjectResponse, AppError> {
    load_project_with_job(store, request, &JobContext::none())
}

pub fn load_project_with_job(
    store: &ProjectsStore,
    request: ProjectRequest,
    job: &JobContext,
) -> Result<LoadProjectResponse, AppError> {
    load(store, request, job, true)
}

/// Case switches load only metadata and the cached import profile. Table rows
/// are requested separately so stale tab responses never retain an extra page.
pub fn load_project_metadata_with_job(
    store: &ProjectsStore,
    request: ProjectRequest,
    job: &JobContext,
) -> Result<LoadProjectResponse, AppError> {
    load(store, request, job, false)
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
    let schema = read_project_schema_columns(&path).map_err(AppError::from)?;
    let rows = read_project_row_count(&path).map_err(AppError::from)?;
    let columns = ProjectColumns::from_meta_and_names(&meta, &schema);
    Ok(load_profile(&dir, rows, &schema, &columns, job)?.kinds)
}

fn load(
    store: &ProjectsStore,
    request: ProjectRequest,
    job: &JobContext,
    initial_page: bool,
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
    let schema = read_project_schema_columns(&path).map_err(AppError::from)?;
    let rows = read_project_row_count(&path).map_err(AppError::from)?;
    let columns = ProjectColumns::from_meta_and_names(&meta, &schema);
    let profile = load_profile(&dir, rows, &schema, &columns, job)?;
    let initial_rows = if initial_page {
        let frame =
            read_page_dataframe_with_job(&path, &schema, 0, DEFAULT_PAGE_SIZE.min(rows), job)
                .map_err(AppError::from)?;
        let ids = (0..frame.height()).collect::<Vec<_>>();
        let flags =
            load_flags_for_rows(&store.flags_path(&meta.id), &ids).map_err(AppError::from)?;
        materialize_rows(&frame, columns.columns(), 0..frame.height(), &flags)
    } else {
        Vec::new()
    };
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
        initial_rows,
    })
}

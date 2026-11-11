use serde::Deserialize;
use uuid::Uuid;

use crate::{
    annotation::{apply_annotation_patch, deserialize_double_option, AnnotationPatch},
    error::AppError,
    models::ProjectRow,
    project_io::{read_project_dataframe, ProjectColumns},
    storage::{
        clear_searchable_cache, count_flagged, load_color_rules, load_flags, remove_flag,
        upsert_flag,
    },
    store::ProjectsStore,
    value_utils::{build_row_cells, CellColumns},
};

use super::query::data_columns;
use super::util::{collect_row_record, matched_rule_color};

#[derive(Debug, Deserialize)]
pub struct UpdateFlagPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub row_index: usize,
    /// `None` keeps the stored flag; `Some("")` clears it.
    #[serde(default)]
    pub flag: Option<String>,
    /// `None` keeps the stored memo; `Some(None)` or a blank string clears it.
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub memo: Option<Option<String>>,
    /// `None` keeps the stored tags; `Some` replaces them.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    /// `None` keeps the stored bookmark state.
    #[serde(default)]
    pub bookmarked: Option<bool>,
}

impl UpdateFlagPayload {
    fn annotation(&self) -> AnnotationPatch {
        AnnotationPatch {
            flag: self.flag.clone(),
            memo: self.memo.clone(),
            tags: self.tags.clone(),
            bookmarked: self.bookmarked,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct HiddenColumnsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub hidden_columns: Vec<String>,
}

/// Applies or clears a user flag for a single row and updates counters.
pub fn update_flag(
    store: &ProjectsStore,
    payload: UpdateFlagPayload,
) -> Result<ProjectRow, AppError> {
    let Some(meta) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = store.project_dir(&payload.project_id);
    let flags_path = project_dir.join("flags.json");
    let existing = load_flags(&flags_path)
        .map_err(AppError::from)?
        .get(&payload.row_index)
        .cloned();
    let next = apply_annotation_patch(existing.as_ref(), &payload.annotation());

    match &next {
        Some(entry) => upsert_flag(&flags_path, payload.row_index, entry).map_err(AppError::from)?,
        None => remove_flag(&flags_path, payload.row_index).map_err(AppError::from)?,
    }

    // The IOC outcome cache is independent of user flags, so a flag update
    // never invalidates it.

    let flagged_records = count_flagged(&flags_path).map_err(AppError::from)?;
    store
        .update_flagged_records(&payload.project_id, flagged_records)
        .map_err(AppError::from)?;

    let parquet_path = project_dir.join("data.parquet");
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let column_names = data_columns(&df);
    let record = collect_row_record(&df, &column_names, payload.row_index);
    let project_columns = ProjectColumns::from_meta(&meta, &df);
    let cell_columns = CellColumns::new(&df, project_columns.columns());
    let cells = build_row_cells(
        &df,
        payload.row_index,
        project_columns.columns(),
        &cell_columns,
    );

    // Same failure-tolerant behavior as the query path: a broken color file
    // must not prevent a flag update from being saved.
    let color = match load_color_rules(&project_dir) {
        Ok(rules) => matched_rule_color(&record, &rules.unwrap_or_default())?,
        Err(err) => {
            eprintln!(
                "[colors] failed to load color rules for {:?}: {:?}",
                project_dir, err
            );
            None
        }
    };

    Ok(ProjectRow {
        row_index: payload.row_index,
        data: record,
        cells,
        flag: next.as_ref().map(|entry| entry.flag.clone()).unwrap_or_default(),
        memo: next.as_ref().and_then(|entry| entry.memo.clone()),
        tags: next
            .as_ref()
            .map(|entry| entry.tags.clone())
            .unwrap_or_default(),
        bookmarked: next.as_ref().is_some_and(|entry| entry.bookmarked),
        color,
    })
}

/// Persists the set of hidden columns for a project and resets search cache.
pub fn set_hidden_columns(
    store: &ProjectsStore,
    payload: HiddenColumnsPayload,
) -> Result<(), AppError> {
    let Some(_) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = store.project_dir(&payload.project_id);
    store
        .update_hidden_columns(&payload.project_id, payload.hidden_columns)
        .map_err(AppError::from)?;
    if let Err(err) = clear_searchable_cache(&project_dir) {
        eprintln!(
            "[cache] failed to clear searchable cache for {:?}: {:?}",
            project_dir, err
        );
    }
    Ok(())
}

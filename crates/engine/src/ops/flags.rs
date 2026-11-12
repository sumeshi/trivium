use serde::Deserialize;
use uuid::Uuid;

use crate::{
    annotation::{deserialize_double_option, AnnotationPatch},
    error::AppError,
    models::UpdateFlagResponse,
    storage::patch_flag_atomic,
    store::ProjectsStore,
};

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
            ..AnnotationPatch::default()
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
) -> Result<UpdateFlagResponse, AppError> {
    let Some(meta) = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
    else {
        return Err(AppError::Message("Project not found.".into()));
    };
    if payload.row_index >= meta.total_records {
        return Err(AppError::Message("Row index out of range.".into()));
    }

    patch_flag_atomic(
        &store.flags_path(&payload.project_id),
        payload.row_index,
        &payload.annotation(),
    )
    .map_err(AppError::from)
}

/// Persists the set of hidden columns for a project.
pub fn set_hidden_columns(
    store: &ProjectsStore,
    payload: HiddenColumnsPayload,
) -> Result<(), AppError> {
    let Some(_) = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
    else {
        return Err(AppError::Message("Project not found.".into()));
    };
    store
        .update_hidden_columns(&payload.project_id, payload.hidden_columns)
        .map_err(AppError::from)
}

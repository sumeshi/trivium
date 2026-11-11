use serde::Deserialize;
use uuid::Uuid;

use crate::{
    annotation::{apply_annotation_patch, deserialize_double_option, AnnotationPatch},
    error::AppError,
    models::UpdateFlagResponse,
    storage::{load_flag, remove_flag, upsert_flag},
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
    let Some(meta) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()));
    };
    if payload.row_index >= meta.total_records {
        return Err(AppError::Message("Row index out of range.".into()));
    }

    let flags_path = store.flags_path(&payload.project_id);
    let existing = load_flag(&flags_path, payload.row_index).map_err(AppError::from)?;
    let was_flagged = existing
        .as_ref()
        .is_some_and(|entry| entry.is_flagged());
    let next = apply_annotation_patch(existing.as_ref(), &payload.annotation());
    let is_flagged = next.as_ref().is_some_and(|entry| entry.is_flagged());

    match &next {
        Some(entry) => upsert_flag(&flags_path, payload.row_index, entry).map_err(AppError::from)?,
        None => remove_flag(&flags_path, payload.row_index).map_err(AppError::from)?,
    }

    // The IOC outcome cache is independent of user flags, so a flag update
    // never invalidates it.

    // Counts are updated by the difference for this row instead of scanning
    // every annotation in the project.
    let flagged_records = match (was_flagged, is_flagged) {
        (false, true) => meta.flagged_records + 1,
        (true, false) => meta.flagged_records.saturating_sub(1),
        _ => meta.flagged_records,
    };
    // The annotation database is authoritative. The project list count is
    // derived and rebuildable, so a metadata write failure must not make the
    // UI report an already-committed annotation as unsaved.
    if let Err(err) = store.update_flagged_records(&payload.project_id, flagged_records) {
        eprintln!(
            "[flags] failed to persist the flagged count for {}: {:?}",
            payload.project_id, err
        );
        // Keep the derived count correct for this session. The annotation DB
        // remains authoritative, and a later successful write persists it.
        store.set_flagged_records_override(&payload.project_id, flagged_records);
    }

    Ok(UpdateFlagResponse {
        entry: next,
        revision: store.next_revision(&payload.project_id),
        flagged_records,
    })
}

/// Persists the set of hidden columns for a project.
pub fn set_hidden_columns(
    store: &ProjectsStore,
    payload: HiddenColumnsPayload,
) -> Result<(), AppError> {
    let Some(_) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()));
    };
    store
        .update_hidden_columns(&payload.project_id, payload.hidden_columns)
        .map_err(AppError::from)
}

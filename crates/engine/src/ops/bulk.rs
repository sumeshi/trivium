use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    annotation::AnnotationPatch,
    error::AppError,
    jobs::JobContext,
    storage::{apply_bulk_patch, load_page_flag_snapshot, undo_bulk_patch},
    store::ProjectsStore,
};

use super::query::{resolve_project_target, spec_token_json, QuerySpec};

#[derive(Debug, Clone, Deserialize)]
pub struct BulkAnnotatePayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    /// "selection" | "filtered" | "all"
    pub target: String,
    /// target = "selection" のときだけ使う。重複は破格に dedupe、範囲外はエラー。
    #[serde(default, rename = "rowIndices")]
    pub row_indices: Option<Vec<usize>>,
    /// target = "filtered" のときの条件（query と同一語彙）。
    #[serde(flatten)]
    pub spec: QuerySpec,
    /// 適用内容（省略したフィールドは既存値を維持する）。
    #[serde(flatten)]
    pub patch: AnnotationPatch,
    /// `bulk_preview` が返したトークン。`bulk_update` では必須。
    #[serde(default, rename = "previewToken")]
    pub preview_token: Option<String>,
}

#[derive(Debug, Serialize, ts_rs::TS)]
pub struct BulkCount {
    pub count: usize,
    /// Binds a later `bulk_update` to the same spec, row set, and annotated
    /// count that this preview observed.
    #[serde(rename = "previewToken")]
    pub preview_token: String,
}

#[derive(Debug, Serialize, ts_rs::TS)]
pub struct BulkApplied {
    pub updated: usize,
}

#[derive(Debug, Deserialize)]
pub struct BulkUndoPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
}

struct BulkTarget {
    ids: Arc<Vec<usize>>,
    revision: u64,
    token: String,
}

fn resolve_bulk_target(
    store: &ProjectsStore,
    payload: &BulkAnnotatePayload,
    job: &JobContext,
) -> Result<BulkTarget, AppError> {
    job.check_cancelled()?;
    let meta = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let mut spec = payload.spec.clone();
    if payload.target == "filtered" {
        spec.resolve_search_labels(&meta.columns)?;
    }
    // All/selection operate from Parquet metadata and one batched annotation
    // snapshot. Selecting one row must not allocate IDs or flags for every row.
    let (ids, annotated, revision) = match payload.target.as_str() {
        "filtered" => {
            let resolved = resolve_project_target(store, payload.project_id, &spec, job)?;
            (
                resolved.ids,
                resolved.annotated_filtered,
                resolved.annotation_revision,
            )
        }
        "all" | "selection" => {
            let data = store.project_dir(&payload.project_id).join("data.parquet");
            if !data.exists() {
                return Err(AppError::Message("Project data file missing.".into()));
            }
            let total = crate::project_io::read_project_row_count(&data).map_err(AppError::from)?;
            let mut ids = if payload.target == "all" {
                (0..total).collect()
            } else {
                payload.row_indices.clone().unwrap_or_default()
            };
            ids.sort_unstable();
            ids.dedup();
            if let Some(index) = ids.iter().find(|&&index| index >= total) {
                return Err(AppError::Message(format!(
                    "Row index out of range: {index}."
                )));
            }
            let selected = if payload.target == "selection" {
                ids.as_slice()
            } else {
                &[]
            };
            let (flags, total_flagged, revision) =
                load_page_flag_snapshot(&store.flags_path(&payload.project_id), selected)
                    .map_err(AppError::from)?;
            let annotated = if payload.target == "all" {
                total_flagged
            } else {
                flags
                    .values()
                    .filter(|entry| crate::storage::is_recognized_flag(entry))
                    .count()
            };
            (Arc::new(ids), annotated, revision)
        }
        _ => return Err(AppError::Message("Unknown bulk target.".into())),
    };
    let mut hash = Sha256::new();
    hash.update(payload.project_id.as_bytes());
    hash.update(payload.target.as_bytes());
    hash.update(spec_token_json(&spec).as_bytes());
    hash.update(revision.to_le_bytes());
    hash.update(annotated.to_le_bytes());
    for (position, index) in ids.iter().enumerate() {
        if position.is_multiple_of(4096) {
            job.check_cancelled()?;
        }
        hash.update(index.to_le_bytes());
    }
    let token = format!("{:x}", hash.finalize());
    Ok(BulkTarget {
        ids,
        revision,
        token,
    })
}

pub fn bulk_preview(
    store: &ProjectsStore,
    payload: BulkAnnotatePayload,
) -> Result<BulkCount, AppError> {
    bulk_preview_with_job(store, payload, &JobContext::none())
}

pub fn bulk_preview_with_job(
    store: &ProjectsStore,
    payload: BulkAnnotatePayload,
    job: &JobContext,
) -> Result<BulkCount, AppError> {
    let target = resolve_bulk_target(store, &payload, job)?;
    Ok(BulkCount {
        count: target.ids.len(),
        preview_token: target.token,
    })
}

pub fn bulk_update(
    store: &ProjectsStore,
    payload: BulkAnnotatePayload,
) -> Result<BulkApplied, AppError> {
    bulk_update_with_job(store, payload, &JobContext::none())
}

pub fn bulk_update_with_job(
    store: &ProjectsStore,
    payload: BulkAnnotatePayload,
    job: &JobContext,
) -> Result<BulkApplied, AppError> {
    let target = resolve_bulk_target(store, &payload, job)?;
    if payload.preview_token.as_deref() != Some(target.token.as_str()) {
        return Err(AppError::Message("Target changed; preview again.".into()));
    }
    job.check_cancelled()?;
    let updated = apply_bulk_patch(
        &store.flags_path(&payload.project_id),
        &target.ids,
        &payload.patch,
        target.revision,
        job,
    )
    .map_err(AppError::from)?;
    Ok(BulkApplied { updated })
}

pub fn bulk_undo(store: &ProjectsStore, payload: BulkUndoPayload) -> Result<BulkApplied, AppError> {
    bulk_undo_with_job(store, payload, &JobContext::none())
}

pub fn bulk_undo_with_job(
    store: &ProjectsStore,
    payload: BulkUndoPayload,
    job: &JobContext,
) -> Result<BulkApplied, AppError> {
    job.check_cancelled()?;
    if store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
        .is_none()
    {
        return Err(AppError::Message("Project not found.".into()));
    }
    let updated =
        undo_bulk_patch(&store.flags_path(&payload.project_id), job).map_err(AppError::from)?;
    Ok(BulkApplied { updated })
}

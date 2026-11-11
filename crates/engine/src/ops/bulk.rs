use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::annotation::{apply_annotation_patch, AnnotationPatch};
use crate::error::AppError;
use crate::ioc::{cached_ioc_outcomes, load_ioc_entries, IocRowOutcome};
use crate::models::FlagEntry;
use crate::project_io::read_project_dataframe;
use crate::storage::{count_flagged, load_flags, remove_flag, upsert_flag};
use crate::store::ProjectsStore;

use super::query::{
    data_columns, final_flags, resolve_target, spec_token_json, QuerySpec,
};

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

#[derive(Debug, Serialize)]
pub struct BulkCount {
    pub count: usize,
    /// Binds a later `bulk_update` to the same spec, row set, and annotated
    /// count that this preview observed.
    #[serde(rename = "previewToken")]
    pub preview_token: String,
}

#[derive(Debug, Serialize)]
pub struct BulkApplied {
    pub updated: usize,
}

#[derive(Debug, Deserialize)]
pub struct BulkUndoPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
}

/// 一括操作の Undo 層。プロセスごとに 1 層（最後の bulk_update のみ）。
struct BulkUndo {
    project_id: Uuid,
    /// 実行前の状態（None = エントリ無し）
    snapshot: Vec<(usize, Option<FlagEntry>)>,
}

fn undo_slot() -> &'static Mutex<Option<BulkUndo>> {
    static SLOT: OnceLock<Mutex<Option<BulkUndo>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

fn lock_undo() -> Result<MutexGuard<'static, Option<BulkUndo>>, AppError> {
    undo_slot()
        .lock()
        .map_err(|_| AppError::Message("Bulk undo state is unavailable.".into()))
}

/// Opens the project frame, mirroring the other ops' metadata/read messages.
fn load_frame(store: &ProjectsStore, project_id: &Uuid) -> Result<(PathBuf, DataFrame), AppError> {
    let meta = store
        .find(project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    Ok((project_dir, df))
}

fn annotated_count(final_flag_vec: &[String], row_ids: &[usize]) -> usize {
    row_ids
        .iter()
        .filter(|idx| !final_flag_vec[**idx].trim().is_empty())
        .count()
}

/// 対象行インデックスの解決（プレビューと実行で共通）。target:
/// - "all"       -> 0..df.height()
/// - "filtered"  -> query パスと同一の `resolve_target` で一致行（ソート順）
/// - "selection" -> dedupe + ソートした row_indices（範囲外はエラー）
fn resolve_bulk_target(
    df: &DataFrame,
    project_dir: &Path,
    payload: &BulkAnnotatePayload,
    flags: &std::collections::HashMap<usize, FlagEntry>,
    ioc_outcomes: &[IocRowOutcome],
) -> Result<(Vec<usize>, usize), AppError> {
    match payload.target.as_str() {
        "all" => {
            let row_ids: Vec<usize> = (0..df.height()).collect();
            let final_flag_vec = final_flags(df.height(), flags, ioc_outcomes);
            let annotated = annotated_count(&final_flag_vec, &row_ids);
            Ok((row_ids, annotated))
        }
        "filtered" => {
            let columns = data_columns(df);
            let target = resolve_target(
                df,
                project_dir,
                &payload.spec,
                &columns,
                flags,
                ioc_outcomes,
            )?;
            Ok((target.filtered, target.annotated_filtered))
        }
        "selection" => {
            let mut indices = payload.row_indices.clone().unwrap_or_default();
            indices.sort_unstable();
            indices.dedup();
            if let Some(&out_of_range) = indices.iter().find(|&&idx| idx >= df.height()) {
                return Err(AppError::Message(format!(
                    "Row index out of range: {}.",
                    out_of_range
                )));
            }
            let final_flag_vec = final_flags(df.height(), flags, ioc_outcomes);
            let annotated = annotated_count(&final_flag_vec, &indices);
            Ok((indices, annotated))
        }
        _ => Err(AppError::Message("Unknown bulk target.".into())),
    }
}

fn target_token(spec: &QuerySpec, row_ids: &[usize], annotated_filtered: usize) -> String {
    let joined = row_ids
        .iter()
        .map(|row_id| row_id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let material = format!(
        "{}|{}|{}",
        spec_token_json(spec),
        joined,
        annotated_filtered
    );
    let mut hasher = Sha256::new();
    hasher.update(material.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect()
}

fn resolve_and_token(
    df: &DataFrame,
    project_dir: &Path,
    payload: &BulkAnnotatePayload,
) -> Result<(Vec<usize>, String), AppError> {
    let flags = load_flags(&project_dir.join("flags.json")).map_err(AppError::from)?;
    let iocs = load_ioc_entries(project_dir).map_err(AppError::from)?;
    let ioc_outcomes = cached_ioc_outcomes(project_dir, df, &iocs);
    let (row_ids, annotated_filtered) =
        resolve_bulk_target(df, project_dir, payload, &flags, &ioc_outcomes)?;
    let token = target_token(&payload.spec, &row_ids, annotated_filtered);
    Ok((row_ids, token))
}

/// 対象件数プレビュー（状態を変更しない）
pub fn bulk_preview(
    store: &ProjectsStore,
    payload: BulkAnnotatePayload,
) -> Result<BulkCount, AppError> {
    let (project_dir, df) = load_frame(store, &payload.project_id)?;
    let (target, preview_token) = resolve_and_token(&df, &project_dir, &payload)?;
    Ok(BulkCount {
        count: target.len(),
        preview_token,
    })
}

/// 一括適用
pub fn bulk_update(
    store: &ProjectsStore,
    payload: BulkAnnotatePayload,
) -> Result<BulkApplied, AppError> {
    let (project_dir, df) = load_frame(store, &payload.project_id)?;
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;

    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);
    let (target, annotated_filtered) =
        resolve_bulk_target(&df, &project_dir, &payload, &flags, &ioc_outcomes)?;
    let current_token = target_token(&payload.spec, &target, annotated_filtered);

    // A missing token is the same failure as a stale one: the caller did not
    // preview this exact target. This also covers selection targets.
    if payload.preview_token.as_deref() != Some(current_token.as_str()) {
        return Err(AppError::Message(
            "Target changed; preview again.".into(),
        ));
    }

    if target.is_empty() {
        return Ok(BulkApplied { updated: 0 });
    }

    let snapshot: Vec<(usize, Option<FlagEntry>)> = target
        .iter()
        .map(|&row_index| (row_index, flags.get(&row_index).cloned()))
        .collect();

    // 一括対象にも単行更新と同じ patch を適用する。省略したフィールドは
    // 行ごとの既存値を維持し、全フィールドが空になった行だけ削除する。
    for &row_index in &target {
        let next = apply_annotation_patch(flags.get(&row_index), &payload.patch);
        match next {
            Some(entry) => {
                upsert_flag(&flags_path, row_index, &entry).map_err(AppError::from)?
            }
            None => remove_flag(&flags_path, row_index).map_err(AppError::from)?,
        }
    }

    // The IOC outcome cache is independent of user flags (R5), so a bulk
    // update never invalidates it.
    let flagged_records = count_flagged(&flags_path).map_err(AppError::from)?;
    store
        .update_flagged_records(&payload.project_id, flagged_records)
        .map_err(AppError::from)?;

    *lock_undo()? = Some(BulkUndo {
        project_id: payload.project_id,
        snapshot,
    });

    Ok(BulkApplied {
        updated: target.len(),
    })
}

/// 直前の一括更新を 1 層だけ復元する。
pub fn bulk_undo(store: &ProjectsStore, payload: BulkUndoPayload) -> Result<BulkApplied, AppError> {
    let undo = {
        let mut slot = lock_undo()?;
        match slot.as_ref() {
            Some(state) if state.project_id == payload.project_id => slot.take(),
            _ => None,
        }
    };
    let Some(undo) = undo else {
        return Err(AppError::Message("No bulk operation to undo.".into()));
    };

    let (project_dir, _df) = load_frame(store, &payload.project_id)?;
    let flags_path = project_dir.join("flags.json");
    for (row_index, entry) in &undo.snapshot {
        match entry {
            Some(entry) => upsert_flag(&flags_path, *row_index, entry).map_err(AppError::from)?,
            None => remove_flag(&flags_path, *row_index).map_err(AppError::from)?,
        }
    }

    let flagged_records = count_flagged(&flags_path).map_err(AppError::from)?;
    store
        .update_flagged_records(&payload.project_id, flagged_records)
        .map_err(AppError::from)?;

    Ok(BulkApplied {
        updated: undo.snapshot.len(),
    })
}

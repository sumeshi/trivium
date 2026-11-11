use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::annotation::{apply_annotation_patch, AnnotationPatch};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::FlagEntry;
use crate::project_io::read_project_dataframe;
use crate::storage::{apply_flag_batch, load_flag, load_flags};
use crate::store::ProjectsStore;

use super::query::{data_columns, resolve_target, spec_token_json, user_flags, QuerySpec};

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

/// 一括操作の Undo 層。案件ごとに 1 層を保持し、復元が成功するまで消さない。
#[derive(Clone)]
struct BulkUndo {
    /// 実行前の状態（None = エントリ無し）
    previous: Vec<(usize, Option<FlagEntry>)>,
    /// 実行後の状態（競合検出に使う）
    applied: Vec<(usize, Option<FlagEntry>)>,
    /// この操作で発行した revision。復元後に自分のエントリだけを消すために使う。
    revision_after: u64,
}

fn undo_map() -> &'static Mutex<HashMap<Uuid, BulkUndo>> {
    static MAP: OnceLock<Mutex<HashMap<Uuid, BulkUndo>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_undo() -> Result<MutexGuard<'static, HashMap<Uuid, BulkUndo>>, AppError> {
    undo_map()
        .lock()
        .map_err(|_| AppError::Message("Bulk undo state is unavailable.".into()))
}

fn is_flagged(entry: &Option<FlagEntry>) -> bool {
    entry
        .as_ref()
        .is_some_and(|entry| !entry.flag.trim().is_empty())
}

/// Recomputes a project's flagged count from a known baseline and the rows a
/// bulk operation changed, without scanning every annotation.
fn count_after(
    baseline: usize,
    before: &[(usize, Option<FlagEntry>)],
    after: &[(usize, Option<FlagEntry>)],
) -> usize {
    let mut count = baseline as isize;
    for ((_, before), (_, after)) in before.iter().zip(after.iter()) {
        match (is_flagged(before), is_flagged(after)) {
            (false, true) => count += 1,
            (true, false) => count -= 1,
            _ => {}
        }
    }
    count.max(0) as usize
}

fn warn_count_failure(project_id: &Uuid, err: anyhow::Error) {
    eprintln!(
        "[bulk] annotations were saved but the flagged count for project {} could not be persisted: {:?}",
        project_id, err
    );
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
    payload: &BulkAnnotatePayload,
    flags: &std::collections::HashMap<usize, FlagEntry>,
    storage_version: u32,
) -> Result<(Vec<usize>, usize), AppError> {
    match payload.target.as_str() {
        "all" => {
            let row_ids: Vec<usize> = (0..df.height()).collect();
            let final_flag_vec = user_flags(df.height(), flags);
            let annotated = annotated_count(&final_flag_vec, &row_ids);
            Ok((row_ids, annotated))
        }
        "filtered" => {
            let columns = data_columns(df, storage_version);
            let target = resolve_target(df, &payload.spec, &columns, flags, &JobContext::none())?;
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
            let final_flag_vec = user_flags(df.height(), flags);
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
    storage_version: u32,
) -> Result<(Vec<usize>, String), AppError> {
    let flags = load_flags(&project_dir.join("flags.json")).map_err(AppError::from)?;
    let (row_ids, annotated_filtered) =
        resolve_bulk_target(df, payload, &flags, storage_version)?;
    let token = target_token(&payload.spec, &row_ids, annotated_filtered);
    Ok((row_ids, token))
}

/// 対象件数プレビュー（状態を変更しない）
pub fn bulk_preview(
    store: &ProjectsStore,
    payload: BulkAnnotatePayload,
) -> Result<BulkCount, AppError> {
    let Some(meta) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()));
    };
    let (project_dir, df) = load_frame(store, &payload.project_id)?;
    let (target, preview_token) =
        resolve_and_token(&df, &project_dir, &payload, meta.storage_version)?;
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
    let Some(meta) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()));
    };
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;

    let (target, annotated_filtered) =
        resolve_bulk_target(&df, &payload, &flags, meta.storage_version)?;
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

    // The target was fixed above; compute every row's next entry in memory
    // before touching the database so the batch contains the whole change.
    let previous: Vec<(usize, Option<FlagEntry>)> = target
        .iter()
        .map(|&row_index| (row_index, flags.get(&row_index).cloned()))
        .collect();
    let changes: Vec<(usize, Option<FlagEntry>)> = target
        .iter()
        .map(|&row_index| {
            (
                row_index,
                apply_annotation_patch(flags.get(&row_index), &payload.patch),
            )
        })
        .collect();

    apply_flag_batch(&flags_path, &changes).map_err(AppError::from)?;

    // User annotations and the flagged count are independent of the legacy
    // IOC definitions, so a bulk update never touches IOC state.
    let flagged_records = count_after(meta.flagged_records, &previous, &changes);
    if let Err(err) = store.update_flagged_records(&payload.project_id, flagged_records) {
        warn_count_failure(&payload.project_id, err);
        store.set_flagged_records_override(&payload.project_id, flagged_records);
    }

    let revision_after = store.next_revision(&payload.project_id);
    lock_undo()?.insert(
        payload.project_id,
        BulkUndo {
            previous,
            applied: changes,
            revision_after,
        },
    );

    Ok(BulkApplied {
        updated: target.len(),
    })
}

/// 案件ごとの直前の一括更新を 1 層だけ復元する。
pub fn bulk_undo(store: &ProjectsStore, payload: BulkUndoPayload) -> Result<BulkApplied, AppError> {
    let Some(meta) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()));
    };
    let undo = {
        let map = lock_undo()?;
        map.get(&payload.project_id).cloned()
    };
    let Some(undo) = undo else {
        return Err(AppError::Message("No bulk operation to undo.".into()));
    };

    let flags_path = store.project_dir(&payload.project_id).join("flags.json");

    // Validate every row before writing anything. A later single-row update
    // must not be silently overwritten, and a conflict leaves the undo entry
    // in place so the caller can retry after inspecting the row.
    for (row_index, expected) in &undo.applied {
        let current = load_flag(&flags_path, *row_index).map_err(AppError::from)?;
        if current != *expected {
            return Err(AppError::Message(
                "Annotation changed since the bulk operation; undo would overwrite it.".into(),
            ));
        }
    }

    apply_flag_batch(&flags_path, &undo.previous).map_err(AppError::from)?;
    // Undo restores annotation state, so flag/bookmark-dependent result caches
    // must see a new revision just like a normal bulk update.
    store.next_revision(&payload.project_id);

    let flagged_records = count_after(meta.flagged_records, &undo.applied, &undo.previous);
    if let Err(err) = store.update_flagged_records(&payload.project_id, flagged_records) {
        warn_count_failure(&payload.project_id, err);
        store.set_flagged_records_override(&payload.project_id, flagged_records);
    }

    // Remove only the entry this call restored. Another bulk for the same
    // project may have replaced it while the batch was being written.
    let mut map = lock_undo()?;
    if map
        .get(&payload.project_id)
        .is_some_and(|state| state.revision_after == undo.revision_after)
    {
        map.remove(&payload.project_id);
    }

    Ok(BulkApplied {
        updated: undo.previous.len(),
    })
}

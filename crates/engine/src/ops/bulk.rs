use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::annotation::{apply_annotation_patch, deserialize_double_option, AnnotationPatch};
use crate::error::AppError;
use crate::ioc::{cached_ioc_outcomes, load_ioc_entries};
use crate::models::FlagEntry;
use crate::project_io::read_project_dataframe;
use crate::storage::{count_flagged, load_flags, remove_flag, upsert_flag};
use crate::store::ProjectsStore;

use super::query::{filtered_indices, final_flags, prepare_search};

#[derive(Debug, Deserialize)]
pub struct BulkAnnotatePayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    /// "selection" | "filter" | "all"
    pub target: String,
    /// target = "selection" のときだけ使う。重複は破格に dedupe、範囲外はエラー。
    #[serde(default, rename = "rowIndices")]
    pub row_indices: Option<Vec<usize>>,
    // target = "filter" のときのフィルタ（query と同一語彙）
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default)]
    pub condition: Option<crate::condition::Node>,
    // 適用内容（省略したフィールドは既存値を維持する）
    /// `None` keeps the stored flag; `Some("")` clears it.
    #[serde(default)]
    pub flag: Option<String>,
    /// `None` keeps the stored memo; `Some(None)` or a blank string clears it.
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub memo: Option<Option<String>>,
    /// `None` keeps the stored tags; `Some(vec![])` clears them.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    /// `None` keeps the stored bookmark state; `Some(false)` clears it.
    #[serde(default)]
    pub bookmarked: Option<bool>,
}

impl BulkAnnotatePayload {
    fn annotation(&self) -> AnnotationPatch {
        AnnotationPatch {
            flag: self.flag.clone(),
            memo: self.memo.clone(),
            tags: self.tags.clone(),
            bookmarked: self.bookmarked,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BulkCount {
    pub count: usize,
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

/// 対象行インデックスの解決（プレビューと実行で共通）。target:
/// - "all"       -> 0..df.height()
/// - "filter"    -> query パスと同一の helper で一致行
/// - "selection" -> dedupe + ソートした row_indices（範囲外はエラー）
fn resolve_target_like(
    df: &DataFrame,
    project_dir: &Path,
    payload: &BulkAnnotatePayload,
) -> Result<Vec<usize>, AppError> {
    match payload.target.as_str() {
        "all" => Ok((0..df.height()).collect()),
        "filter" => {
            let columns: Vec<String> = df
                .get_column_names()
                .into_iter()
                .filter(|name| name != &"__rowid")
                .map(|name| name.to_string())
                .collect();
            let search_state = prepare_search(
                df,
                project_dir,
                payload.columns.as_deref(),
                &columns,
                payload.search.as_deref(),
                payload.condition.as_ref(),
            );
            let flags = load_flags(&project_dir.join("flags.json")).map_err(AppError::from)?;
            let iocs = load_ioc_entries(project_dir).map_err(AppError::from)?;
            let ioc_outcomes = cached_ioc_outcomes(project_dir, df, &iocs);
            let final_flag_vec = final_flags(df.height(), &flags, &ioc_outcomes);
            let bookmarked: Vec<bool> = (0..df.height())
                .map(|idx| flags.get(&idx).is_some_and(|entry| entry.bookmarked))
                .collect();
            let ordered: Vec<usize> = (0..df.height()).collect();
            let (filtered, _) = filtered_indices(
                &ordered,
                &final_flag_vec,
                &bookmarked,
                None,
                payload.flag_filter.as_deref(),
                search_state.mask.as_deref(),
            );
            Ok(filtered)
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
            Ok(indices)
        }
        _ => Err(AppError::Message("Unknown bulk target.".into())),
    }
}

/// 対象件数プレビュー（状態を変更しない）
pub fn bulk_preview(
    store: &ProjectsStore,
    payload: BulkAnnotatePayload,
) -> Result<BulkCount, AppError> {
    let (project_dir, df) = load_frame(store, &payload.project_id)?;
    let target = resolve_target_like(&df, &project_dir, &payload)?;
    Ok(BulkCount {
        count: target.len(),
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

    let target = resolve_target_like(&df, &project_dir, &payload)?;
    if target.is_empty() {
        return Ok(BulkApplied { updated: 0 });
    }

    let snapshot: Vec<(usize, Option<FlagEntry>)> = target
        .iter()
        .map(|&row_index| (row_index, flags.get(&row_index).cloned()))
        .collect();

    // 一括対象にも単行更新と同じ patch を適用する。省略したフィールドは
    // 行ごとの既存値を維持し、全フィールドが空になった行だけ削除する。
    let patch = payload.annotation();
    for &row_index in &target {
        let next = apply_annotation_patch(flags.get(&row_index), &patch);
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

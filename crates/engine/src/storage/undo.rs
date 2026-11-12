use std::path::Path;

use anyhow::{bail, Result};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::{
    annotation::{apply_annotation_patch, AnnotationPatch},
    jobs::JobContext,
    models::FlagEntry,
};

use super::{
    annotations::{connection, read_entry, write_entry},
    sqlite::{advance_revision, revision},
};

/// Maximum retained serialized undo payload per project (one operation).
pub const MAX_BULK_UNDO_BYTES: usize = 512 * 1024 * 1024;

fn checkpoint(job: &JobContext, index: usize) -> Result<()> {
    if index.is_multiple_of(256) {
        job.check_cancelled().map_err(anyhow::Error::new)?;
    }
    Ok(())
}

/// Previous/applied entries live on disk. Neither undo nor its validation
/// retains a vector proportional to the number of changed annotations.
pub fn apply_bulk_patch(
    path: &Path,
    rows: &[usize],
    patch: &AnnotationPatch,
    expected_revision: u64,
    job: &JobContext,
) -> Result<usize> {
    let limit = super::settings::load_setting(
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("Project directory missing"))?,
        "bulk_undo_limit_bytes",
    )?
    .map(|value| {
        value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .filter(|limit| *limit > 0)
            .ok_or_else(|| anyhow::anyhow!("Invalid bulk undo byte budget"))
    })
    .transpose()?
    .unwrap_or(MAX_BULK_UNDO_BYTES);
    apply_bulk_patch_with_limit(path, rows, patch, expected_revision, job, limit)
}

pub(super) fn apply_bulk_patch_with_limit(
    path: &Path,
    rows: &[usize],
    patch: &AnnotationPatch,
    expected_revision: u64,
    job: &JobContext,
    max_bytes: usize,
) -> Result<usize> {
    job.check_cancelled().map_err(anyhow::Error::new)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut db = connection(path)?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if revision(&tx)? != expected_revision {
        bail!("Annotations changed; preview again.");
    }
    tx.execute("DELETE FROM bulk_undo", [])?;
    tx.execute("DELETE FROM bulk_undo_state", [])?;
    let mut undo_bytes = 0usize;
    let mut delta = 0i64;
    for (position, index) in rows.iter().enumerate() {
        if position > 0 && position.is_multiple_of(256) {
            job.report_progress(
                position as u64,
                Some(rows.len() as u64),
                Some("Saving annotations".into()),
            );
        }
        checkpoint(job, position)?;
        let previous = read_entry(&tx, *index)?;
        let next = apply_annotation_patch(previous.as_ref(), patch);
        delta += i64::from(next.as_ref().is_some_and(super::is_recognized_flag))
            - i64::from(previous.as_ref().is_some_and(super::is_recognized_flag));
        let previous_json = previous.as_ref().map(serde_json::to_string).transpose()?;
        let applied_json = next.as_ref().map(serde_json::to_string).transpose()?;
        undo_bytes = undo_bytes
            .saturating_add(previous_json.as_ref().map_or(0, String::len))
            .saturating_add(applied_json.as_ref().map_or(0, String::len));
        if undo_bytes > max_bytes {
            bail!("Bulk annotation exceeds the configured undo budget ({max_bytes} bytes); select fewer rows or increase bulk_undo_limit_bytes.");
        }
        tx.execute(
            "INSERT INTO bulk_undo(row_id,previous_entry,applied_entry) VALUES(?1,?2,?3)",
            params![i64::try_from(*index)?, previous_json, applied_json],
        )?;
        write_entry(&tx, *index, next.as_ref())?;
    }
    let (revision, _) = advance_revision(&tx, delta)?;
    tx.execute(
        "INSERT INTO bulk_undo_state(singleton,revision) VALUES(1,?1)",
        [i64::try_from(revision)?],
    )?;
    job.check_cancelled().map_err(anyhow::Error::new)?;
    tx.commit()?;
    Ok(rows.len())
}

pub fn undo_bulk_patch(path: &Path, job: &JobContext) -> Result<usize> {
    job.check_cancelled().map_err(anyhow::Error::new)?;
    let mut db = connection(path)?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let exists = tx
        .query_row(
            "SELECT revision FROM bulk_undo_state WHERE singleton=1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    if exists.is_none() {
        bail!("No bulk operation to undo.");
    }
    let mut statement = tx.prepare("SELECT row_id,applied_entry FROM bulk_undo ORDER BY row_id")?;
    let mut entries = statement.query([])?;
    let mut count = 0;
    while let Some(row) = entries.next()? {
        checkpoint(job, count)?;
        let index = usize::try_from(row.get::<_, i64>(0)?)?;
        let expected: Option<FlagEntry> = row
            .get::<_, Option<String>>(1)?
            .map(|text| serde_json::from_str(&text))
            .transpose()?;
        if read_entry(&tx, index)? != expected {
            bail!("Annotation changed since the bulk operation; undo would overwrite it.");
        }
        count += 1;
    }
    drop(entries);
    drop(statement);
    let mut statement =
        tx.prepare("SELECT row_id,previous_entry,applied_entry FROM bulk_undo ORDER BY row_id")?;
    let mut entries = statement.query([])?;
    let mut delta = 0;
    let mut position = 0;
    while let Some(row) = entries.next()? {
        checkpoint(job, position)?;
        let index = usize::try_from(row.get::<_, i64>(0)?)?;
        let previous: Option<FlagEntry> = row
            .get::<_, Option<String>>(1)?
            .map(|text| serde_json::from_str(&text))
            .transpose()?;
        let applied: Option<FlagEntry> = row
            .get::<_, Option<String>>(2)?
            .map(|text| serde_json::from_str(&text))
            .transpose()?;
        delta += i64::from(previous.as_ref().is_some_and(super::is_recognized_flag))
            - i64::from(applied.as_ref().is_some_and(super::is_recognized_flag));
        write_entry(&tx, index, previous.as_ref())?;
        position += 1;
    }
    drop(entries);
    drop(statement);
    advance_revision(&tx, delta)?;
    tx.execute("DELETE FROM bulk_undo", [])?;
    tx.execute("DELETE FROM bulk_undo_state", [])?;
    job.check_cancelled().map_err(anyhow::Error::new)?;
    tx.commit()?;
    Ok(count)
}

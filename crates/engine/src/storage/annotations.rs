use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::{
    annotation::{apply_annotation_patch, AnnotationPatch},
    models::{FlagEntry, UpdateFlagResponse},
};

use super::{
    legacy::ensure_annotations,
    sqlite::{advance_revision, connect, mark_migrated, revision},
};

pub(crate) fn connection(path: &Path) -> Result<Connection> {
    let parent = path
        .parent()
        .context("Annotation path has no project directory")?;
    let mut db = connect(parent)?;
    ensure_annotations(&mut db, path)?;
    Ok(db)
}

pub(crate) fn read_entry(db: &Connection, index: usize) -> Result<Option<FlagEntry>> {
    let value: Option<String> = db
        .query_row(
            "SELECT entry FROM annotations WHERE row_id=?1",
            [i64::try_from(index)?],
            |row| row.get(0),
        )
        .optional()?;
    value
        .map(|text| serde_json::from_str(&text).context("invalid SQLite annotation entry"))
        .transpose()
}

pub(crate) fn write_entry(db: &Connection, index: usize, entry: Option<&FlagEntry>) -> Result<()> {
    if let Some(entry) = entry {
        db.execute("INSERT INTO annotations(row_id,entry,flagged) VALUES(?1,?2,?3)
                    ON CONFLICT(row_id) DO UPDATE SET entry=excluded.entry,flagged=excluded.flagged",
            params![i64::try_from(index)?, serde_json::to_string(entry)?, super::is_recognized_flag(entry)])?;
    } else {
        db.execute(
            "DELETE FROM annotations WHERE row_id=?1",
            [i64::try_from(index)?],
        )?;
    }
    Ok(())
}

pub fn load_flag_snapshot(path: &Path) -> Result<(HashMap<usize, FlagEntry>, u64)> {
    let mut db = connection(path)?;
    let tx = db.transaction()?;
    let version = revision(&tx)?;
    let mut statement = tx.prepare("SELECT row_id,entry FROM annotations ORDER BY row_id")?;
    let mut rows = statement.query([])?;
    let mut flags = HashMap::new();
    while let Some(row) = rows.next()? {
        flags.insert(
            usize::try_from(row.get::<_, i64>(0)?)?,
            serde_json::from_str(&row.get::<_, String>(1)?)?,
        );
    }
    drop(rows);
    drop(statement);
    tx.commit()?;
    Ok((flags, version))
}

pub fn load_flags(path: &Path) -> Result<HashMap<usize, FlagEntry>> {
    Ok(load_flag_snapshot(path)?.0)
}

pub fn load_flags_for_rows(path: &Path, rows: &[usize]) -> Result<HashMap<usize, FlagEntry>> {
    Ok(load_page_flag_snapshot(path, rows)?.0)
}

pub fn load_page_flag_snapshot(
    path: &Path,
    rows: &[usize],
) -> Result<(HashMap<usize, FlagEntry>, usize, u64)> {
    let mut db = connection(path)?;
    let tx = db.transaction()?;
    let (count, version): (i64, i64) = tx.query_row(
        "SELECT flagged_count,revision FROM project_state WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let mut statement = tx.prepare("SELECT entry FROM annotations WHERE row_id=?1")?;
    let mut flags = HashMap::new();
    for index in rows {
        let text: Option<String> = statement
            .query_row([i64::try_from(*index)?], |row| row.get(0))
            .optional()?;
        if let Some(text) = text {
            flags.insert(*index, serde_json::from_str(&text)?);
        }
    }
    drop(statement);
    tx.commit()?;
    Ok((flags, usize::try_from(count)?, u64::try_from(version)?))
}

pub fn load_flag(path: &Path, index: usize) -> Result<Option<FlagEntry>> {
    read_entry(&connection(path)?, index)
}

pub fn save_flags(path: &Path, flags: &HashMap<usize, FlagEntry>) -> Result<()> {
    let mut db = connection(path)?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute("DELETE FROM annotations", [])?;
    let mut count = 0;
    for (index, entry) in flags {
        write_entry(&tx, *index, Some(entry))?;
        count += usize::from(super::is_recognized_flag(entry));
    }
    tx.execute(
        "UPDATE project_state SET flagged_count=?1 WHERE singleton=1",
        [i64::try_from(count)?],
    )?;
    advance_revision(&tx, 0)?;
    mark_migrated(&tx, "annotations")?;
    tx.commit()?;
    Ok(())
}

pub fn apply_flag_batch(path: &Path, changes: &[(usize, Option<FlagEntry>)]) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let mut db = connection(path)?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut delta = 0;
    for (index, entry) in changes {
        let old = read_entry(&tx, *index)?;
        delta += i64::from(entry.as_ref().is_some_and(super::is_recognized_flag))
            - i64::from(old.as_ref().is_some_and(super::is_recognized_flag));
        write_entry(&tx, *index, entry.as_ref())?;
    }
    advance_revision(&tx, delta)?;
    tx.commit()?;
    Ok(())
}

pub fn upsert_flag(path: &Path, index: usize, entry: &FlagEntry) -> Result<()> {
    apply_flag_batch(path, &[(index, Some(entry.clone()))])
}

pub fn remove_flag(path: &Path, index: usize) -> Result<()> {
    apply_flag_batch(path, &[(index, None)])
}

pub fn count_flagged(path: &Path) -> Result<usize> {
    let count: i64 = connection(path)?.query_row(
        "SELECT flagged_count FROM project_state WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    Ok(usize::try_from(count)?)
}

pub fn patch_flag_atomic(
    path: &Path,
    index: usize,
    patch: &AnnotationPatch,
) -> Result<UpdateFlagResponse> {
    let mut db = connection(path)?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let previous = read_entry(&tx, index)?;
    let entry = apply_annotation_patch(previous.as_ref(), patch);
    let delta = i64::from(entry.as_ref().is_some_and(super::is_recognized_flag))
        - i64::from(previous.as_ref().is_some_and(super::is_recognized_flag));
    write_entry(&tx, index, entry.as_ref())?;
    let (revision, flagged_records) = advance_revision(&tx, delta)?;
    tx.commit()?;
    Ok(UpdateFlagResponse {
        entry,
        revision,
        flagged_records,
    })
}

use std::path::Path;

use anyhow::{bail, Context, Result};
use rusqlite::OptionalExtension;

use crate::models::ProjectMeta;

use super::sqlite::{connect, migrated, project_database_path, revision};

pub fn read_project_meta(project_dir: &Path) -> Result<Option<ProjectMeta>> {
    if !project_database_path(project_dir).exists() {
        return Ok(None);
    }
    let db = connect(project_dir)?;
    let text: Option<String> = db
        .query_row(
            "SELECT metadata FROM project_state WHERE singleton=1 AND deleted=0",
            [],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    text.map(|value| serde_json::from_str(&value).context("invalid SQLite project metadata"))
        .transpose()
}

pub fn project_was_registered(project_dir: &Path) -> Result<bool> {
    if !project_database_path(project_dir).exists() {
        return Ok(false);
    }
    let db = connect(project_dir)?;
    Ok(db.query_row(
        "SELECT metadata IS NOT NULL OR deleted=1 FROM project_state WHERE singleton=1",
        [],
        |row| row.get(0),
    )?)
}

pub fn insert_project_meta(project_dir: &Path, meta: &ProjectMeta) -> Result<()> {
    let mut db = connect(project_dir)?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let registered: bool = tx.query_row(
        "SELECT metadata IS NOT NULL OR deleted=1 FROM project_state WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if registered {
        bail!("Project is already registered.");
    }
    let mut meta = meta.clone();
    if migrated(&tx, "annotations")? {
        let count: i64 = tx.query_row(
            "SELECT flagged_count FROM project_state WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        meta.flagged_records = usize::try_from(count)?;
    }
    tx.execute(
        "UPDATE project_state SET metadata=?1,flagged_count=?2,deleted=0 WHERE singleton=1",
        rusqlite::params![
            serde_json::to_string(&meta)?,
            i64::try_from(meta.flagged_records)?
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn update_project_meta(
    project_dir: &Path,
    update: impl FnOnce(&mut ProjectMeta),
) -> Result<()> {
    let mut db = connect(project_dir)?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let text: Option<String> = tx
        .query_row(
            "SELECT metadata FROM project_state WHERE singleton=1 AND deleted=0",
            [],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let Some(text) = text else {
        return Ok(());
    };
    let mut meta: ProjectMeta = serde_json::from_str(&text)?;
    update(&mut meta);
    tx.execute(
        "UPDATE project_state SET metadata=?1,flagged_count=?2 WHERE singleton=1",
        rusqlite::params![
            serde_json::to_string(&meta)?,
            i64::try_from(meta.flagged_records)?
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn remove_project_meta(project_dir: &Path) -> Result<()> {
    let db = connect(project_dir)?;
    db.execute("UPDATE project_state SET deleted=1 WHERE singleton=1", [])?;
    Ok(())
}

pub fn project_revision(project_dir: &Path) -> Result<u64> {
    revision(&connect(project_dir)?)
}

pub fn bump_project_revision(project_dir: &Path) -> Result<u64> {
    let db = connect(project_dir)?;
    db.execute(
        "UPDATE project_state SET revision=revision+1 WHERE singleton=1",
        [],
    )?;
    revision(&db)
}

use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use rusqlite::{Connection, OptionalExtension};

pub const PROJECT_DATABASE: &str = "project.sqlite";

pub fn project_database_path(project_dir: &Path) -> PathBuf {
    project_dir.join(PROJECT_DATABASE)
}

/// Connections belong to one operation, never to an inactive project. SQLite's
/// page cache is bounded to 2 MiB and the WAL provides transactional recovery.
pub(crate) fn connect(project_dir: &Path) -> Result<Connection> {
    fs::create_dir_all(project_dir)
        .with_context(|| format!("failed to create project directory {project_dir:?}"))?;
    let path = project_database_path(project_dir);
    let mut db = Connection::open(&path)
        .with_context(|| format!("failed to open project database {path:?}"))?;
    db.busy_timeout(Duration::from_secs(5))?;
    db.pragma_update(None, "cache_size", -2048)?;
    db.pragma_update(None, "foreign_keys", true)?;
    let version: u32 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > 1 {
        bail!("Project database uses a newer schema ({version}); not modifying it.");
    }
    if version == 0 {
        let tables: i64 = db.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if tables != 0 {
            bail!(
                "Project database has an unrecognized schema; preserving it without modification."
            );
        }
        db.pragma_update(None, "journal_mode", "WAL")?;
        let tx = db.transaction()?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS project_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                metadata TEXT, flagged_count INTEGER NOT NULL DEFAULT 0,
                revision INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0
             );
             INSERT OR IGNORE INTO project_state(singleton) VALUES(1);
             CREATE TABLE IF NOT EXISTS annotations (
                row_id INTEGER PRIMARY KEY CHECK(row_id>=0),
                entry TEXT NOT NULL, flagged INTEGER NOT NULL CHECK(flagged IN (0,1))
             );
             CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS migrations (key TEXT PRIMARY KEY);
             CREATE TABLE IF NOT EXISTS bulk_undo (
                row_id INTEGER PRIMARY KEY, previous_entry TEXT, applied_entry TEXT
             );
             CREATE TABLE IF NOT EXISTS bulk_undo_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision INTEGER NOT NULL
             );
             PRAGMA user_version=1;",
        )?;
        tx.commit()?;
    }
    Ok(db)
}

pub(crate) fn migrated(db: &Connection, key: &str) -> Result<bool> {
    Ok(db
        .query_row("SELECT 1 FROM migrations WHERE key=?1", [key], |_| Ok(()))
        .optional()?
        .is_some())
}

pub(crate) fn mark_migrated(db: &Connection, key: &str) -> Result<()> {
    db.execute("INSERT OR IGNORE INTO migrations(key) VALUES(?1)", [key])?;
    Ok(())
}

pub(crate) fn revision(db: &Connection) -> Result<u64> {
    let value: i64 = db.query_row(
        "SELECT revision FROM project_state WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(value)?)
}

pub(crate) fn advance_revision(db: &Connection, delta: i64) -> Result<(u64, usize)> {
    db.execute(
        "UPDATE project_state SET flagged_count=MAX(0,flagged_count+?1), revision=revision+1 WHERE singleton=1",
        [delta],
    )?;
    // Keep the serialized metadata and indexed count in the same transaction.
    db.execute(
        "UPDATE project_state SET metadata=json_set(metadata,'$.flagged_records',flagged_count)
         WHERE singleton=1 AND metadata IS NOT NULL",
        [],
    )?;
    let (version, count): (i64, i64) = db.query_row(
        "SELECT revision,flagged_count FROM project_state WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok((u64::try_from(version)?, usize::try_from(count)?))
}

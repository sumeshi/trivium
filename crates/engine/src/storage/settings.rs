use std::{fs, path::Path};

use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};
use serde_json::Value;

use super::sqlite::{connect, mark_migrated, migrated};

pub fn load_setting_bytes(project_dir: &Path, key: &str) -> Result<Option<Vec<u8>>> {
    let mut db = connect(project_dir)?;
    let migration = format!("setting:{key}");
    if migrated(&db, &migration)? {
        return Ok(db
            .query_row("SELECT value FROM settings WHERE key=?1", [key], |row| {
                row.get(0)
            })
            .optional()?);
    }
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if !migrated(&tx, &migration)? {
        let path = project_dir.join(format!("{key}.json"));
        if path.exists() {
            let bytes = fs::read(&path)
                .with_context(|| format!("failed to read legacy setting {path:?}"))?;
            tx.execute(
                "INSERT OR IGNORE INTO settings(key,value) VALUES(?1,?2)",
                params![key, bytes],
            )?;
        }
        mark_migrated(&tx, &migration)?;
    }
    let value = tx
        .query_row("SELECT value FROM settings WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?;
    tx.commit()?;
    Ok(value)
}

pub fn save_setting_bytes(project_dir: &Path, key: &str, bytes: &[u8]) -> Result<()> {
    let mut db = connect(project_dir)?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tx.execute("INSERT INTO settings(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![key, bytes])?;
    mark_migrated(&tx, &format!("setting:{key}"))?;
    tx.commit()?;
    Ok(())
}

pub fn load_setting(project_dir: &Path, key: &str) -> Result<Option<Value>> {
    load_setting_bytes(project_dir, key)?
        .map(|bytes| {
            serde_json::from_slice(&bytes).with_context(|| format!("invalid project setting {key}"))
        })
        .transpose()
}

pub fn save_setting(project_dir: &Path, key: &str, value: &Value) -> Result<()> {
    save_setting_bytes(project_dir, key, &serde_json::to_vec(value)?)
}

pub fn remove_setting(project_dir: &Path, key: &str) -> Result<()> {
    let mut db = connect(project_dir)?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tx.execute("DELETE FROM settings WHERE key=?1", [key])?;
    mark_migrated(&tx, &format!("setting:{key}"))?;
    tx.commit()?;
    Ok(())
}

/// Validate and replace a setting under one write transaction. A newer or
/// unreadable view cannot race a save and be silently overwritten.
pub fn save_setting_bytes_checked(
    project_dir: &Path,
    key: &str,
    bytes: &[u8],
    validate: impl FnOnce(Option<&[u8]>) -> Result<()>,
) -> Result<()> {
    // Perform any legacy import first. It is idempotent and retains its source.
    load_setting_bytes(project_dir, key)?;
    let mut db = connect(project_dir)?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let previous: Option<Vec<u8>> = tx
        .query_row("SELECT value FROM settings WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?;
    validate(previous.as_deref())?;
    tx.execute("INSERT INTO settings(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![key, bytes])?;
    mark_migrated(&tx, &format!("setting:{key}"))?;
    tx.commit()?;
    Ok(())
}

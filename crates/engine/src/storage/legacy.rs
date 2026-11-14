use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use fs2::FileExt;
use rusqlite::{params, Connection};
use uuid::Uuid;

use crate::models::FlagEntry;

use super::sqlite::{mark_migrated, migrated};

struct Snapshot(PathBuf);
impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn copy_directory(source: &Path, target: &Path) -> Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            bail!(
                "Legacy annotation database contains a symlink: {:?}",
                entry.path()
            );
        }
        if kind.is_dir() {
            copy_directory(&entry.path(), &target.join(entry.file_name()))?;
        } else if kind.is_file() {
            let copy = target.join(entry.file_name());
            fs::copy(entry.path(), &copy)?;
            // Recovery may write only to the private snapshot, even when the
            // source backup is read-only.
            let mut permissions = fs::metadata(&copy)?.permissions();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                permissions.set_mode(permissions.mode() | 0o200);
            }
            #[cfg(not(unix))]
            {
                // Windows clears a file attribute without widening its ACL;
                // Unix uses only the owner's write bit above.
                #[allow(clippy::permissions_set_readonly_false)]
                permissions.set_readonly(false);
            }
            fs::set_permissions(copy, permissions)?;
        }
    }
    Ok(())
}

/// Never open the original sled database: recovery/open can write to its log.
/// Hold the same db-file lock as sled while taking and reading a private copy.
fn visit_sled(path: &Path, mut visit: impl FnMut(usize, FlagEntry) -> Result<()>) -> Result<File> {
    let lock_path = path.join("db");
    let lock = File::open(&lock_path)
        .with_context(|| format!("failed to open legacy database lock {lock_path:?}"))?;
    lock.try_lock_exclusive().with_context(||
        format!("Legacy annotation database is in use; close the previous application before migration: {path:?}"))?;
    let snapshot =
        Snapshot(std::env::temp_dir().join(format!("trivium-sled-migration-{}", Uuid::new_v4())));
    copy_directory(path, &snapshot.0)?;
    let db = sled::Config::new()
        .path(&snapshot.0)
        .cache_capacity(2 * 1024 * 1024)
        .flush_every_ms(None)
        .open()
        .context("failed to read legacy annotation snapshot")?;
    for value in db.iter() {
        let (key, value) = value?;
        let key = <[u8; 8]>::try_from(key.as_ref())
            .context("invalid legacy annotation row key; original database is preserved")?;
        let index = usize::try_from(u64::from_be_bytes(key))
            .context("legacy row ID does not fit this platform")?;
        visit(
            index,
            serde_json::from_slice(&value).context("invalid legacy annotation entry")?,
        )?;
    }
    drop(db);
    Ok(lock)
}

pub(crate) fn ensure_annotations(db: &mut Connection, legacy_path: &Path) -> Result<()> {
    if migrated(db, "annotations")? {
        return Ok(());
    }
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if migrated(&tx, "annotations")? {
        return Ok(());
    }
    let sled_path = legacy_path.with_extension("db");
    let mut count = 0usize;
    let mut insert = |index: usize, entry: FlagEntry| -> Result<()> {
        count += usize::from(super::is_recognized_flag(&entry));
        tx.execute(
            "INSERT INTO annotations(row_id,entry,flagged) VALUES(?1,?2,?3)",
            params![
                i64::try_from(index)?,
                serde_json::to_string(&entry)?,
                super::is_recognized_flag(&entry)
            ],
        )?;
        Ok(())
    };
    let _legacy_lock = if sled_path.exists() {
        Some(visit_sled(&sled_path, &mut insert)?)
    } else if legacy_path.exists() {
        let bytes = fs::read(legacy_path).context("failed to read legacy flags.json")?;
        let flags: HashMap<usize, FlagEntry> = serde_json::from_slice(&bytes)
            .context("failed to parse legacy flags.json; original file is preserved")?;
        for (index, entry) in flags {
            insert(index, entry)?;
        }
        None
    } else {
        None
    };
    tx.execute(
        "UPDATE project_state SET flagged_count=?1 WHERE singleton=1",
        [i64::try_from(count)?],
    )?;
    tx.execute(
        "UPDATE project_state SET metadata=json_set(metadata,'$.flagged_records',flagged_count)
                WHERE singleton=1 AND metadata IS NOT NULL",
        [],
    )?;
    mark_migrated(&tx, "annotations")?;
    tx.commit()?;
    Ok(())
}

use std::{fs, path::PathBuf};

use anyhow::{bail, Context, Result};
use uuid::Uuid;

use crate::{
    models::ProjectMeta,
    storage::{self, PROJECT_DATABASE},
};

/// A workspace contains portable project directories. The SQLite metadata in
/// each directory is authoritative; there is no second mutable registry or
/// process-wide collection of open annotation DBs.
#[derive(Clone)]
pub struct ProjectsStore {
    root_dir: PathBuf,
}

impl ProjectsStore {
    pub fn new(root_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(root_dir.join("projects"))?;
        let store = Self { root_dir };
        store.migrate_legacy_registry()?;
        store.try_all()?;
        crate::ops::query::clear_result_cache();
        Ok(store)
    }

    fn migrate_legacy_registry(&self) -> Result<()> {
        let path = self.root_dir.join("projects.json");
        if !path.exists() {
            return Ok(());
        }
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read legacy registry {path:?}"))?;
        let projects: Vec<ProjectMeta> = match serde_json::from_slice(&bytes) {
            Ok(projects) => projects,
            Err(error) => {
                let backup = self
                    .root_dir
                    .join(format!("projects.json.corrupt.{}", Uuid::new_v4()));
                fs::copy(&path, &backup).context("failed to preserve corrupt legacy registry")?;
                // Canonical cases can still be recovered without trusting the
                // damaged legacy index, but never claim an old orphan is empty.
                let dirs = fs::read_dir(self.root_dir.join("projects"))?;
                let mut canonical = 0;
                for entry in dirs {
                    let entry = entry?;
                    if !entry.file_type()?.is_dir() {
                        continue;
                    }
                    if entry.path().join(PROJECT_DATABASE).exists()
                        && storage::read_project_meta(&entry.path())?.is_some()
                    {
                        canonical += 1;
                    } else {
                        bail!("Legacy registry is unreadable ({error}); project directories remain unmigrated. Original and backup {backup:?} are preserved.");
                    }
                }
                if canonical == 0 {
                    bail!("Legacy registry is unreadable ({error}); original and backup {backup:?} are preserved.");
                }
                eprintln!("[store] recovered {canonical} canonical cases; damaged legacy registry preserved at {backup:?}");
                return Ok(());
            }
        };
        for meta in projects {
            let dir = self.project_dir(&meta.id);
            // A deleted legacy case must not be recreated from the retained
            // registry. A tombstone also prevents resurrection after failed IO.
            if !dir.exists() || storage::project_was_registered(&dir)? {
                continue;
            }
            storage::insert_project_meta(&dir, &meta)?;
        }
        Ok(())
    }

    pub fn try_all(&self) -> Result<Vec<ProjectMeta>> {
        let mut projects = Vec::new();
        for entry in fs::read_dir(self.root_dir.join("projects"))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() || !entry.path().join(PROJECT_DATABASE).exists() {
                continue;
            }
            if let Some(meta) = storage::read_project_meta(&entry.path()).with_context(|| {
                format!(
                    "cannot read saved case {:?}; its files were preserved",
                    entry.path()
                )
            })? {
                let folder = entry.file_name();
                if folder.to_str() != Some(meta.id.to_string().as_str()) {
                    bail!("Saved case metadata ID does not match its directory {:?}; files were preserved", entry.path());
                }
                projects.push(meta);
            } else if !storage::project_was_registered(&entry.path())? {
                eprintln!("[store] case {:?} has a settings database without registered metadata; preserving its files and skipping it", entry.path());
            }
        }
        Ok(projects)
    }

    /// Compatibility for existing callers; application entry points use the
    /// checked variant so corruption is surfaced rather than presented empty.
    pub fn all(&self) -> Vec<ProjectMeta> {
        self.try_all().unwrap_or_else(|error| {
            eprintln!("[store] {error:#}");
            Vec::new()
        })
    }

    pub fn try_find(&self, id: &Uuid) -> Result<Option<ProjectMeta>> {
        let metadata = storage::read_project_meta(&self.project_dir(id))?;
        if metadata.as_ref().is_some_and(|meta| meta.id != *id) {
            bail!("Saved case metadata ID does not match project {id}; files were preserved");
        }
        Ok(metadata)
    }

    pub fn find(&self, id: &Uuid) -> Option<ProjectMeta> {
        self.try_find(id).unwrap_or_else(|error| {
            eprintln!("[store] {error:#}");
            None
        })
    }

    pub fn insert(&self, meta: ProjectMeta) -> Result<()> {
        storage::insert_project_meta(&self.project_dir(&meta.id), &meta)
    }

    pub fn update_hidden_columns(&self, id: &Uuid, columns: Vec<String>) -> Result<()> {
        storage::update_project_meta(&self.project_dir(id), |meta| meta.hidden_columns = columns)
    }

    pub fn update_flagged_records(&self, id: &Uuid, count: usize) -> Result<()> {
        storage::update_project_meta(&self.project_dir(id), |meta| meta.flagged_records = count)
    }

    pub fn set_flagged_records_override(&self, id: &Uuid, count: usize) {
        if let Err(error) = self.update_flagged_records(id, count) {
            eprintln!("[store] {error:#}");
        }
    }

    pub fn update_ioc_applied_records(&self, id: &Uuid, count: usize) -> Result<()> {
        storage::update_project_meta(&self.project_dir(id), |meta| {
            meta.ioc_applied_records = count
        })
    }

    pub fn next_revision(&self, id: &Uuid) -> u64 {
        storage::bump_project_revision(&self.project_dir(id)).unwrap_or_else(|error| {
            eprintln!("[store] {error:#}");
            0
        })
    }

    pub fn current_revision(&self, id: &Uuid) -> u64 {
        storage::project_revision(&self.project_dir(id)).unwrap_or_else(|error| {
            eprintln!("[store] {error:#}");
            0
        })
    }

    pub fn remove(&self, id: &Uuid) -> Result<()> {
        storage::remove_project_meta(&self.project_dir(id))
    }

    pub fn project_dir(&self, id: &Uuid) -> PathBuf {
        self.root_dir.join("projects").join(id.to_string())
    }
    pub fn flags_path(&self, id: &Uuid) -> PathBuf {
        self.project_dir(id).join("flags.json")
    }
    pub fn staging_root(&self) -> PathBuf {
        self.root_dir.join(".staging")
    }
    /// Owned deletion workspace, excluded from saved-case discovery.
    pub fn trash_root(&self) -> PathBuf {
        self.root_dir.join(".trash")
    }
    pub fn staging_dir(&self, id: &Uuid) -> PathBuf {
        self.staging_root().join(id.to_string())
    }
}

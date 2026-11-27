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
    preferences_root: Option<PathBuf>,
}

impl ProjectsStore {
    pub fn new(root_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(root_dir.join("projects"))?;
        let store = Self {
            root_dir,
            preferences_root: None,
        };
        // Checked listing validates saved cases when requested. The desktop
        // runs that scan off its window thread; do not repeat it during setup.
        crate::ops::query::clear_result_cache();
        Ok(store)
    }

    /// Desktop preferences have a separate root. CLI stores do not infer one
    /// from their parent, which can also be an ordinary user output directory.
    pub fn with_preferences_root(mut self, root: PathBuf) -> Self {
        self.preferences_root = Some(root);
        self
    }

    /// Exact managed trees and files that every export must preserve.
    pub(crate) fn managed_output_paths(&self) -> (Vec<PathBuf>, Vec<PathBuf>) {
        let mut directories = vec![
            self.root_dir.join("projects"),
            self.staging_root(),
            self.trash_root(),
            self.formats_dir(),
        ];
        let mut files = Vec::new();
        if let Some(root) = &self.preferences_root {
            directories.extend([root.join("themes"), root.join("appearance")]);
            files.extend([
                root.join("theme.json"),
                root.join("appearance.json"),
                root.join("table-text-size.json"),
            ]);
        }
        (directories, files)
    }

    pub fn try_all(&self) -> Result<Vec<ProjectMeta>> {
        let mut projects = Vec::new();
        for entry in fs::read_dir(self.root_dir.join("projects"))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if !entry.path().join(PROJECT_DATABASE).exists() {
                bail!(
                    "Unsupported saved case {:?}: project.sqlite is required; files were preserved",
                    entry.path()
                );
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

    pub fn try_find(&self, id: &Uuid) -> Result<Option<ProjectMeta>> {
        let directory = self.project_dir(id);
        if directory.exists() && !directory.join(PROJECT_DATABASE).exists() {
            bail!("Unsupported saved case {directory:?}: project.sqlite is required; files were preserved");
        }
        let metadata = storage::read_project_meta(&directory)?;
        if metadata.as_ref().is_some_and(|meta| meta.id != *id) {
            bail!("Saved case metadata ID does not match project {id}; files were preserved");
        }
        Ok(metadata)
    }

    pub fn insert(&self, meta: ProjectMeta) -> Result<()> {
        storage::insert_project_meta(&self.project_dir(&meta.id), &meta)
    }

    pub fn update_hidden_columns(&self, id: &Uuid, columns: Vec<String>) -> Result<()> {
        storage::update_project_meta(&self.project_dir(id), |meta| meta.hidden_columns = columns)
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
    /// Global named pattern definitions owned by this application workspace.
    pub fn formats_dir(&self) -> PathBuf {
        self.root_dir.join("formats")
    }
    pub fn database_path(&self, id: &Uuid) -> PathBuf {
        storage::project_database_path(&self.project_dir(id))
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

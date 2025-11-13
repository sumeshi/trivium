use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use uuid::Uuid;

use crate::{models::ProjectMeta, storage::load_flags};

pub struct ProjectsStore {
    root_dir: PathBuf,
    meta_path: PathBuf,
    inner: Mutex<Vec<ProjectMeta>>,
}

impl ProjectsStore {
    pub fn new(root_dir: PathBuf) -> Result<Self> {
        let projects_dir = root_dir.join("projects");
        fs::create_dir_all(&projects_dir)
            .with_context(|| format!("failed to create projects dir at {:?}", projects_dir))?;

        let meta_path = root_dir.join("projects.json");
        let mut projects: Vec<ProjectMeta> = if meta_path.exists() {
            let data = fs::read(&meta_path)
                .with_context(|| format!("failed to read metadata file {:?}", meta_path))?;
            serde_json::from_slice(&data)
                .with_context(|| format!("failed to parse metadata file {:?}", meta_path))?
        } else {
            Vec::new()
        };

        // Migrate existing project data (calculate if flagged_records is 0)
        let mut needs_save = false;
        for project in &mut projects {
            if project.flagged_records == 0 {
                let project_dir = root_dir.join("projects").join(project.id.to_string());
                let flags_path = project_dir.join("flags.json");
                if flags_path.exists() {
                    if let Ok(flags) = load_flags(&flags_path) {
                        project.flagged_records = flags
                            .values()
                            .filter(|entry| !entry.flag.trim().is_empty())
                            .count();
                        needs_save = true;
                    }
                }
            }
        }

        // Save migrated data
        if needs_save {
            let data = serde_json::to_vec_pretty(&projects)
                .with_context(|| format!("failed to serialize metadata to {:?}", meta_path))?;
            fs::write(&meta_path, data)
                .with_context(|| format!("failed to write metadata file {:?}", meta_path))?;
        }

        Ok(Self {
            root_dir,
            meta_path,
            inner: Mutex::new(projects),
        })
    }

    pub fn all(&self) -> Vec<ProjectMeta> {
        self.inner.lock().clone()
    }

    pub fn insert(&self, project: ProjectMeta) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.push(project);
        self.persist_locked(&guard)
    }

    pub fn update_hidden_columns(&self, id: &Uuid, hidden_columns: Vec<String>) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.hidden_columns = hidden_columns;
        }
        self.persist_locked(&guard)
    }

    pub fn update_flagged_records(&self, id: &Uuid, flagged_records: usize) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.flagged_records = flagged_records;
        }
        self.persist_locked(&guard)
    }

    pub fn update_ioc_applied_records(&self, id: &Uuid, count: usize) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.ioc_applied_records = count;
        }
        self.persist_locked(&guard)
    }

    pub fn remove(&self, id: &Uuid) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.retain(|meta| &meta.id != id);
        self.persist_locked(&guard)
    }

    pub fn find(&self, id: &Uuid) -> Option<ProjectMeta> {
        self.inner
            .lock()
            .iter()
            .find(|meta| &meta.id == id)
            .cloned()
    }

    pub fn project_dir(&self, id: &Uuid) -> PathBuf {
        self.root_dir.join("projects").join(id.to_string())
    }

    fn persist_locked(&self, guard: &[ProjectMeta]) -> Result<()> {
        let data = serde_json::to_vec_pretty(guard)?;
        fs::write(&self.meta_path, data)
            .with_context(|| format!("failed to write metadata file {:?}", self.meta_path))
    }
}

pub struct AppState {
    pub projects: ProjectsStore,
}

impl AppState {
    pub fn new(app: &tauri::App<tauri::Wry>) -> Result<Self> {
        let base_dir = tauri::api::path::app_local_data_dir(&app.config())
            .context("failed to resolve app data dir")?
            .join("trivium");
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("failed to create app data dir {:?}", base_dir))?;
        let projects = ProjectsStore::new(base_dir)?;
        Ok(Self { projects })
    }
}

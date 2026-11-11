use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use uuid::Uuid;

use crate::{models::ProjectMeta, storage::load_flags};

/// Cloneable handle to the shared project store. Cloning is cheap and shares
/// the in-memory metadata and revision counters, so a background worker can
/// own a copy without holding the Tauri state guard for the whole operation.
#[derive(Clone)]
pub struct ProjectsStore {
    root_dir: PathBuf,
    meta_path: PathBuf,
    inner: Arc<Mutex<Vec<ProjectMeta>>>,
    /// Process-local annotation revisions. Durable state is the annotation DB;
    /// this counter only lets the frontend discard stale responses.
    revisions: Arc<Mutex<HashMap<Uuid, AtomicU64>>>,
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
            inner: Arc::new(Mutex::new(projects)),
            revisions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn all(&self) -> Vec<ProjectMeta> {
        self.inner.lock().clone()
    }

    pub fn insert(&self, project: ProjectMeta) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.push(project);
        if let Err(err) = self.persist_locked(&guard) {
            // Registration failed; do not leave the project visible in memory
            // while the caller removes its directory.
            guard.pop();
            return Err(err);
        }
        Ok(())
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

    /// Returns the next annotation revision for a project.
    ///
    /// Revisions are process-local and monotonic; they are not persisted and
    /// do not need to match across restarts.
    pub fn next_revision(&self, id: &Uuid) -> u64 {
        self.revisions
            .lock()
            .entry(*id)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    /// Returns the current annotation revision without advancing it.
    ///
    /// A project with no in-process update yet reports revision 0.
    pub fn current_revision(&self, id: &Uuid) -> u64 {
        self.revisions
            .lock()
            .get(id)
            .map(|revision| revision.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn remove(&self, id: &Uuid) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.retain(|meta| &meta.id != id);
        let result = self.persist_locked(&guard);
        if result.is_ok() {
            self.revisions.lock().remove(id);
        }
        result
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

    /// Root of the per-import staging area. Completed projects are renamed
    /// into `projects/` from here, so a failed import leaves no project dir.
    pub fn staging_root(&self) -> PathBuf {
        self.root_dir.join(".staging")
    }

    /// Staging directory for one in-progress import.
    pub fn staging_dir(&self, id: &Uuid) -> PathBuf {
        self.staging_root().join(id.to_string())
    }

    fn persist_locked(&self, guard: &[ProjectMeta]) -> Result<()> {
        let data = serde_json::to_vec_pretty(guard)?;
        // Replace the project list atomically: a failed write must not leave a
        // truncated metadata file that loses the existing projects on restart.
        let temp_path = self
            .meta_path
            .with_extension(format!("json.tmp.{}", std::process::id()));
        let result = fs::write(&temp_path, data)
            .with_context(|| format!("failed to write metadata file {:?}", temp_path))
            .and_then(|_| {
                fs::rename(&temp_path, &self.meta_path).with_context(|| {
                    format!(
                        "failed to replace metadata file {:?} with {:?}",
                        self.meta_path, temp_path
                    )
                })
            });
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result
    }
}

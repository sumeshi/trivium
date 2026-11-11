use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
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
    /// Derived flagged counts that could not be persisted yet.
    flagged_overrides: Arc<Mutex<HashMap<Uuid, usize>>>,
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
            match serde_json::from_slice(&data) {
                Ok(projects) => projects,
                Err(err) => {
                    // Keep the damaged file for recovery, but do not let one
                    // bad metadata file prevent the application from starting.
                    let backup = meta_path.with_extension(format!(
                        "json.corrupt.{}.{}",
                        std::process::id(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|duration| duration.as_secs())
                            .unwrap_or(0)
                    ));
                    let _ = fs::rename(&meta_path, &backup);
                    eprintln!(
                        "[store] projects.json was unreadable ({err}); moved to {:?} and started empty",
                        backup
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        // Rebuild a missing derived count from the annotation database. This
        // also covers projects migrated from the old flags.json format.
        let mut needs_save = false;
        for project in &mut projects {
            if project.flagged_records == 0 {
                let project_dir = root_dir.join("projects").join(project.id.to_string());
                let flags_path = project_dir.join("flags.json");
                if let Ok(flags) = load_flags(&flags_path) {
                    let count = flags
                        .values()
                        .filter(|entry| entry.is_flagged())
                        .count();
                    if count > 0 {
                        project.flagged_records = count;
                        needs_save = true;
                    }
                }
            }
        }

        // Save migrated data
        if needs_save {
            write_metadata_atomic(&meta_path, &projects)?;
        }

        Ok(Self {
            root_dir,
            meta_path,
            inner: Arc::new(Mutex::new(projects)),
            revisions: Arc::new(Mutex::new(HashMap::new())),
            flagged_overrides: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Counts that were committed to the annotation db but could not be
    /// persisted in `projects.json`. They keep the current session and later
    /// successful metadata writes from drifting; the next process recreates
    /// them from the annotation db when needed.
    pub fn set_flagged_records_override(&self, id: &Uuid, count: usize) {
        self.flagged_overrides.lock().insert(*id, count);
    }

    fn apply_flagged_overrides(&self, projects: &mut [ProjectMeta]) {
        let overrides = self.flagged_overrides.lock();
        for meta in projects {
            if let Some(count) = overrides.get(&meta.id) {
                meta.flagged_records = *count;
            }
        }
    }

    pub fn all(&self) -> Vec<ProjectMeta> {
        let mut projects = self.inner.lock().clone();
        self.apply_flagged_overrides(&mut projects);
        projects
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

    /// Applies one metadata change only after the replacement file is written.
    /// A failed write leaves both the file and the in-memory list unchanged.
    fn update_meta(
        &self,
        id: &Uuid,
        update: impl FnOnce(&mut ProjectMeta),
    ) -> Result<()> {
        let mut guard = self.inner.lock();
        let mut next = guard.clone();
        self.apply_flagged_overrides(&mut next);
        let Some(meta) = next.iter_mut().find(|meta| &meta.id == id) else {
            return Ok(());
        };
        update(meta);
        self.persist_locked(&next)?;
        *guard = next;
        Ok(())
    }

    pub fn update_hidden_columns(&self, id: &Uuid, hidden_columns: Vec<String>) -> Result<()> {
        self.update_meta(id, |meta| meta.hidden_columns = hidden_columns)
    }

    pub fn update_flagged_records(&self, id: &Uuid, flagged_records: usize) -> Result<()> {
        let result = self.update_meta(id, |meta| meta.flagged_records = flagged_records);
        if result.is_ok() {
            self.flagged_overrides.lock().remove(id);
        }
        result
    }

    pub fn update_ioc_applied_records(&self, id: &Uuid, count: usize) -> Result<()> {
        self.update_meta(id, |meta| meta.ioc_applied_records = count)
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
        let mut next = guard.clone();
        self.apply_flagged_overrides(&mut next);
        next.retain(|meta| &meta.id != id);
        self.persist_locked(&next)?;
        *guard = next;
        self.revisions.lock().remove(id);
        self.flagged_overrides.lock().remove(id);
        Ok(())
    }

    pub fn find(&self, id: &Uuid) -> Option<ProjectMeta> {
        let mut meta = self
            .inner
            .lock()
            .iter()
            .find(|meta| &meta.id == id)
            .cloned()?;
        if let Some(count) = self.flagged_overrides.lock().get(id) {
            meta.flagged_records = *count;
        }
        Some(meta)
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
        write_metadata_atomic(&self.meta_path, guard)
    }
}

/// Replaces the project list atomically. A failed write must not leave a
/// truncated metadata file that loses the existing projects on restart.
fn write_metadata_atomic(meta_path: &Path, projects: &[ProjectMeta]) -> Result<()> {
    let data = serde_json::to_vec_pretty(projects)?;
    let temp_path = meta_path.with_extension(format!("json.tmp.{}", std::process::id()));
    let result = fs::write(&temp_path, data)
        .with_context(|| format!("failed to write metadata file {:?}", temp_path))
        .and_then(|_| {
            fs::rename(&temp_path, meta_path).with_context(|| {
                format!(
                    "failed to replace metadata file {:?} with {:?}",
                    meta_path, temp_path
                )
            })
        });
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

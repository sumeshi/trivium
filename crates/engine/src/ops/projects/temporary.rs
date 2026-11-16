use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::Context;
use uuid::Uuid;

use crate::{error::AppError, store::ProjectsStore};

const MARKER: &str = ".trivium-extract-result";
const CONTENT: &[u8; 26] = b"trivium-extract-result-v1\n";
const SOURCE: &str = "source.csv";

/// Check an owned marker, not a user-editable name/description or filename.
/// Ordinary case discovery and addressing remain unchanged.
pub fn is_temporary_project(store: &ProjectsStore, id: &Uuid) -> bool {
    let Ok(mut marker) = File::open(store.project_dir(id).join(MARKER)) else {
        return false;
    };
    if marker
        .metadata()
        .ok()
        .is_none_or(|meta| meta.len() != CONTENT.len() as u64)
    {
        return false;
    }
    let mut contents = [0; CONTENT.len()];
    marker.read_exact(&mut contents).is_ok() && contents == *CONTENT
}

pub(super) fn source_path(store: &ProjectsStore, id: &Uuid) -> Result<PathBuf, AppError> {
    let case = store.project_dir(id);
    let projects = case.parent().expect("a project directory has a parent");
    Ok(fs::canonicalize(projects)
        .context("failed to resolve extraction directory")?
        .join(id.to_string())
        .join(SOURCE))
}

pub(super) fn retain_source(staging: &Path, copied: &Path) -> Result<(), AppError> {
    fs::rename(copied, staging.join(SOURCE)).context("failed to retain extracted CSV")?;
    fs::write(staging.join(MARKER), CONTENT).context("failed to mark temporary extraction")?;
    Ok(())
}

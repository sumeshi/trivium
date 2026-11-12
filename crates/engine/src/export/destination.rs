//! Protect evidence and every case-managed file before opening an output.
use crate::{error::AppError, models::ProjectMeta, store::ProjectsStore};
use anyhow::Context;
use std::{
    collections::HashSet,
    fs, io,
    path::{Component, Path, PathBuf},
};

fn normalize(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            other => result.push(other.as_os_str()),
        }
    }
    result
}

/// Resolve an existing ancestor even if the destination's directory does not
/// exist yet. Canonicalization follows existing symlinks before folding `..`.
fn canonical_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut ancestor = absolute.as_path();
    let mut suffix = Vec::new();
    loop {
        match fs::canonicalize(ancestor) {
            Ok(mut resolved) => {
                for component in suffix.iter().rev() {
                    resolved.push(component);
                }
                return Ok(normalize(&resolved));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let component = ancestor
            .components()
            .next_back()
            .ok_or_else(|| io::Error::other("Cannot resolve output path"))?;
        suffix.push(component.as_os_str().to_os_string());
        ancestor = ancestor
            .parent()
            .ok_or_else(|| io::Error::other("Cannot resolve output parent"))?;
    }
}

pub(crate) fn ensure_destination_is_not_source(
    destination: &Path,
    meta: &ProjectMeta,
) -> Result<(), AppError> {
    if let Some(source) = &meta.source_path {
        let source = Path::new(source);
        let same_path = canonical_path(source)
            .ok()
            .zip(canonical_path(destination).ok())
            .is_some_and(|(a, b)| a == b);
        let same_file = source.exists()
            && destination.exists()
            && same_file::is_same_file(source, destination)
                .context("failed to compare evidence/output identity")?;
        if same_path || same_file {
            return Err(AppError::Message(
                "Choose a different file name: the destination is the source evidence file.".into(),
            ));
        }
    }
    Ok(())
}

/// The projects/staging trees are application storage, not user output
/// directories. Blocking the whole tree also covers WAL files, legacy case
/// copies, migrations and future managed files without a fragile name list.
pub(crate) fn ensure_destination_is_not_managed(
    destination: &Path,
    store: &ProjectsStore,
) -> Result<(), AppError> {
    let project = store.project_dir(&uuid::Uuid::nil());
    let projects = project.parent().expect("project directory has parent");
    let staging = store.staging_root();
    let registry = projects
        .parent()
        .expect("projects directory has parent")
        .join("projects.json");
    let destination_path = canonical_path(destination).context("failed to resolve output path")?;
    for directory in [projects, staging.as_path()] {
        let path = canonical_path(directory).context("failed to resolve case storage")?;
        if destination_path.starts_with(path) {
            return Err(managed_error());
        }
    }
    if destination_path == canonical_path(&registry).context("failed to resolve case registry")? {
        return Err(managed_error());
    }
    if !destination.exists() {
        return Ok(());
    }
    let destination =
        same_file::Handle::from_path(destination).context("failed to inspect output identity")?;
    let mut pending = vec![projects.to_path_buf(), staging];
    let mut visited = HashSet::new();
    pending.push(registry);
    while let Some(path) = pending.pop() {
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(anyhow::Error::new(error)
                    .context("failed to inspect managed output guard")
                    .into())
            }
        };
        if metadata.is_dir() {
            if !visited
                .insert(fs::canonicalize(&path).context("failed to resolve managed directory")?)
            {
                continue;
            }
            for entry in fs::read_dir(&path).context("failed to inspect managed directory")? {
                pending.push(entry.context("failed to inspect managed entry")?.path());
            }
        } else if metadata.is_file() {
            match same_file::Handle::from_path(&path) {
                Ok(handle) if handle == destination => return Err(managed_error()),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(anyhow::Error::new(error)
                        .context("failed to inspect managed file identity")
                        .into())
                }
            }
        }
    }
    Ok(())
}
fn managed_error() -> AppError {
    AppError::Message(
        "Choose a different file name: the destination is managed case storage.".into(),
    )
}

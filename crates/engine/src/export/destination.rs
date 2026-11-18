//! Require explicit overwrite consent and protect every case-managed file.
use crate::{error::AppError, store::ProjectsStore};
use anyhow::Context;
use std::{
    collections::HashSet,
    fs, io,
    path::{Component, Path, PathBuf},
};

/// The validator is the single source of overwrite authorization for the
/// final filesystem operation; a preceding existence check is not a lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublishMode {
    CreateNew,
    Replace,
}

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

/// Existing outputs, including original evidence files, require explicit
/// consent. Application storage remains protected even when consent is given.
/// Re-run immediately before replacement to catch a newly created destination.
pub(crate) fn validate_destination(
    destination: &Path,
    store: &ProjectsStore,
    allow_overwrite: bool,
) -> Result<PublishMode, AppError> {
    ensure_destination_is_not_managed(destination, store)?;
    let mode = if allow_overwrite {
        PublishMode::Replace
    } else {
        PublishMode::CreateNew
    };
    match fs::symlink_metadata(destination) {
        Ok(_) if !allow_overwrite => Err(AppError::Message(format!(
            "Overwrite confirmation required: {}",
            destination.display()
        ))),
        Ok(_) => Ok(mode),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(mode),
        Err(error) => Err(anyhow::Error::new(error)
            .context("failed to inspect output destination")
            .into()),
    }
}

/// Managed case and configuration trees are not export directories. Desktop
/// preference paths are configured explicitly; a CLI store's parent stays usable.
pub(crate) fn ensure_destination_is_not_managed(
    destination: &Path,
    store: &ProjectsStore,
) -> Result<(), AppError> {
    let (directories, files) = store.managed_output_paths();
    let destination_path = canonical_path(destination).context("failed to resolve output path")?;
    for directory in &directories {
        let path = canonical_path(directory).context("failed to resolve managed storage")?;
        if matches_managed_path(&destination_path, &path, true) {
            return Err(managed_error());
        }
    }
    for file in &files {
        let path = canonical_path(file).context("failed to resolve managed setting")?;
        if matches_managed_path(&destination_path, &path, false) {
            return Err(managed_error());
        }
    }
    if !destination.exists() {
        return Ok(());
    }
    let destination =
        same_file::Handle::from_path(destination).context("failed to inspect output identity")?;
    let mut pending = directories;
    pending.extend(files);
    let mut visited = HashSet::new();
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
fn matches_managed_path(destination: &Path, managed: &Path, directory: bool) -> bool {
    #[cfg(windows)]
    {
        let destination: Vec<_> = destination.components().collect();
        let managed: Vec<_> = managed.components().collect();
        (directory || destination.len() == managed.len())
            && destination.len() >= managed.len()
            && destination.iter().zip(managed).all(|(left, right)| {
                left.as_os_str()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(&right.as_os_str().to_string_lossy())
            })
    }
    #[cfg(not(windows))]
    {
        if directory {
            destination.starts_with(managed)
        } else {
            destination == managed
        }
    }
}

fn managed_error() -> AppError {
    AppError::Message(
        "Choose a different file name: the destination is managed case storage.".into(),
    )
}

#[cfg(test)]
mod tests;

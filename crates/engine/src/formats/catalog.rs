//! Built-in and managed user definitions share the same validated format schema.
use std::{
    collections::HashSet,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use serde::{Deserialize, Serialize};

use crate::{error::AppError, storage::write_file_atomic};

use super::{CompiledFormat, FormatDefinition};

include!(concat!(env!("OUT_DIR"), "/builtin_formats.rs"));

const MAX_FORMAT_BYTES: u64 = 256 * 1024;
const MAX_CUSTOM_FORMATS: usize = 128;

#[derive(Clone, Debug, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct FormatCatalogEntry {
    pub definition: FormatDefinition,
    pub builtin: bool,
}

#[derive(Debug, Deserialize, ts_rs::TS)]
pub struct SaveFormatPayload {
    pub definition: FormatDefinition,
}

#[derive(Debug, Deserialize, ts_rs::TS)]
pub struct DeleteFormatPayload {
    pub id: String,
}

fn error(message: impl Into<String>) -> AppError {
    AppError::Message(message.into())
}

/// IDs are file names too; keep them portable between Linux and Windows.
pub fn validate_format_id(id: &str) -> Result<(), AppError> {
    let device = matches!(id, "con" | "prn" | "aux" | "nul")
        || ["com", "lpt"].iter().any(|prefix| {
            id.strip_prefix(prefix).is_some_and(|suffix| {
                suffix.len() == 1 && matches!(suffix.as_bytes()[0], b'1'..=b'9')
            })
        });
    if id.is_empty()
        || id.len() > 64
        || !id.as_bytes()[0].is_ascii_lowercase()
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
        || device
    {
        return Err(error("Format ID must start with a lowercase letter and contain at most 64 lowercase letters, digits, '-' or '_'; device names are not allowed."));
    }
    Ok(())
}

fn builtins() -> Result<&'static [FormatDefinition], AppError> {
    static DEFINITIONS: OnceLock<Result<Vec<FormatDefinition>, String>> = OnceLock::new();
    DEFINITIONS
        .get_or_init(|| {
            let mut ids = HashSet::new();
            BUILTIN_FORMAT_JSON
                .iter()
                .map(|(id, source)| {
                    let definition: FormatDefinition = serde_json::from_str(source)
                        .map_err(|error| format!("Invalid built-in format {id}: {error}"))?;
                    validate_format_id(&definition.id).map_err(|error| error.to_string())?;
                    if definition.id != *id || !ids.insert(definition.id.clone()) {
                        return Err(format!(
                            "Built-in format ID must match its unique file name: {id}"
                        ));
                    }
                    CompiledFormat::new(&definition)
                        .map_err(|error| format!("Invalid built-in format {id}: {error}"))?;
                    Ok(definition)
                })
                .collect()
        })
        .as_deref()
        .map_err(|message| error(message.clone()))
}

fn directory(root: &Path, create: bool) -> Result<bool, AppError> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(error(
            "The formats directory must be an ordinary directory.",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !create => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(root).map_err(|error| AppError::Other(error.into()))?;
            directory(root, false)
        }
        Err(error) => Err(AppError::Other(error.into())),
    }
}

fn custom_paths(root: &Path) -> Result<Vec<PathBuf>, AppError> {
    if !directory(root, false)? {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(root).map_err(|error| AppError::Other(error.into()))? {
        let path = entry.map_err(|error| AppError::Other(error.into()))?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            ordinary_file(&path)?;
            paths.push(path);
            if paths.len() > MAX_CUSTOM_FORMATS {
                return Err(error("Too many custom formats (limit: 128)."));
            }
        }
    }
    paths.sort();
    Ok(paths)
}

fn ordinary_file(path: &Path) -> Result<(), AppError> {
    if fs::symlink_metadata(path)
        .map_err(|error| AppError::Other(error.into()))?
        .file_type()
        .is_file()
    {
        Ok(())
    } else {
        Err(error(
            "Custom format definitions must be ordinary JSON files.",
        ))
    }
}

fn custom_definition(path: &Path) -> Result<FormatDefinition, AppError> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|error| AppError::Other(error.into()))?
        .take(MAX_FORMAT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| AppError::Other(error.into()))?;
    if bytes.len() as u64 > MAX_FORMAT_BYTES {
        return Err(error("Custom format definition exceeds 256 KiB."));
    }
    let definition: FormatDefinition = serde_json::from_slice(&bytes)
        .map_err(|error| self::error(format!("Invalid custom format JSON: {error}")))?;
    validate_format_id(&definition.id)?;
    if path.file_stem().and_then(|name| name.to_str()) != Some(definition.id.as_str()) {
        return Err(error("Custom format ID must match its file name."));
    }
    CompiledFormat::new(&definition)?;
    Ok(definition)
}

/// The root is the app's managed formats directory, not a user-supplied path.
pub fn list_formats(root: &Path) -> Result<Vec<FormatCatalogEntry>, AppError> {
    let mut entries: Vec<_> = builtins()?
        .iter()
        .cloned()
        .map(|definition| FormatCatalogEntry {
            definition,
            builtin: true,
        })
        .collect();
    let mut ids: HashSet<_> = entries
        .iter()
        .map(|entry| entry.definition.id.clone())
        .collect();
    for path in custom_paths(root)? {
        let definition = custom_definition(&path)?;
        if !ids.insert(definition.id.clone()) {
            return Err(error(format!(
                "Custom format cannot replace a built-in format: {}",
                definition.id
            )));
        }
        entries.push(FormatCatalogEntry {
            definition,
            builtin: false,
        });
    }
    Ok(entries)
}

pub fn save_format(
    root: &Path,
    definition: &FormatDefinition,
) -> Result<FormatCatalogEntry, AppError> {
    validate_format_id(&definition.id)?;
    CompiledFormat::new(definition)?;
    if builtins()?
        .iter()
        .any(|builtin| builtin.id == definition.id)
    {
        return Err(error(
            "Built-in formats cannot be replaced. Save a copy with another ID.",
        ));
    }
    let bytes =
        serde_json::to_vec_pretty(definition).map_err(|error| AppError::Other(error.into()))?;
    if bytes.len() as u64 > MAX_FORMAT_BYTES {
        return Err(error("Custom format definition exceeds 256 KiB."));
    }
    directory(root, true)?;
    let path = root.join(format!("{}.json", definition.id));
    match fs::symlink_metadata(&path) {
        Ok(_) => ordinary_file(&path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if custom_paths(root)?.len() >= MAX_CUSTOM_FORMATS {
                return Err(self::error("Too many custom formats (limit: 128)."));
            }
        }
        Err(error) => return Err(AppError::Other(error.into())),
    }
    write_file_atomic(&path, &bytes).map_err(AppError::from)?;
    Ok(FormatCatalogEntry {
        definition: definition.clone(),
        builtin: false,
    })
}

pub fn delete_format(root: &Path, id: &str) -> Result<(), AppError> {
    validate_format_id(id)?;
    if builtins()?.iter().any(|builtin| builtin.id == id) {
        return Err(error("Built-in formats cannot be deleted."));
    }
    if !directory(root, false)? {
        return Ok(());
    }
    let path = root.join(format!("{id}.json"));
    match fs::symlink_metadata(&path) {
        Ok(_) => {
            ordinary_file(&path)?;
            fs::remove_file(path).map_err(|error| AppError::Other(error.into()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(AppError::Other(error.into())),
    }
}

#[cfg(test)]
mod tests;

//! Theme profiles are saved only in the managed catalog, without renderer paths.
use std::{fs, path::Path};

use tauri::Manager;
use trivium_engine::{error::AppError, storage::write_file_atomic};
use uuid::Uuid;

use super::storage_worker::run_storage;

const MAX_THEME_BYTES: usize = 65_536;
const MAX_USER_THEMES: usize = 128;

fn error(message: impl Into<String>) -> AppError {
    AppError::Message(message.into())
}

fn validate_profile(id: &str, text: &str) -> Result<(), AppError> {
    let device = matches!(id, "con" | "prn" | "aux" | "nul")
        || ["com", "lpt"].iter().any(|prefix| {
            id.strip_prefix(prefix).is_some_and(|suffix| {
                suffix.len() == 1 && matches!(suffix.as_bytes()[0], b'1'..=b'9')
            })
        });
    if id.is_empty()
        || id.len() > 64
        || !id.as_bytes()[0].is_ascii_alphanumeric()
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
        || device
        || matches!(id, "constructor" | "prototype")
    {
        return Err(error("Invalid theme ID."));
    }
    if text.len() > MAX_THEME_BYTES {
        return Err(error("Theme JSON must be 64 KiB or smaller."));
    }
    let definition: serde_json::Value =
        serde_json::from_str(text).map_err(|_| error("Invalid theme JSON."))?;
    if !definition.is_object()
        || definition.get("type").and_then(|value| value.as_str()) != Some("trivium-theme")
        || definition.get("version").and_then(|value| value.as_u64()) != Some(1)
        || definition.get("id").and_then(|value| value.as_str()) != Some(id)
    {
        return Err(error(
            "Expected a version 1 Trivium theme with the same ID.",
        ));
    }
    Ok(())
}

fn save_profile(directory: &Path, id: &str, text: &str) -> Result<(), AppError> {
    validate_profile(id, text)?;
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Err(error("The theme folder must be an ordinary directory.")),
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(directory).map_err(|error| AppError::Other(error.into()))?;
        }
        Err(failure) => return Err(AppError::Other(failure.into())),
    }
    let destination = directory.join(format!("{id}.json"));
    let replacing = match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_file() => true,
        Ok(_) => return Err(error("A theme profile must be an ordinary JSON file.")),
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => false,
        Err(failure) => return Err(AppError::Other(failure.into())),
    };
    if !replacing {
        let mut count = 0;
        for entry in fs::read_dir(directory).map_err(|error| AppError::Other(error.into()))? {
            let entry = entry.map_err(|error| AppError::Other(error.into()))?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json")
                && entry
                    .file_type()
                    .map_err(|error| AppError::Other(error.into()))?
                    .is_file()
            {
                count += 1;
                if count >= MAX_USER_THEMES {
                    return Err(error("The theme folder already contains 128 theme files. Replace an existing theme to import another."));
                }
            }
        }
    }
    // Preserve the validated JSON bytes; only publish after its temporary write succeeds.
    write_file_atomic(&destination, text.as_bytes()).map_err(AppError::Other)
}

#[tauri::command]
pub async fn save_theme_profile(
    app: tauri::AppHandle,
    id: String,
    text: String,
) -> Result<(), String> {
    let directory = app
        .path()
        .app_local_data_dir()
        .map_err(|error| format!("Could not locate theme storage: {error}"))?
        .join("themes");
    run_storage(Uuid::nil(), move || save_profile(&directory, &id, &text)).await
}

#[cfg(test)]
mod tests;

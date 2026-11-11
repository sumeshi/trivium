use std::fs;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use serde::Deserialize;
use uuid::Uuid;

use crate::error::AppError;
use crate::models::{ViewState, VIEW_STATE_VERSION};
use crate::store::ProjectsStore;

const VIEW_STATE_FILE: &str = "view_state.json";

#[derive(Debug, Deserialize)]
pub struct ViewStateRequest {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct SaveViewStatePayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub state: ViewState,
}

fn view_state_path(store: &ProjectsStore, project_id: &Uuid) -> PathBuf {
    store.project_dir(project_id).join(VIEW_STATE_FILE)
}

/// Loads the reusable view state for one project.
///
/// A missing file is normal for a new or legacy project. A broken file or a
/// file written by a newer version is ignored (and left on disk) so the
/// project, its evidence, and its annotations still open.
pub fn load_view_state(
    store: &ProjectsStore,
    request: ViewStateRequest,
) -> Result<Option<ViewState>, AppError> {
    if store.find(&request.project_id).is_none() {
        return Err(AppError::Message("Project not found.".into()));
    }
    let path = view_state_path(store, &request.project_id);
    let data = match fs::read(&path) {
        Ok(data) => data,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(AppError::from(
                anyhow::Error::new(err).context(format!("failed to read view state {:?}", path)),
            ))
        }
    };

    let state: ViewState = match serde_json::from_slice(&data) {
        Ok(state) => state,
        Err(err) => {
            eprintln!(
                "[view] ignoring unreadable view state for {:?}: {:?}",
                path, err
            );
            return Ok(None);
        }
    };

    if state.version > VIEW_STATE_VERSION {
        eprintln!(
            "[view] ignoring newer view state version {} (supported {}) for {:?}",
            state.version, VIEW_STATE_VERSION, path
        );
        return Ok(None);
    }

    Ok(Some(state))
}

/// Writes the view state through a sibling temporary file.
///
/// The annotation database is not touched. A failed write removes only the
/// temporary file and leaves the previous view state (and annotations) intact.
pub fn save_view_state(
    store: &ProjectsStore,
    payload: SaveViewStatePayload,
) -> Result<(), AppError> {
    if store.find(&payload.project_id).is_none() {
        return Err(AppError::Message("Project not found.".into()));
    }

    let path = view_state_path(store, &payload.project_id);
    let parent = path
        .parent()
        .ok_or_else(|| AppError::Message("Project directory missing.".into()))?;
    let temp_path = parent.join(format!(
        "{}.tmp.{}.{}",
        VIEW_STATE_FILE,
        std::process::id(),
        Uuid::new_v4()
    ));

    let result = write_and_replace(&path, &temp_path, &payload.state);
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result.map_err(AppError::from)
}

fn write_and_replace(path: &PathBuf, temp_path: &PathBuf, state: &ViewState) -> Result<()> {
    let data = serde_json::to_vec_pretty(state).context("failed to serialize view state")?;
    fs::write(temp_path, data)
        .with_context(|| format!("failed to write view state {:?}", temp_path))?;
    fs::rename(temp_path, path)
        .with_context(|| format!("failed to replace view state {:?}", path))?;
    Ok(())
}

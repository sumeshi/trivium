use anyhow::{bail, Context as _, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::AppError;
use crate::models::{ViewState, VIEW_STATE_VERSION};
use crate::store::ProjectsStore;

/// Result of reading a project's view state.
///
/// `status` distinguishes a genuinely missing file (`missing`) from a file the
/// frontend must not overwrite (`unreadable` or `newer`). The caller can show
/// defaults without immediately replacing the original file.
#[derive(Debug, Serialize, ts_rs::TS)]
pub struct ViewStateLoad {
    pub state: Option<ViewState>,
    pub status: String,
}

const VIEW_STATE_KEY: &str = "view_state";

#[derive(Debug, Deserialize, ts_rs::TS)]
pub struct ViewStateRequest {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
}

#[derive(Debug, Deserialize, ts_rs::TS)]
pub struct SaveViewStatePayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub state: ViewState,
}

/// Loads the reusable view state for one project.
///
/// A missing file is normal for a new or legacy project. A broken file or a
/// file written by a newer version is ignored (and left on disk) so the
/// project, its evidence, and its annotations still open.
pub fn load_view_state(
    store: &ProjectsStore,
    request: ViewStateRequest,
) -> Result<ViewStateLoad, AppError> {
    if store
        .try_find(&request.project_id)
        .map_err(AppError::from)?
        .is_none()
    {
        return Err(AppError::Message("Project not found.".into()));
    }
    let path = store.project_dir(&request.project_id);
    let Some(data) =
        crate::storage::load_setting_bytes(&path, VIEW_STATE_KEY).map_err(AppError::from)?
    else {
        return Ok(ViewStateLoad {
            state: None,
            status: "missing".to_string(),
        });
    };

    let state: ViewState = match serde_json::from_slice(&data) {
        Ok(state) => state,
        Err(err) => {
            eprintln!(
                "[view] ignoring unreadable view state for {:?}: {:?}",
                path, err
            );
            return Ok(ViewStateLoad {
                state: None,
                status: "unreadable".to_string(),
            });
        }
    };

    if state.version > VIEW_STATE_VERSION {
        eprintln!(
            "[view] ignoring newer view state version {} (supported {}) for {:?}",
            state.version, VIEW_STATE_VERSION, path
        );
        return Ok(ViewStateLoad {
            state: None,
            status: "newer".to_string(),
        });
    }

    Ok(ViewStateLoad {
        state: Some(state),
        status: "ok".to_string(),
    })
}

/// Persists the view atomically, preserving unreadable and newer saved state.
pub fn save_view_state(
    store: &ProjectsStore,
    payload: SaveViewStatePayload,
) -> Result<(), AppError> {
    if store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
        .is_none()
    {
        return Err(AppError::Message("Project not found.".into()));
    }
    let bytes = serde_json::to_vec(&payload.state)
        .context("failed to serialize view state")
        .map_err(AppError::from)?;
    crate::storage::save_setting_bytes_checked(
        &store.project_dir(&payload.project_id),
        VIEW_STATE_KEY,
        &bytes,
        |existing| {
            if let Some(existing) = existing {
                match serde_json::from_slice::<ViewState>(existing) {
                    Ok(state) if state.version > VIEW_STATE_VERSION => bail!(
                        "Existing view state uses a newer version ({}); not overwriting.",
                        state.version
                    ),
                    Ok(_) => {}
                    Err(_) => bail!("Existing view state is unreadable; not overwriting."),
                }
            }
            Ok(())
        },
    )
    .map_err(AppError::from)
}

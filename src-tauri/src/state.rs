use std::fs;

use anyhow::{Context, Result};

pub struct AppState {
    pub projects: trivium_engine::store::ProjectsStore,
}

impl AppState {
    pub fn new(app: &tauri::App<tauri::Wry>) -> Result<Self> {
        let base_dir = tauri::api::path::app_local_data_dir(&app.config())
            .context("failed to resolve app data dir")?
            .join("trivium");
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("failed to create app data dir {:?}", base_dir))?;
        let projects = trivium_engine::store::ProjectsStore::new(base_dir)?;
        Ok(Self { projects })
    }
}

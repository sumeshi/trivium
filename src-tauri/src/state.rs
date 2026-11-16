use std::fs;

use anyhow::{Context, Result};
use tauri::Manager;

pub struct AppState {
    pub projects: trivium_engine::store::ProjectsStore,
}

impl AppState {
    pub fn new(app: &tauri::App<tauri::Wry>) -> Result<Self> {
        let preferences_root = app
            .path()
            .app_local_data_dir()
            .context("failed to resolve app data dir")?;
        let base_dir = preferences_root.join("trivium");
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("failed to create app data dir {:?}", base_dir))?;
        let projects = trivium_engine::store::ProjectsStore::new(base_dir)?
            .with_preferences_root(preferences_root);
        Ok(Self { projects })
    }
}

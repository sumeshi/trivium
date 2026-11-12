use std::fs;

use anyhow::{Context, Result};
use tauri::Manager;

pub struct AppState {
    pub projects: trivium_engine::store::ProjectsStore,
}

impl AppState {
    pub fn new(app: &tauri::App<tauri::Wry>) -> Result<Self> {
        // Windows known folders ignore APPDATA/LOCALAPPDATA overrides. Native
        // automation must use its own compiled namespace before opening cases.
        #[cfg(all(target_os = "windows", debug_assertions))]
        validate_native_test_namespace(
            std::env::var("TAURI_WEBVIEW_AUTOMATION").as_deref() == Ok("true"),
            &app.config().identifier,
        )?;
        let base_dir = app
            .path()
            .app_local_data_dir()
            .context("failed to resolve app data dir")?
            .join("trivium");
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("failed to create app data dir {:?}", base_dir))?;
        let projects = trivium_engine::store::ProjectsStore::new(base_dir)?;
        Ok(Self { projects })
    }
}

#[cfg(any(all(target_os = "windows", debug_assertions), test))]
fn validate_native_test_namespace(automation: bool, identifier: &str) -> Result<()> {
    if automation && identifier != "com.trivium.native-tests" {
        anyhow::bail!("Windows native automation requires the dedicated com.trivium.native-tests build; production case storage was not opened. Set TAURI_CONFIG to the native-test identifier before cargo build.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_native_test_namespace;

    #[test]
    fn automation_rejects_the_normal_case_namespace() {
        assert!(validate_native_test_namespace(true, "com.trivium.app").is_err());
    }

    #[test]
    fn automation_accepts_only_the_dedicated_namespace() {
        assert!(validate_native_test_namespace(true, "com.trivium.native-tests").is_ok());
        assert!(validate_native_test_namespace(true, "com.trivium.other").is_err());
    }

    #[test]
    fn normal_app_launch_retains_its_existing_namespace() {
        assert!(validate_native_test_namespace(false, "com.trivium.app").is_ok());
    }
}

//! Explicit WebView2 settings for the owned Windows debug automation window.
//! This module is absent from production builds and never changes user cases.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tauri::utils::config::WindowConfig;

pub struct NativeWindow {
    config: WindowConfig,
    profile: PathBuf,
    arguments: String,
}

fn settings(
    automation: bool,
    identifier: &str,
    window: &WindowConfig,
    port: Option<&str>,
    profile: Option<&Path>,
) -> Result<Option<NativeWindow>> {
    if !automation {
        return Ok(None);
    }
    // Windows known folders ignore APPDATA/LOCALAPPDATA overrides. Reject the
    // normal namespace before any configured WebView or case store is opened.
    if identifier != "com.trivium.native-tests" {
        bail!("Windows native automation requires the dedicated com.trivium.native-tests build; production case storage was not opened.");
    }
    let port = port
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|value| *value != 0)
        .context("Windows native automation requires TRIVIUM_NATIVE_DEBUG_PORT in 1..=65535")?;
    let profile = profile
        .filter(|value| value.is_absolute())
        .context("Windows native automation requires an absolute TRIVIUM_NATIVE_PROFILE")?;
    // Preserve Wry's normal WebView2 feature defaults when no custom arguments
    // were configured. These options go through the WebView2 API, not env aliases.
    let base = window
        .additional_browser_args
        .as_deref()
        .unwrap_or("--disable-features=msWebOOUI,msPdfOOUI,msSmartScreenProtection");
    Ok(Some(NativeWindow {
        config: window.clone(),
        profile: profile.to_owned(),
        arguments: format!(
            "{base} --remote-debugging-address=127.0.0.1 --remote-debugging-port={port}"
        ),
    }))
}

// Also type-check the Tauri builder API during host-side unit checks.
#[cfg_attr(test, allow(dead_code))]
pub fn prepare(
    mut context: tauri::Context<tauri::Wry>,
) -> Result<(tauri::Context<tauri::Wry>, Option<NativeWindow>)> {
    if std::env::var("TAURI_WEBVIEW_AUTOMATION").as_deref() != Ok("true") {
        return Ok((context, None));
    }
    let config = context.config_mut();
    let window = config
        .app
        .windows
        .iter_mut()
        .find(|window| window.label == "main")
        .context("Windows native automation requires the configured main window")?;
    let profile = std::env::var_os("TRIVIUM_NATIVE_PROFILE").map(PathBuf::from);
    let native = settings(
        true,
        &config.identifier,
        window,
        std::env::var("TRIVIUM_NATIVE_DEBUG_PORT").ok().as_deref(),
        profile.as_deref(),
    )?;
    if native.is_some() {
        window.create = false;
    }
    Ok((context, native))
}

impl NativeWindow {
    #[cfg_attr(test, allow(dead_code))]
    pub fn create(self, app: &tauri::App<tauri::Wry>) -> Result<()> {
        tauri::WebviewWindowBuilder::from_config(app, &self.config)?
            .additional_browser_args(&self.arguments)
            .data_directory(self.profile)
            .build()
            .context("failed to create the Windows native automation WebView")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> PathBuf {
        std::env::temp_dir().join("trivium-native-test-profile")
    }

    #[test]
    fn normal_debug_launch_ignores_automation_settings() {
        assert!(settings(
            false,
            "com.trivium.app",
            &WindowConfig::default(),
            None,
            None
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn automation_rejects_the_normal_namespace_before_opening_a_window() {
        for identifier in ["com.trivium.app", "com.trivium.other"] {
            assert!(settings(
                true,
                identifier,
                &WindowConfig::default(),
                Some("12345"),
                Some(&profile())
            )
            .is_err());
        }
    }

    #[test]
    fn automation_rejects_missing_or_invalid_endpoint_and_profile() {
        let window = WindowConfig::default();
        for port in [None, Some("0"), Some("65536"), Some("not-a-port")] {
            assert!(settings(
                true,
                "com.trivium.native-tests",
                &window,
                port,
                Some(&profile())
            )
            .is_err());
        }
        for profile in [None, Some(Path::new("relative/profile"))] {
            assert!(settings(
                true,
                "com.trivium.native-tests",
                &window,
                Some("12345"),
                profile
            )
            .is_err());
        }
    }

    #[test]
    fn dedicated_window_preserves_its_config_and_gets_an_explicit_endpoint() {
        let window = WindowConfig {
            title: "Preserved window".into(),
            additional_browser_args: Some("--disable-features=Example".into()),
            ..WindowConfig::default()
        };
        let options = settings(
            true,
            "com.trivium.native-tests",
            &window,
            Some("12345"),
            Some(&profile()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(options.profile, profile());
        assert_eq!(options.config.title, "Preserved window");
        assert!(options.arguments.starts_with("--disable-features=Example "));
        assert!(options.arguments.ends_with("--remote-debugging-port=12345"));
        assert!(options
            .arguments
            .contains("--remote-debugging-address=127.0.0.1"));
    }
}

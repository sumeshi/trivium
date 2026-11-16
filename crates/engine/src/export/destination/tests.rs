use std::{fs, path::PathBuf};
use uuid::Uuid;

use super::*;

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    preferences: PathBuf,
}
impl Fixture {
    fn new(configured: bool) -> Self {
        let root = std::env::temp_dir().join(format!("trivium-managed-export-{}", Uuid::new_v4()));
        let preferences = root.join("desktop-preferences");
        let store = ProjectsStore::new(root.join("workspace")).unwrap();
        let store = if configured {
            store.with_preferences_root(preferences.clone())
        } else {
            store
        };
        Self {
            root,
            store,
            preferences,
        }
    }
    fn managed(&self) -> Vec<PathBuf> {
        vec![
            self.store.formats_dir().join("custom.json"),
            self.store
                .trash_root()
                .join("deleted-case")
                .join("project.sqlite"),
            self.preferences.join("themes").join("custom.json"),
            self.preferences.join("appearance").join("wallpaper.bin"),
            self.preferences.join("theme.json"),
            self.preferences.join("appearance.json"),
            self.preferences.join("table-text-size.json"),
        ]
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn explicit_overwrite_never_replaces_managed_formats_or_desktop_appearance_files() {
    let fixture = Fixture::new(true);
    for path in fixture.managed() {
        // Protect files before their first creation, not only files already on disk.
        assert!(validate_destination(&path, &fixture.store, true)
            .unwrap_err()
            .to_string()
            .contains("managed case storage"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"saved settings").unwrap();
        for consent in [false, true] {
            assert!(
                validate_destination(&path, &fixture.store, consent).is_err(),
                "{}",
                path.display()
            );
        }
        assert_eq!(fs::read(&path).unwrap(), b"saved settings");
    }
}

#[test]
fn hardlink_aliases_cannot_bypass_new_managed_directories_or_preferences() {
    let fixture = Fixture::new(true);
    for (index, path) in fixture.managed().into_iter().enumerate() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"managed bytes").unwrap();
        let alias = fixture.root.join(format!("ordinary-output-{index}.csv"));
        fs::hard_link(&path, &alias).unwrap();
        assert!(validate_destination(&alias, &fixture.store, true)
            .unwrap_err()
            .to_string()
            .contains("managed case storage"));
        assert_eq!(fs::read(alias).unwrap(), b"managed bytes");
    }
}

#[test]
fn cli_store_does_not_guess_a_preferences_root_or_block_ordinary_sibling_outputs() {
    let fixture = Fixture::new(false);
    for path in [
        fixture.root.join("theme.json"),
        fixture.root.join("themes/user.json"),
        fixture.preferences.join("appearance/wallpaper.bin"),
        fixture.root.join("workspace/theme.json"),
    ] {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"user output").unwrap();
        assert!(validate_destination(&path, &fixture.store, false)
            .unwrap_err()
            .to_string()
            .contains("Overwrite confirmation required"));
        assert!(
            validate_destination(&path, &fixture.store, true).is_ok(),
            "{}",
            path.display()
        );
    }
    assert!(validate_destination(
        &fixture.store.formats_dir().join("new.json"),
        &fixture.store,
        true
    )
    .is_err());
}

#[test]
fn a_configured_preferences_root_protects_only_owned_paths_and_allows_confirmed_outputs() {
    let fixture = Fixture::new(true);
    for path in [
        fixture.preferences.join("investigation.csv"),
        fixture.root.join("output.csv"),
    ] {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"ordinary output").unwrap();
        assert!(validate_destination(&path, &fixture.store, false).is_err());
        assert!(validate_destination(&path, &fixture.store, true).is_ok());
    }
    // Reusing or cloning the configured store retains the explicit protection.
    assert!(validate_destination(
        &fixture.preferences.join("theme.json"),
        &fixture.store.clone(),
        true
    )
    .is_err());
}

#[cfg(unix)]
#[test]
fn symlinked_files_and_directory_aliases_preserve_managed_storage() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new(true);
    let formats = fixture.store.formats_dir();
    fs::create_dir_all(&formats).unwrap();
    let directory_alias = fixture.root.join("format-alias");
    symlink(&formats, &directory_alias).unwrap();
    assert!(validate_destination(
        &directory_alias.join("new/nested.csv"),
        &fixture.store,
        true
    )
    .is_err());
    fs::create_dir_all(&fixture.preferences).unwrap();
    let setting = fixture.preferences.join("appearance.json");
    fs::write(&setting, b"appearance settings").unwrap();
    let file_alias = fixture.root.join("export.csv");
    symlink(&setting, &file_alias).unwrap();
    assert!(validate_destination(&file_alias, &fixture.store, true).is_err());
    assert_eq!(fs::read(setting).unwrap(), b"appearance settings");
    assert!(
        validate_destination(&formats.join("missing/../fresh.json"), &fixture.store, true).is_err()
    );
}

#[cfg(windows)]
#[test]
fn windows_managed_names_are_protected_before_creation_regardless_of_casing() {
    let fixture = Fixture::new(true);
    for path in [
        fixture.preferences.join("THEME.JSON"),
        fixture.preferences.join("THEMES/new.json"),
        fixture.root.join("workspace/FORMATS/new.json"),
    ] {
        assert!(
            validate_destination(&path, &fixture.store, true).is_err(),
            "{}",
            path.display()
        );
    }
}

//! Small user-selected JSON inputs must be bounded while they are read.

use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tauri::Manager;
use tauri_plugin_fs::FsExt;
use trivium_engine::source_file::open_regular;

#[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImportTextKind {
    Theme,
    Pattern,
}

impl ImportTextKind {
    fn limit(self) -> usize {
        match self {
            Self::Theme => 65_536,
            Self::Pattern => 1_048_576,
        }
    }
}

fn limit_error(limit: usize) -> String {
    let unit = if limit >= 1_048_576 {
        format!("{} MiB", limit / 1_048_576)
    } else {
        format!("{} KiB", limit / 1024)
    };
    format!("File must be {unit} or smaller.")
}

fn read_limited(reader: impl Read, limit: usize) -> Result<Vec<u8>, String> {
    // A source may grow after fstat. Never read or allocate the whole new size.
    let mut bytes = Vec::new();
    reader
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("Could not read import file: {error}"))?;
    if bytes.len() > limit {
        return Err(limit_error(limit));
    }
    Ok(bytes)
}

pub(super) fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, String> {
    // On Unix this opens with O_NONBLOCK, then checks the opened file itself.
    // A FIFO, including one swapped in after a path check, cannot hang open.
    let file =
        open_regular(path).map_err(|error| format!("Could not open import file: {error}"))?;
    let size = file
        .metadata()
        .map_err(|error| format!("Could not inspect import file: {error}"))?
        .len();
    if size > limit as u64 {
        return Err(limit_error(limit));
    }
    read_limited(file, limit)
}

fn read_text(path: &Path, kind: ImportTextKind) -> Result<String, String> {
    let text = String::from_utf8(read_bounded(path, kind.limit())?)
        .map_err(|_| "Choose a UTF-8 JSON file.".to_owned())?;
    // Accept an optional UTF-8 BOM before parsing the JSON definition.
    Ok(text.strip_prefix('\u{feff}').unwrap_or(&text).to_owned())
}

fn authorize_import(
    path: &Path,
    kind: ImportTextKind,
    app_data: &Path,
    selected: impl FnOnce(&Path) -> bool,
) -> Result<PathBuf, String> {
    let denied = || "Choose the import file in the file picker first.".to_owned();
    let path = path.canonicalize().map_err(|_| denied())?;
    let managed_theme = kind == ImportTextKind::Theme
        && path
            .extension()
            .is_some_and(|extension| extension == "json")
        && app_data
            .canonicalize()
            .is_ok_and(|directory| path.parent() == Some(directory.join("themes").as_path()));
    if managed_theme || selected(&path) {
        Ok(path)
    } else {
        Err(denied())
    }
}

#[tauri::command]
pub async fn read_import_text(
    app: tauri::AppHandle,
    path: String,
    kind: ImportTextKind,
) -> Result<String, String> {
    let app_data = app
        .path()
        .app_local_data_dir()
        .map_err(|_| "Could not locate theme storage.".to_owned())?;
    let scope = app.fs_scope();
    tauri::async_runtime::spawn_blocking(move || {
        let path = authorize_import(Path::new(&path), kind, &app_data, |path| {
            scope.is_allowed(path)
        })?;
        read_text(&path, kind)
    })
    .await
    .map_err(|error| format!("Import file task failed: {error}"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{self, Cursor};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("trivium-bounded-text-{}", Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn json_limits_are_fixed_and_unknown_kinds_are_rejected() {
        assert_eq!(ImportTextKind::Theme.limit(), 65_536);
        assert_eq!(ImportTextKind::Pattern.limit(), 1_048_576);
        assert!(serde_json::from_str::<ImportTextKind>("\"other\"").is_err());
    }

    #[test]
    fn text_preserves_unicode_and_accepts_a_utf8_bom() {
        let directory = Directory::new();
        let path = directory.0.join("theme.json");
        fs::write(&path, "\u{feff}{\"name\":\"紫\"}").unwrap();
        assert_eq!(
            read_text(&path, ImportTextKind::Theme).unwrap(),
            "{\"name\":\"紫\"}"
        );
        fs::write(&path, [0xff, 0xfe]).unwrap();
        assert!(read_text(&path, ImportTextKind::Theme)
            .unwrap_err()
            .contains("UTF-8"));
    }

    #[test]
    fn file_limit_counts_encoded_bytes_and_rejects_directories() {
        let directory = Directory::new();
        let path = directory.0.join("theme.json");
        fs::write(&path, "界".repeat(21_846)).unwrap();
        assert!(read_text(&path, ImportTextKind::Theme)
            .unwrap_err()
            .contains("64 KiB"));
        assert!(read_text(&directory.0, ImportTextKind::Theme).is_err());
        assert!(read_text(&path, ImportTextKind::Pattern).is_ok());
    }

    #[test]
    fn growing_input_is_rejected_after_at_most_one_byte_beyond_the_limit() {
        struct CountingReader<'a>(&'a AtomicUsize, Cursor<Vec<u8>>);
        impl Read for CountingReader<'_> {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let read = self.1.read(buffer)?;
                self.0.fetch_add(read, Ordering::SeqCst);
                Ok(read)
            }
        }
        let count = AtomicUsize::new(0);
        let source = CountingReader(&count, Cursor::new(vec![b' '; 4096]));
        assert!(read_limited(source, 64).is_err());
        assert_eq!(count.load(Ordering::SeqCst), 65);
    }

    #[test]
    fn text_import_requires_a_selected_file_except_for_managed_theme_profiles() {
        let directory = Directory::new();
        let app_data = directory.0.join("managed");
        let themes = app_data.join("themes");
        fs::create_dir_all(&themes).unwrap();
        let profile = themes.join("saved.json");
        let outside = directory.0.join("unselected.json");
        fs::write(&profile, "{}").unwrap();
        fs::write(&outside, "private fixture").unwrap();
        assert!(authorize_import(&outside, ImportTextKind::Theme, &app_data, |_| false).is_err());
        assert!(authorize_import(&outside, ImportTextKind::Pattern, &app_data, |_| false).is_err());
        assert_eq!(
            authorize_import(&profile, ImportTextKind::Theme, &app_data, |_| false).unwrap(),
            profile.canonicalize().unwrap()
        );
        assert!(authorize_import(&profile, ImportTextKind::Pattern, &app_data, |_| false).is_err());
        for kind in [ImportTextKind::Theme, ImportTextKind::Pattern] {
            assert_eq!(
                authorize_import(&outside, kind, &app_data, |path| {
                    path == outside.canonicalize().unwrap()
                })
                .unwrap(),
                outside.canonicalize().unwrap()
            );
        }
        let nested = themes.join("nested");
        fs::create_dir(&nested).unwrap();
        let nested_profile = nested.join("saved.json");
        fs::write(&nested_profile, "{}").unwrap();
        assert!(
            authorize_import(&nested_profile, ImportTextKind::Theme, &app_data, |_| false).is_err()
        );
        let non_json = themes.join("private.txt");
        fs::write(&non_json, "private fixture").unwrap();
        assert!(authorize_import(&non_json, ImportTextKind::Theme, &app_data, |_| false).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn managed_theme_import_rejects_symlinks_to_files_and_directories_outside_the_catalog() {
        use std::os::unix::fs::symlink;
        let directory = Directory::new();
        let app_data = directory.0.join("managed");
        let themes = app_data.join("themes");
        fs::create_dir_all(&themes).unwrap();
        let outside = directory.0.join("outside.json");
        fs::write(&outside, "private fixture").unwrap();
        let linked_profile = themes.join("linked.json");
        symlink(&outside, &linked_profile).unwrap();
        assert!(
            authorize_import(&linked_profile, ImportTextKind::Theme, &app_data, |_| false).is_err()
        );
        fs::remove_file(&linked_profile).unwrap();
        fs::remove_dir(&themes).unwrap();
        symlink(&directory.0, &themes).unwrap();
        assert!(authorize_import(
            &themes.join("outside.json"),
            ImportTextKind::Theme,
            &app_data,
            |_| false
        )
        .is_err());
    }
}

//! Small user-selected JSON inputs must be bounded while they are read.

use std::io::Read;
use std::path::Path;

use serde::Deserialize;
use trivium_engine::source_file::open_regular;

#[derive(Clone, Copy, Deserialize)]
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
    // Match the former browser TextDecoder's UTF-8 BOM handling.
    Ok(text.strip_prefix('\u{feff}').unwrap_or(&text).to_owned())
}

#[tauri::command]
pub async fn read_import_text(path: String, kind: ImportTextKind) -> Result<String, String> {
    tauri::async_runtime::spawn_blocking(move || read_text(Path::new(&path), kind))
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
}

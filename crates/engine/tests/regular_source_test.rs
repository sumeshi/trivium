//! Production imports reject non-file sources before copying any bytes.
use std::{fs, path::PathBuf};
use trivium_engine::{
    ops::projects::{create_project, CreateProjectPayload},
    store::ProjectsStore,
};
use uuid::Uuid;
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("trivium-import-source-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn payload(path: &std::path::Path) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().into(),
        description: None,
        delimiter: None,
        restore_annotations: None,
    }
}

#[test]
fn ordinary_import_still_copies_an_exact_read_only_snapshot() {
    let root = Fixture::new();
    let source = root.0.join("source.csv");
    let bytes = b"value\n18446744073709551615\n";
    fs::write(&source, bytes).unwrap();
    let store = ProjectsStore::new(root.0.join("store")).unwrap();
    let imported = create_project(&store, payload(&source)).unwrap();
    assert_eq!(imported.meta.total_records, 1);
    assert_eq!(imported.meta.source_size, Some(bytes.len() as u64));
    assert_eq!(fs::read(&source).unwrap(), bytes);
    assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
}

#[cfg(unix)]
#[test]
fn fifo_and_device_imports_leave_no_case_or_staged_snapshot() {
    use std::{os::unix::fs::FileTypeExt, process::Command};
    let root = Fixture::new();
    let fifo = root.0.join("pipe.csv");
    assert!(Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    let store = ProjectsStore::new(root.0.join("store")).unwrap();
    for source in [fifo.as_path(), std::path::Path::new("/dev/zero")] {
        let error = create_project(&store, payload(source)).unwrap_err();
        assert!(error.to_string().contains("not a regular file"), "{error}");
        assert!(store.try_all().unwrap().is_empty());
        assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
    }
    assert!(fs::metadata(&fifo).unwrap().file_type().is_fifo());
}

#[cfg(unix)]
#[test]
fn external_ioc_and_preset_readers_reject_named_pipes() {
    use std::process::Command;
    use trivium_engine::{
        ioc::read_ioc_csv,
        ops::preset::{load_preset, LoadPresetPayload},
    };
    let root = Fixture::new();
    let fifo = root.0.join("configuration.csv");
    assert!(Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    let store = ProjectsStore::new(root.0.join("store")).unwrap();
    assert!(read_ioc_csv(&fifo).is_err());
    assert!(load_preset(
        &store,
        LoadPresetPayload {
            project_id: None,
            source: fifo.to_string_lossy().into()
        }
    )
    .is_err());
}

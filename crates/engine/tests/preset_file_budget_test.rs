//! User-selected preset JSON is bounded before parsing or publishing output.
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
};

use trivium_engine::{
    ops::{
        preset::{load_preset, save_preset, LoadPresetPayload, SavePresetPayload},
        projects::{create_project, CreateProjectPayload},
    },
    store::ProjectsStore,
};

const MAX_BYTES: usize = 32 * 1024 * 1024;

fn write_padded(path: &Path, size: usize) {
    let mut file = File::create(path).unwrap();
    let json = r#"{"version":2,"name":"調査 雪"}"#.as_bytes();
    file.write_all(json).unwrap();
    io::copy(
        &mut io::repeat(b' ').take((size - json.len()) as u64),
        &mut file,
    )
    .unwrap();
}

fn load(store: &ProjectsStore, path: &Path) -> Result<String, String> {
    load_preset(
        store,
        LoadPresetPayload {
            project_id: None,
            source: path.to_string_lossy().into(),
        },
    )
    .map(|preset| preset.name.unwrap())
    .map_err(|error| error.to_string())
}

#[test]
fn unicode_presets_accept_only_one_file_prefix_utf8_bom() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let path = root.path().join("windows-editor.json");
    {
        let version = 2;
        let text = format!("\u{feff}\n{{\"version\":{version},\"name\":\"調査\u{feff}保存\"}}");
        fs::write(&path, &text).unwrap();
        assert_eq!(load(&store, &path).unwrap(), "調査\u{feff}保存");
        assert_eq!(fs::read_to_string(&path).unwrap(), text);
    }
    for text in [
        "\u{feff}\u{feff}{\"version\":2,\"name\":\"Double prefix\"}",
        " \u{feff}{\"version\":2,\"name\":\"After whitespace\"}",
        "{\"version\":2,\"name\":\"Trailing\"}\u{feff}",
    ] {
        fs::write(&path, text).unwrap();
        assert!(load(&store, &path)
            .unwrap_err()
            .contains("failed to parse preset"));
        assert_eq!(fs::read_to_string(&path).unwrap(), text);
    }
}

#[test]
fn exact_limit_current_presets_load_but_larger_valid_json_is_rejected_and_left_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let path = root.path().join("selected-log.json");
    write_padded(&path, MAX_BYTES);
    assert_eq!(load(&store, &path).unwrap(), "調査 雪");
    write_padded(&path, MAX_BYTES + 1);
    let before = fs::metadata(&path).unwrap();
    assert!(load(&store, &path).unwrap_err().contains("32 MiB"));
    let after = fs::metadata(&path).unwrap();
    assert_eq!(before.len(), after.len());
    assert_eq!(before.modified().unwrap(), after.modified().unwrap());
    assert!(store.try_all().unwrap().is_empty());
}

#[test]
fn oversized_encoded_output_cannot_replace_a_saved_preset_or_publish_a_new_one() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let source = root.path().join("source.csv");
    let evidence = b"value\n00123\n";
    fs::write(&source, evidence).unwrap();
    let project = create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    let previous = root.path().join("saved.json");
    let saved = b"{\"version\":2,\"name\":\"Saved investigation\"}";
    fs::write(&previous, saved).unwrap();
    let new = root.path().join("new.json");
    for path in [&previous, &new] {
        let error = save_preset(
            &store,
            SavePresetPayload {
                project_id: project.meta.id,
                // JSON escaping doubles the encoded bytes; budget is not
                // calculated from the unescaped Rust string length.
                name: Some("\n".repeat(MAX_BYTES / 2)),
                condition: None,
                filters: None,
                hidden_columns: Vec::new(),
                sort_key: None,
                sort_direction: None,
                column_order: None,
                column_widths: None,
                destination: path.to_string_lossy().into(),
                allow_overwrite: true,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("32 MiB"), "{error}");
    }
    assert_eq!(fs::read(&previous).unwrap(), saved);
    assert_eq!(load(&store, &previous).unwrap(), "Saved investigation");
    assert!(!new.exists());
    assert_eq!(fs::read(&source).unwrap(), evidence);
    assert!(fs::read_dir(root.path()).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".trivium-export-")));
}

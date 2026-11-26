use super::*;
use serde_json::json;
use uuid::Uuid;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("trivium-format-catalog-{}", Uuid::new_v4())))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn custom(id: &str) -> FormatDefinition {
    serde_json::from_value(json!({
        "schemaVersion": 1, "id": id, "name": "Synthetic marker", "pattern": "MARKER-[0-9]+",
        "examples": [{"text": "x MARKER-42 y", "values": ["MARKER-42"]}]
    }))
    .unwrap()
}

#[test]
fn embedded_definitions_are_available_without_any_user_directory() {
    let directory = Directory::new();
    let entries = list_formats(&directory.0).unwrap();
    assert!(
        !directory.0.exists(),
        "a read must not create a settings directory"
    );
    assert!(entries
        .iter()
        .any(|entry| entry.definition.id == "ipv4" && entry.builtin));
    assert_eq!(entries.len(), BUILTIN_FORMAT_JSON.len());
    assert!(entries
        .iter()
        .any(|entry| entry.definition.id == "base64" && entry.builtin));
    for retired in [
        "hex",
        "bitlocker-recovery-key",
        "cve",
        "jwt",
        "pem",
        "ssh-public-key",
        "ipv4-cidr",
        "ipv6-cidr",
        "ip-port",
        "defanged-ipv4",
        "defanged-domain",
        "defanged-url",
    ] {
        assert!(
            !entries.iter().any(|entry| entry.definition.id == retired),
            "{retired}"
        );
    }
    assert!(!entries
        .iter()
        .any(|entry| entry.definition.id.starts_with("phone")));
    for entry in entries {
        CompiledFormat::new(&entry.definition).unwrap();
        let embedded = BUILTIN_FORMAT_JSON
            .iter()
            .find(|(id, _)| *id == entry.definition.id)
            .unwrap();
        let original: FormatDefinition = serde_json::from_str(embedded.1).unwrap();
        assert_eq!(
            entry.definition, original,
            "catalog snapshots retain the entire embedded definition"
        );
    }
}

#[test]
fn user_definitions_round_trip_update_and_delete_without_touching_builtins() {
    let directory = Directory::new();
    let before = list_formats(&directory.0).unwrap();
    let definition = custom("review-marker");
    let saved = save_format(&directory.0, &definition).unwrap();
    assert!(!saved.builtin);
    let bytes = fs::read(directory.0.join("review-marker.json")).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["id"],
        "review-marker"
    );
    // A fresh catalog read is also the application-restart path.
    let reopened = list_formats(&directory.0).unwrap();
    assert_eq!(reopened.len(), before.len() + 1);
    let saved = reopened
        .iter()
        .find(|entry| entry.definition.id == "review-marker")
        .unwrap();
    assert!(!saved.builtin);
    assert_eq!(saved.definition.pattern, "MARKER-[0-9]+");
    let mut updated = definition.clone();
    updated.name = "Revised marker".into();
    save_format(&directory.0, &updated).unwrap();
    assert_eq!(
        list_formats(&directory.0)
            .unwrap()
            .last()
            .unwrap()
            .definition
            .name,
        "Revised marker"
    );
    delete_format(&directory.0, "review-marker").unwrap();
    assert_eq!(list_formats(&directory.0).unwrap().len(), before.len());
    assert!(!directory.0.join("review-marker.json").exists());
    delete_format(&directory.0, "review-marker").unwrap();
}

#[test]
fn invalid_replacement_preserves_a_saved_user_definition() {
    let directory = Directory::new();
    let definition = custom("preserved");
    save_format(&directory.0, &definition).unwrap();
    let path = directory.0.join("preserved.json");
    let original = fs::read(&path).unwrap();
    let mut invalid = definition.clone();
    invalid.pattern = "(?=unsupported-lookahead)".into();
    assert!(save_format(&directory.0, &invalid).is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    invalid = definition;
    invalid.schema_version = 2;
    assert!(save_format(&directory.0, &invalid).is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
}

#[test]
fn builtins_cannot_be_overridden_or_deleted_and_file_identity_is_checked() {
    let directory = Directory::new();
    let entries = list_formats(&directory.0).unwrap();
    let builtin = entries
        .iter()
        .find(|entry| entry.definition.id == "ipv4")
        .unwrap();
    assert!(save_format(&directory.0, &builtin.definition).is_err());
    assert!(delete_format(&directory.0, "ipv4").is_err());
    assert!(!directory.0.exists());
    fs::create_dir_all(&directory.0).unwrap();
    fs::write(
        directory.0.join("different.json"),
        serde_json::to_vec(&custom("actual")).unwrap(),
    )
    .unwrap();
    assert!(list_formats(&directory.0)
        .unwrap_err()
        .to_string()
        .contains("file name"));
    fs::remove_file(directory.0.join("different.json")).unwrap();
    fs::write(
        directory.0.join("ipv4.json"),
        serde_json::to_vec(&builtin.definition).unwrap(),
    )
    .unwrap();
    assert!(list_formats(&directory.0)
        .unwrap_err()
        .to_string()
        .contains("built-in"));
}

#[test]
fn invalid_and_nonportable_ids_cannot_escape_the_managed_directory() {
    let directory = Directory::new();
    for id in [
        "",
        "../escape",
        "a/b",
        "a\\b",
        ".hidden",
        "Upper",
        "con",
        "com1",
        "lpt9",
        "trailing.",
    ] {
        assert!(validate_format_id(id).is_err(), "{id:?}");
        assert!(save_format(&directory.0, &custom(id)).is_err(), "{id:?}");
        assert!(delete_format(&directory.0, id).is_err(), "{id:?}");
    }
    assert!(!directory.0.exists());
    assert!(validate_format_id("user_ipv4-2").is_ok());
}

#[test]
fn malformed_and_oversized_custom_files_are_reported_without_modifying_them() {
    let directory = Directory::new();
    fs::create_dir_all(&directory.0).unwrap();
    let path = directory.0.join("broken.json");
    fs::write(&path, b"{").unwrap();
    assert!(list_formats(&directory.0)
        .unwrap_err()
        .to_string()
        .contains("Invalid custom format JSON"));
    assert_eq!(fs::read(&path).unwrap(), b"{");
    fs::write(&path, vec![b' '; MAX_FORMAT_BYTES as usize + 1]).unwrap();
    assert!(list_formats(&directory.0)
        .unwrap_err()
        .to_string()
        .contains("256 KiB"));
    assert_eq!(fs::metadata(&path).unwrap().len(), MAX_FORMAT_BYTES + 1);
}

#[cfg(unix)]
#[test]
fn managed_directory_and_definition_symlinks_are_rejected() {
    use std::os::unix::fs::symlink;
    let directory = Directory::new();
    fs::create_dir_all(&directory.0).unwrap();
    let outside = directory.0.join("outside");
    fs::create_dir(&outside).unwrap();
    let root_link = directory.0.join("linked");
    symlink(&outside, &root_link).unwrap();
    assert!(save_format(&root_link, &custom("marker")).is_err());
    assert!(list_formats(&root_link).is_err());
    let root = directory.0.join("formats");
    fs::create_dir(&root).unwrap();
    let original = outside.join("source.json");
    let bytes = serde_json::to_vec(&custom("marker")).unwrap();
    fs::write(&original, &bytes).unwrap();
    symlink(&original, root.join("marker.json")).unwrap();
    assert!(list_formats(&root).is_err());
    assert!(save_format(&root, &custom("marker")).is_err());
    assert!(delete_format(&root, "marker").is_err());
    assert_eq!(fs::read(&original).unwrap(), bytes);
}

#[test]
fn validation_capture_and_examples_round_trip_as_a_complete_definition_snapshot() {
    let directory = Directory::new();
    let definition: FormatDefinition = serde_json::from_value(json!({
        "schemaVersion": 1, "id": "exact-ip", "name": "確認用IP",
        "pattern": "(?P<value>2001:db8::1)", "valueGroup": "value", "validation": "ipv6",
        "examples": [{"text": "日本語 2001:db8::1", "values": ["2001:db8::1"]}]
    }))
    .unwrap();
    let saved = save_format(&directory.0, &definition).unwrap();
    assert_eq!(saved.definition, definition);
    let reopened = list_formats(&directory.0).unwrap();
    let stored = reopened
        .iter()
        .find(|entry| entry.definition.id == "exact-ip")
        .unwrap();
    assert_eq!(stored.definition, definition);
    assert!(CompiledFormat::new(&stored.definition)
        .unwrap()
        .has_match("日本語 2001:db8::1", Some("2001:db8::1")));
}

#[test]
fn unsupported_definition_metadata_is_rejected_without_rewriting_the_file() {
    let directory = Directory::new();
    fs::create_dir_all(&directory.0).unwrap();
    let path = directory.0.join("marker.json");
    for property in ["classifier", "classification"] {
        let mut unsupported = serde_json::to_value(custom("marker")).unwrap();
        unsupported[property] = json!({
            "kind": "ipv4-cidr", "default": "Unclassified",
            "rules": [{"cidr": "10.0.0.0/8", "label": "Private"}]
        });
        let original = serde_json::to_vec(&unsupported).unwrap();
        fs::write(&path, &original).unwrap();
        let error = list_formats(&directory.0).unwrap_err().to_string();
        assert!(error.contains("unknown field"), "{error}");
        assert!(error.contains(property), "{error}");
        assert_eq!(fs::read(&path).unwrap(), original);
    }
}

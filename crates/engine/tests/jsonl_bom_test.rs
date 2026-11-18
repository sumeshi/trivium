use std::{fs, path::PathBuf};

use serde_json::json;
use sha2::{Digest, Sha256};
use trivium_engine::{
    ops::projects::{create_project, CreateProjectPayload},
    project_io::read_original_records,
    store::ProjectsStore,
};
use uuid::Uuid;

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn jsonl_ndjson_strip_only_file_prefix_bom_and_keep_lines_source_hash_and_values() {
    for extension in ["jsonl", "ndjson"] {
        let fixture =
            Fixture(std::env::temp_dir().join(format!("trivium-jsonl-bom-{}", Uuid::new_v4())));
        fs::create_dir_all(&fixture.0).unwrap();
        let store = ProjectsStore::new(fixture.0.join("workspace")).unwrap();
        let records = [
            json!({"message":"雪\u{feff}空", "nested":{"value":"⭐"}}),
            json!({"message":""}),
        ];
        let source = fixture.0.join(format!("source.{extension}"));
        let body = format!("\u{feff}\n\n{}\n{}\n", records[0], records[1]);
        fs::write(&source, &body).unwrap();
        let meta = create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta;
        assert_eq!(meta.total_records, 2);
        assert_eq!(
            meta.source_sha256,
            Some(format!("{:x}", Sha256::digest(body.as_bytes())))
        );
        assert_eq!(
            read_original_records(&store.project_dir(&meta.id)).unwrap(),
            records
        );
        assert_eq!(fs::read(&source).unwrap(), body.as_bytes());

        let invalid = fixture.0.join(format!("duplicate.{extension}"));
        let bad = format!(
            "\u{feff}\n\n{}\n\n{{\"利用者\":\"alice\",\"\\u5229\\u7528\\u8005\":\"bob\"}}\n",
            records[0]
        );
        fs::write(&invalid, &bad).unwrap();
        let error = create_project(
            &store,
            CreateProjectPayload {
                path: invalid.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("JSONL line 5")
                && error.contains("Duplicate JSON key")
                && error.contains("利用者"),
            "{error}"
        );
        assert_eq!(store.try_all().unwrap().len(), 1);
        assert_eq!(fs::read(&invalid).unwrap(), bad.as_bytes());

        let later = fixture.0.join(format!("later-bom.{extension}"));
        fs::write(&later, "{\"message\":1}\n\u{feff}{\"message\":2}\n").unwrap();
        let error = create_project(
            &store,
            CreateProjectPayload {
                path: later.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("JSONL line 2"), "{error}");
        assert_eq!(store.try_all().unwrap().len(), 1);
        assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
    }
}

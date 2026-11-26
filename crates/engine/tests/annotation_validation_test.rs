//! Invalid requested flags must not destroy annotations or the last bulk undo.

use std::{fs, path::PathBuf};

use trivium_engine::{
    annotation::AnnotationPatch,
    models::FlagEntry,
    ops::{
        bulk::{bulk_preview, bulk_undo, bulk_update, BulkAnnotatePayload, BulkUndoPayload},
        flags::{update_flag, UpdateFlagPayload},
        projects::{create_project, CreateProjectPayload},
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    storage::{load_flag_snapshot, load_flags, upsert_flag},
    store::ProjectsStore,
};
use uuid::Uuid;

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    project: Uuid,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-flag-validation-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source.csv");
        fs::write(&source, "name\nalpha\nbeta\ngamma\n").unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let project = create_project(
            &store,
            CreateProjectPayload {
                import_options: None,
                path: source.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        Self {
            root,
            store,
            project,
        }
    }

    fn update(&self, flag: Option<&str>) -> UpdateFlagPayload {
        UpdateFlagPayload {
            project_id: self.project,
            row_index: 0,
            flag: flag.map(str::to_owned),
            memo: Some(Some("saved note".into())),
            tags: Some(vec!["review".into()]),
            bookmarked: Some(true),
        }
    }

    fn bulk(&self, flag: &str) -> BulkAnnotatePayload {
        BulkAnnotatePayload {
            project_id: self.project,
            target: "selection".into(),
            row_indices: Some(vec![1]),
            spec: QuerySpec::default(),
            patch: AnnotationPatch {
                flag: Some(flag.into()),
                ..Default::default()
            },
            preview_token: None,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn invalid_single_and_bulk_flags_leave_entries_revision_and_previous_undo_intact() {
    let fixture = Fixture::new();
    update_flag(&fixture.store, fixture.update(Some("critical"))).unwrap();
    let mut prior = fixture.bulk("suspicious");
    prior.preview_token = Some(
        bulk_preview(&fixture.store, prior.clone())
            .unwrap()
            .preview_token,
    );
    assert_eq!(bulk_update(&fixture.store, prior).unwrap().updated, 1);

    let snapshot = load_flag_snapshot(&fixture.store.database_path(&fixture.project)).unwrap();
    let mut valid = fixture.bulk("safe");
    valid.preview_token = Some(
        bulk_preview(&fixture.store, valid.clone())
            .unwrap()
            .preview_token,
    );
    for invalid in [
        "critcal", "none", "safe|critical", "future", "SAFE", " critical ", "◯", "?", "✗",
    ] {
        let mut single = fixture.update(Some(invalid));
        single.memo = Some(Some("must not be saved".into()));
        let error = update_flag(&fixture.store, single).unwrap_err().to_string();
        assert!(error.contains("Invalid flag value"), "{error}");
        assert!(error.contains(invalid), "{error}");

        let mut bulk = valid.clone();
        bulk.patch.flag = Some(invalid.into());
        assert!(bulk_preview(&fixture.store, bulk.clone())
            .unwrap_err()
            .to_string()
            .contains("Invalid flag value"));
        assert!(bulk_update(&fixture.store, bulk)
            .unwrap_err()
            .to_string()
            .contains("Invalid flag value"));
        assert_eq!(
            load_flag_snapshot(&fixture.store.database_path(&fixture.project)).unwrap(),
            snapshot
        );
    }
    assert_eq!(
        bulk_undo(
            &fixture.store,
            BulkUndoPayload {
                project_id: fixture.project
            }
        )
        .unwrap()
        .updated,
        1
    );
    let entries = load_flags(&fixture.store.database_path(&fixture.project)).unwrap();
    assert_eq!(entries[&0].flag, "critical");
    assert_eq!(entries[&0].memo.as_deref(), Some("saved note"));
    assert!(!entries.contains_key(&1));
}

#[test]
fn canonical_flags_and_explicit_clear_remain_writable() {
    let fixture = Fixture::new();
    for flag in ["safe", "suspicious", "critical", ""] {
        update_flag(&fixture.store, fixture.update(Some(flag))).unwrap();
        let mut bulk = fixture.bulk(flag);
        bulk.preview_token = Some(
            bulk_preview(&fixture.store, bulk.clone())
                .unwrap()
                .preview_token,
        );
        bulk_update(&fixture.store, bulk).unwrap();
    }
    let entries = load_flags(&fixture.store.database_path(&fixture.project)).unwrap();
    assert_eq!(entries[&0].flag, "");
    assert_eq!(entries[&0].memo.as_deref(), Some("saved note"));
    assert_eq!(entries[&0].tags, ["review"]);
    assert!(entries[&0].bookmarked);
    assert!(!entries.contains_key(&1));
}

#[test]
fn updating_a_note_preserves_an_unknown_stored_flag_without_accepting_new_unknown_flags() {
    let fixture = Fixture::new();
    upsert_flag(
        &fixture.store.database_path(&fixture.project),
        0,
        &FlagEntry {
            flag: "unknown-custom".into(),
            memo: None,
            tags: vec![],
            bookmarked: false,
        },
    )
    .unwrap();
    update_flag(&fixture.store, fixture.update(None)).unwrap();
    let entries = load_flags(&fixture.store.database_path(&fixture.project)).unwrap();
    assert_eq!(entries[&0].flag, "unknown-custom");
    assert_eq!(entries[&0].memo.as_deref(), Some("saved note"));
    let page = query_project_rows(
        &fixture.store,
        QueryRowsPayload {
            project_id: fixture.project,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .unwrap();
    assert_eq!(page.rows[0].flag, "");
    assert_eq!(page.annotated_total, 0);
}

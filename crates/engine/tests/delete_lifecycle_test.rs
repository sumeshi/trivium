//! Deletion publishes a directory move before touching case contents.
use std::{fs, path::PathBuf};
use trivium_engine::{
    ops::{
        flags::{update_flag, UpdateFlagPayload},
        projects::{create_project, delete_project, CreateProjectPayload, ProjectRequest},
    },
    storage::project_database_path,
    store::ProjectsStore,
};
use uuid::Uuid;

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    source: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("trivium-delete-lifecycle-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let source = root.join("evidence.csv");
        fs::write(&source, b"id,value\n0,evidence\n").unwrap();
        let id = create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index: 0,
                flag: Some("critical".into()),
                memo: Some(Some("preserve".into())),
                tags: None,
                bookmarked: Some(true),
            },
        )
        .unwrap();
        Self {
            root,
            store,
            id,
            source,
        }
    }
    fn delete(&self) -> Result<(), trivium_engine::error::AppError> {
        delete_project(
            &self.store,
            ProjectRequest {
                project_id: self.id,
            },
        )
    }
    fn evidence_unchanged(&self) {
        assert_eq!(fs::read(&self.source).unwrap(), b"id,value\n0,evidence\n");
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn successful_deletion_removes_only_the_saved_case_and_preserves_source() {
    let fixture = Fixture::new();
    fixture.delete().unwrap();
    assert!(fixture.store.try_all().unwrap().is_empty());
    assert!(!fixture.store.project_dir(&fixture.id).exists());
    assert_eq!(fs::read_dir(fixture.store.trash_root()).unwrap().count(), 0);
    let reopened = ProjectsStore::new(fixture.root.clone()).unwrap();
    assert!(reopened.try_find(&fixture.id).unwrap().is_none());
    fixture.evidence_unchanged();
}

#[test]
fn unavailable_deletion_workspace_preserves_case_database_annotations_and_data() {
    let fixture = Fixture::new();
    fs::write(fixture.store.trash_root(), b"blocked workspace fixture").unwrap();
    let case = fixture.store.project_dir(&fixture.id);
    let parquet = fs::read(case.join("data.parquet")).unwrap();
    fixture
        .delete()
        .expect_err("no deletion may happen without its atomic workspace move");
    assert_eq!(fs::read(case.join("data.parquet")).unwrap(), parquet);
    assert!(project_database_path(&case).is_file());
    let entry = trivium_engine::storage::load_flag(&fixture.store.flags_path(&fixture.id), 0)
        .unwrap()
        .unwrap();
    assert_eq!(entry.flag, "critical");
    assert_eq!(entry.memo.as_deref(), Some("preserve"));
    assert!(entry.bookmarked);
    assert!(fixture.store.try_find(&fixture.id).unwrap().is_some());
    fixture.evidence_unchanged();
}

#[cfg(unix)]
#[test]
fn cleanup_failure_keeps_owned_trash_and_logically_deletes_the_case() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    let case = fixture.store.project_dir(&fixture.id);
    let protected = case.join("protected-fixture");
    fs::create_dir(&protected).unwrap();
    fs::write(protected.join("locked"), b"retained cleanup fixture").unwrap();
    let original = fs::metadata(&protected).unwrap().permissions();
    fs::set_permissions(&protected, fs::Permissions::from_mode(0o555)).unwrap();
    if fs::write(protected.join("probe"), b"permission probe").is_ok() {
        fs::set_permissions(&protected, original).unwrap();
        return;
    }
    fixture
        .delete()
        .expect("the atomic move committed logical deletion before cleanup");
    assert!(fixture.store.try_find(&fixture.id).unwrap().is_none());
    assert!(!case.exists());
    let leftovers = fs::read_dir(fixture.store.trash_root())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(leftovers.len(), 1);
    assert!(leftovers[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with(&fixture.id.to_string()));
    assert_eq!(
        fs::read(leftovers[0].join("protected-fixture/locked")).unwrap(),
        b"retained cleanup fixture"
    );
    fs::set_permissions(leftovers[0].join("protected-fixture"), original).unwrap();
    let reopened = ProjectsStore::new(fixture.root.clone()).unwrap();
    assert!(reopened.try_find(&fixture.id).unwrap().is_none());
    fixture.evidence_unchanged();
}

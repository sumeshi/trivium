//! The final directory rename publishes a complete SQLite/Parquet case.
use std::{
    fs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use trivium_engine::{
    jobs::{self, JobContext, JobKind, PROGRESS_INTERVAL},
    ops::projects::{create_project_with_job, CreateProjectPayload},
    storage,
    store::ProjectsStore,
};
use uuid::Uuid;

fn inspect_publication(cancel: bool) {
    let root = std::env::temp_dir().join(format!("trivium-import-publish-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let store = ProjectsStore::new(root.clone()).unwrap();
    let source = root.join("source.csv");
    let evidence = b"timestamp,value\n2026-01-01T00:00:00.123456789,exact\n";
    fs::write(&source, evidence).unwrap();
    let observed = Arc::new(AtomicBool::new(false));
    let seen = observed.clone();
    let inspect = store.clone();
    let guard = jobs::try_begin_job(JobKind::Import, None).unwrap();
    let context = JobContext::with_job(
        guard,
        Some(Arc::new(move |progress| {
            if progress.message.as_deref() == Some("Creating project") {
                assert!(
                    inspect.try_all().unwrap().is_empty(),
                    "staging must not appear in the workspace index"
                );
                let stages = fs::read_dir(inspect.staging_root())
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>();
                assert_eq!(stages.len(), 1);
                let metadata = storage::read_project_meta(&stages[0])
                    .unwrap()
                    .expect("metadata must commit before publication");
                assert_eq!(metadata.total_records, 1);
                assert!(stages[0].join("data.parquet").is_file());
                assert!(storage::load_setting(&stages[0], "column_profile")
                    .unwrap()
                    .is_some());
                assert!(!inspect.project_dir(&metadata.id).exists());
                seen.store(true, Ordering::SeqCst);
                if cancel {
                    jobs::request_cancel(progress.id);
                }
            } else {
                // Keep the real progress callback observable for this small fixture;
                // production progress deliberately suppresses events within 200 ms.
                std::thread::sleep(PROGRESS_INTERVAL + Duration::from_millis(1));
            }
        })),
    );
    let result = create_project_with_job(
        &store,
        CreateProjectPayload {
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
        &context,
    );
    assert!(observed.load(Ordering::SeqCst));
    if cancel {
        assert_eq!(result.unwrap_err().to_string(), "Cancelled.");
        assert!(store.try_all().unwrap().is_empty());
        assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
        assert_eq!(fs::read_dir(root.join("projects")).unwrap().count(), 0);
    } else {
        let meta = result.unwrap().meta;
        assert_eq!(store.try_find(&meta.id).unwrap().unwrap().total_records, 1);
        assert_eq!(
            storage::read_project_meta(&store.project_dir(&meta.id))
                .unwrap()
                .unwrap()
                .id,
            meta.id
        );
        assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
    }
    assert_eq!(fs::read(source).unwrap(), evidence);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn metadata_and_profile_are_complete_before_case_publication() {
    inspect_publication(false);
}

#[test]
fn cancellation_before_publication_removes_staged_database_and_keeps_evidence() {
    inspect_publication(true);
}

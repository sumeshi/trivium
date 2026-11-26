#![cfg(unix)]

use std::{
    fs::{self, OpenOptions},
    os::unix::{fs::symlink, fs::OpenOptionsExt},
    path::Path,
    process::Command,
    sync::mpsc,
    thread,
    time::Duration,
};

use trivium_engine::{
    ops::projects::{create_project, is_temporary_project, list_projects, CreateProjectPayload},
    store::ProjectsStore,
};

const MARKER: &str = ".trivium-extract-result";

/// A regression must fail rather than leave an indefinitely blocked test
/// thread. Only after the guard expires do we provide both ends of the FIFO,
/// allowing the old blocking implementation to finish and be joined safely.
fn inspect_without_blocking(store: &ProjectsStore, id: uuid::Uuid, fifo: &Path) {
    let worker_store = store.clone();
    let (send, receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        let temporary = is_temporary_project(&worker_store, &id);
        let listed = list_projects(&worker_store).map(|projects| {
            projects
                .into_iter()
                .map(|project| project.meta.id)
                .collect::<Vec<_>>()
        });
        send.send((temporary, listed)).unwrap();
    });
    let timely = receive.recv_timeout(Duration::from_secs(10));
    let rescue = timely.as_ref().err().map(|_| {
        OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(fifo)
            .expect("unblock the regressed reader before reporting failure")
    });
    let result = match timely {
        Ok(result) => result,
        Err(error) => receive
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| panic!("marker inspection did not unblock: {error}")),
    };
    worker.join().unwrap();
    let blocked = rescue.is_some();
    drop(rescue);
    assert!(
        !blocked,
        "a FIFO marker must not block saved-case discovery"
    );
    assert!(!result.0, "a FIFO is never a valid temporary-case marker");
    assert_eq!(
        result.1.unwrap(),
        vec![id],
        "the saved case must remain visible"
    );
}

#[test]
fn fifo_markers_and_symlinks_to_them_do_not_block_or_hide_saved_cases() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("ordinary.csv");
    fs::write(&source, b"message\nexact source\n").unwrap();
    let store = ProjectsStore::new(root.path().join("store")).unwrap();
    let project = create_project(
        &store,
        CreateProjectPayload {
            path: source.to_string_lossy().into_owned(),
            description: None,
            delimiter: None,
            restore_annotations: None,
            import_options: None,
        },
    )
    .unwrap();
    let id = project.meta.id;
    let marker = store.project_dir(&id).join(MARKER);
    let fifo = root.path().join("marker-pipe");
    assert!(Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());

    symlink(&fifo, &marker).unwrap();
    inspect_without_blocking(&store, id, &fifo);
    fs::remove_file(&marker).unwrap();
    fs::rename(&fifo, &marker).unwrap();
    inspect_without_blocking(&store, id, &marker);
    fs::remove_file(&marker).unwrap();

    // Ordinary symlinks retain the existing marker semantics.
    let regular = root.path().join("regular-marker");
    fs::write(&regular, b"trivium-extract-result-v1\n").unwrap();
    symlink(&regular, &marker).unwrap();
    assert!(is_temporary_project(&store, &id));
    assert!(list_projects(&store).unwrap().is_empty());
    fs::remove_file(&marker).unwrap();
    fs::create_dir(&marker).unwrap();
    assert!(!is_temporary_project(&store, &id));
    assert_eq!(list_projects(&store).unwrap().len(), 1);
    assert_eq!(fs::read(source).unwrap(), b"message\nexact source\n");
}

use super::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};
use trivium_engine::error::AppError;
use uuid::Uuid;

fn finishing(state: &CloseState) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !state.0.lock().finishing {
        assert!(Instant::now() < deadline, "close did not start");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn exit_waits_for_accepted_storage_and_happens_once() {
    let pool = Arc::new(StorageWorker::new(1, 8).unwrap());
    let state = Arc::new(CloseState::default());
    let exits = Arc::new(AtomicUsize::new(0));
    let (release, gate) = mpsc::channel();
    let mut write = pool
        .submit(Uuid::new_v4(), move || {
            gate.recv().unwrap();
            Ok(())
        })
        .unwrap();
    let (closing, storage, exit_count) = (state.clone(), pool.clone(), exits.clone());
    let close = tauri::async_runtime::spawn(async move {
        finish_with(&closing, &storage, || {
            exit_count.fetch_add(1, Ordering::SeqCst);
        })
        .await
    });
    finishing(&state);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    assert!(pool.submit(Uuid::new_v4(), || Ok(())).is_err());
    release.send(()).unwrap();
    tauri::async_runtime::block_on(close).unwrap().unwrap();
    assert!(tauri::async_runtime::block_on(write.recv())
        .unwrap()
        .is_ok());
    assert!(state.approved());
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert!(state.cancel(&pool).is_err());
}

#[test]
fn failed_dropped_reply_blocks_exit_until_error_is_acknowledged() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let state = CloseState::default();
    let exits = AtomicUsize::new(0);
    drop(
        pool.submit::<(), _>(Uuid::new_v4(), || {
            Err(AppError::Message("failed write".into()))
        })
        .unwrap(),
    );
    let failed = tauri::async_runtime::block_on(finish_with(&state, &pool, || {
        exits.fetch_add(1, Ordering::SeqCst);
    }));
    assert!(failed.unwrap_err().contains("failed write"));
    assert!(!state.approved());
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    // Failure reopens admission for retry; the error itself stays unacknowledged.
    let mut write = pool.submit(Uuid::new_v4(), || Ok(())).unwrap();
    assert!(tauri::async_runtime::block_on(write.recv())
        .unwrap()
        .is_ok());
    assert!(
        tauri::async_runtime::block_on(finish_with(&state, &pool, || panic!("must not exit")))
            .is_err()
    );
    state.cancel(&pool).unwrap();
    tauri::async_runtime::block_on(finish_with(&state, &pool, || {
        exits.fetch_add(1, Ordering::SeqCst);
    }))
    .unwrap();
    assert!(state.approved());
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}

#[test]
fn cancelling_an_inflight_close_prevents_its_late_exit() {
    let pool = Arc::new(StorageWorker::new(1, 8).unwrap());
    let state = Arc::new(CloseState::default());
    let exits = Arc::new(AtomicUsize::new(0));
    let (release, gate) = mpsc::channel();
    let mut write = pool
        .submit(Uuid::new_v4(), move || {
            gate.recv().unwrap();
            Ok(())
        })
        .unwrap();
    let (closing, storage, exit_count) = (state.clone(), pool.clone(), exits.clone());
    let close = tauri::async_runtime::spawn(async move {
        finish_with(&closing, &storage, || {
            exit_count.fetch_add(1, Ordering::SeqCst);
        })
        .await
    });
    finishing(&state);
    state.cancel(&pool).unwrap();
    assert!(tauri::async_runtime::block_on(close)
        .unwrap()
        .unwrap_err()
        .contains("cancelled"));
    release.send(()).unwrap();
    assert!(tauri::async_runtime::block_on(write.recv())
        .unwrap()
        .is_ok());
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    assert!(!state.approved());
    tauri::async_runtime::block_on(finish_with(&state, &pool, || {
        exits.fetch_add(1, Ordering::SeqCst);
    }))
    .unwrap();
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}

#[test]
fn cancel_before_finish_does_not_acknowledge_unreported_failures() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let state = CloseState::default();
    drop(
        pool.submit::<(), _>(Uuid::new_v4(), || {
            Err(AppError::Message("unreported".into()))
        })
        .unwrap(),
    );
    // Cancelling the unsaved-note dialog has not shown a native save error.
    state.cancel(&pool).unwrap();
    let failed =
        tauri::async_runtime::block_on(finish_with(&state, &pool, || panic!("must not exit")));
    assert!(failed.unwrap_err().contains("unreported"));
    assert!(!state.approved());
}

#[test]
fn an_error_displayed_by_frontend_can_be_acknowledged_before_native_finish() {
    let pool = StorageWorker::new(1, 8).unwrap();
    let state = CloseState::default();
    let mut failed = pool
        .submit::<(), _>(Uuid::new_v4(), || {
            Err(AppError::Message("database is locked".into()))
        })
        .unwrap();
    assert!(tauri::async_runtime::block_on(failed.recv())
        .unwrap()
        .is_err());
    // The frontend reported this error before finish_app_close was invoked.
    assert!(state.0.lock().reported_failure.is_none());
    tauri::async_runtime::block_on(cancel_with(&state, &pool, true)).unwrap();
    let exits = AtomicUsize::new(0);
    tauri::async_runtime::block_on(finish_with(&state, &pool, || {
        exits.fetch_add(1, Ordering::SeqCst);
    }))
    .unwrap();
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}

#[test]
fn explicit_acknowledgement_waits_for_workers_and_preserves_later_errors() {
    let pool = Arc::new(StorageWorker::new(1, 8).unwrap());
    let state = Arc::new(CloseState::default());
    let (started, running) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let mut failed = pool
        .submit::<(), _>(Uuid::new_v4(), move || {
            started.send(()).unwrap();
            gate.recv().unwrap();
            Err(AppError::Message("first failure".into()))
        })
        .unwrap();
    running.recv_timeout(Duration::from_secs(2)).unwrap();
    let (ack_state, storage) = (state.clone(), pool.clone());
    let ack =
        tauri::async_runtime::spawn(async move { cancel_with(&ack_state, &storage, true).await });
    finishing(&state);
    assert!(
        pool.submit(Uuid::new_v4(), || Ok(())).is_err(),
        "admission stays sealed through acknowledgement"
    );
    release.send(()).unwrap();
    assert!(tauri::async_runtime::block_on(failed.recv())
        .unwrap()
        .is_err());
    tauri::async_runtime::block_on(ack).unwrap().unwrap();
    let mut later = pool
        .submit::<(), _>(Uuid::new_v4(), || {
            Err(AppError::Message("later failure".into()))
        })
        .unwrap();
    assert!(tauri::async_runtime::block_on(later.recv())
        .unwrap()
        .is_err());
    let failed_close =
        tauri::async_runtime::block_on(finish_with(&state, &pool, || panic!("must not exit")));
    assert!(failed_close.unwrap_err().contains("later failure"));
    assert!(!state.approved());
}

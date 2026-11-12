use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

use uuid::Uuid;

use super::*;
use crate::{
    jobs::{self, JobKind},
    ops::query::{result_cache::query_cache_key, QuerySpec},
};

fn key() -> QueryCacheKey {
    let path = std::env::temp_dir().join(format!("trivium-flight-{}", Uuid::new_v4()));
    std::fs::write(&path, b"revision").unwrap();
    let key = query_cache_key(Uuid::new_v4(), &QuerySpec::default(), &path, None).unwrap();
    std::fs::remove_file(path).unwrap();
    key
}

fn wait_for_waiter(key: &QueryCacheKey) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        // Map + owner + waiter. Inspect without cloning the Arc ourselves.
        if flights()
            .lock()
            .get(key)
            .is_some_and(|flight| Arc::strong_count(flight) >= 3)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "waiter did not join the cold query"
        );
        thread::yield_now();
    }
}

fn cancellable_job() -> JobContext {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(guard) = jobs::try_begin_job(JobKind::Query, None) {
            return JobContext::with_job(guard, None);
        }
        assert!(
            Instant::now() < deadline,
            "other test jobs did not release their slots"
        );
        thread::yield_now();
    }
}

#[test]
fn concurrent_cold_queries_share_work_and_cancelled_or_panicking_owners_do_not_poison_waiters() {
    for owner_behavior in ["success", "cancel", "panic"] {
        let key = key();
        let calls = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let owner_job = if owner_behavior == "cancel" {
            cancellable_job()
        } else {
            JobContext::none()
        };
        let owner_id = owner_job.id();
        let owner_key = key.clone();
        let owner_calls = Arc::clone(&calls);
        let owner = thread::spawn(move || {
            resolve_once(Some(owner_key), &owner_job, || {
                owner_calls.fetch_add(1, Ordering::SeqCst);
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
                if owner_behavior == "panic" {
                    panic!("interrupted evaluator");
                }
                Ok(CachedResult::new(vec![2, 5, 8], 0, 0, 0))
            })
        });
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let waiter_key = key.clone();
        let waiter_calls = Arc::clone(&calls);
        let waiter = thread::spawn(move || {
            resolve_once(Some(waiter_key), &JobContext::none(), || {
                waiter_calls.fetch_add(1, Ordering::SeqCst);
                Ok(CachedResult::new(vec![2, 5, 8], 0, 0, 0))
            })
        });
        wait_for_waiter(&key);
        if let Some(id) = owner_id {
            assert!(jobs::request_cancel(id));
        }
        release_tx.send(()).unwrap();
        let first = owner.join();
        let second = waiter.join().unwrap().unwrap();
        assert_eq!(&*second.ids, &[2, 5, 8]);
        match owner_behavior {
            "success" => {
                let first = first.unwrap().unwrap();
                assert!(Arc::ptr_eq(&first.ids, &second.ids));
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    1,
                    "prefetches resolved the same query twice"
                );
            }
            "cancel" => {
                assert_eq!(first.unwrap().unwrap_err().to_string(), "Cancelled.");
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            }
            _ => {
                assert!(first.is_err());
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            }
        }
        assert!(!flights().lock().contains_key(&key));
    }
}

#[test]
fn cancelling_a_waiter_leaves_the_owner_and_other_cache_users_alive() {
    let key = key();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let owner_key = key.clone();
    let owner = thread::spawn(move || {
        resolve_once(Some(owner_key), &JobContext::none(), || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            Ok(CachedResult::new(vec![4], 0, 0, 0))
        })
    });
    started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let job = cancellable_job();
    let id = job.id().unwrap();
    let waiter_key = key.clone();
    let waiter = thread::spawn(move || {
        resolve_once(Some(waiter_key), &job, || {
            panic!("cancelled waiter evaluated")
        })
    });
    wait_for_waiter(&key);
    assert!(jobs::request_cancel(id));
    assert_eq!(
        waiter.join().unwrap().unwrap_err().to_string(),
        "Cancelled."
    );
    release_tx.send(()).unwrap();
    let first = owner.join().unwrap().unwrap();
    let cached = resolve_once(Some(key), &JobContext::none(), || {
        panic!("successful owner was not cached")
    })
    .unwrap();
    assert!(Arc::ptr_eq(&first.ids, &cached.ids));
}

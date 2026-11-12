//! Coalesce identical cold resolutions without serializing different queries.
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::Duration,
};

use parking_lot::{Condvar, Mutex};

use super::result_cache::{cache_lookup, cache_peek, cache_store, CachedResult, QueryCacheKey};
use crate::{error::AppError, jobs::JobContext};

#[cfg(test)]
mod tests;

#[derive(Default)]
struct Flight {
    ready: Mutex<Option<Result<CachedResult, String>>>,
    changed: Condvar,
}

fn flights() -> &'static Mutex<HashMap<QueryCacheKey, Arc<Flight>>> {
    static FLIGHTS: OnceLock<Mutex<HashMap<QueryCacheKey, Arc<Flight>>>> = OnceLock::new();
    FLIGHTS.get_or_init(|| Mutex::new(HashMap::new()))
}

struct Owner {
    key: QueryCacheKey,
    flight: Arc<Flight>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        let mut ready = self.flight.ready.lock();
        if ready.is_none() {
            *ready = Some(Err("Query evaluation interrupted.".into()));
        }
        drop(ready);
        let mut active = flights().lock();
        if active
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            active.remove(&self.key);
        }
        drop(active);
        self.flight.changed.notify_all();
    }
}

pub(super) fn resolve_once(
    key: Option<QueryCacheKey>,
    job: &JobContext,
    resolve: impl FnOnce() -> Result<CachedResult, AppError>,
) -> Result<CachedResult, AppError> {
    let Some(key) = key else {
        return resolve();
    };
    loop {
        job.check_cancelled()?;
        if let Some(result) = cache_lookup(&key) {
            return Ok(result);
        }
        let (flight, owns) = {
            let mut active = flights().lock();
            match active.get(&key) {
                Some(flight) => (Arc::clone(flight), false),
                None => {
                    let flight = Arc::new(Flight::default());
                    active.insert(key.clone(), Arc::clone(&flight));
                    (flight, true)
                }
            }
        };
        if owns {
            let owner = Owner {
                key: key.clone(),
                flight,
            };
            // A previous owner may have published between the first lookup
            // and insertion into the flight map.
            let result = match cache_peek(&key) {
                Some(cached) => Ok(cached),
                None => resolve().and_then(|cached| {
                    job.check_cancelled()?;
                    cache_store(key.clone(), cached.clone());
                    Ok(cached)
                }),
            };
            *owner.flight.ready.lock() = Some(match &result {
                Ok(cached) => Ok(cached.clone()),
                Err(error) => Err(error.to_string()),
            });
            return result;
        }
        let mut ready = flight.ready.lock();
        while ready.is_none() {
            job.check_cancelled()?;
            flight
                .changed
                .wait_for(&mut ready, Duration::from_millis(20));
        }
        job.check_cancelled()?;
        if let Some(Ok(cached)) = ready.as_ref() {
            return Ok(cached.clone());
        }
        // A cancelled/failed owner does not cancel an independent waiter.
        // Retrying elects a fresh owner; failures are never cached.
    }
}

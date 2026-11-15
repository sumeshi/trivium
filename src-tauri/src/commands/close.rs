use parking_lot::Mutex;
use tauri::{AppHandle, Emitter, Manager, State};

use super::storage_worker::{storage_worker, DrainOutcome, StorageWorker};

pub(crate) const CLOSE_REQUESTED_EVENT: &str = "trivium:close-requested";

#[derive(Default)]
struct CloseAttempt {
    generation: u64,
    finishing: bool,
    approved: bool,
    requested: bool,
    reported_failure: Option<u64>,
}

#[derive(Default)]
pub(crate) struct CloseState(Mutex<CloseAttempt>);

impl CloseState {
    pub(crate) fn approved(&self) -> bool {
        self.0.lock().approved
    }

    fn cancel(&self, pool: &StorageWorker) -> Result<(), String> {
        let mut state = self.0.lock();
        if state.approved {
            return Err("The application is already exiting.".into());
        }
        state.generation += 1;
        state.finishing = false;
        state.requested = false;
        pool.resume(state.reported_failure.take());
        Ok(())
    }
}

/// The frontend drains its own queues before calling this final native barrier.
/// A late cancellation invalidates the attempt before it can approve exit.
async fn finish_with(
    state: &CloseState,
    pool: &StorageWorker,
    exit: impl FnOnce(),
) -> Result<(), String> {
    let (generation, mut reply) = {
        let mut state = state.0.lock();
        if state.finishing || state.approved {
            return Err("The application is already closing.".into());
        }
        let reply = pool.seal_and_drain()?;
        state.generation += 1;
        state.finishing = true;
        state.reported_failure = None;
        (state.generation, reply)
    };
    let saved = reply.recv().await.unwrap_or_else(|| DrainOutcome {
        result: Err("Storage drain did not complete.".into()),
        failure_sequence: 0,
    });
    {
        let mut state = state.0.lock();
        if state.generation != generation {
            return Err("Closing was cancelled.".into());
        }
        state.finishing = false;
        if let Err(error) = saved.result {
            // Preserve the failure until the user has seen it and cancels this
            // attempt. Admission resumes so a failed save can be retried.
            state.reported_failure = Some(saved.failure_sequence);
            pool.resume(None);
            return Err(error);
        }
        state.approved = true;
    }
    exit();
    Ok(())
}

async fn cancel_with(
    state: &CloseState,
    pool: &StorageWorker,
    acknowledge_failures: bool,
) -> Result<(), String> {
    if !acknowledge_failures {
        return state.cancel(pool);
    }
    // The UI has displayed a strict-drain error and stays frozen until this
    // acknowledgement returns. Wait for native completion too: a save's reply
    // can arrive just before its worker records the failure and releases it.
    let (generation, mut reply) = {
        let mut state = state.0.lock();
        if state.approved {
            return Err("The application is already exiting.".into());
        }
        state.generation += 1;
        state.finishing = false;
        state.requested = false;
        state.reported_failure = None;
        pool.resume(None);
        let reply = pool.seal_and_drain()?;
        state.finishing = true;
        (state.generation, reply)
    };
    let receipt = reply.recv().await;
    let mut state = state.0.lock();
    if state.generation != generation {
        return Err("Closing was cancelled.".into());
    }
    state.finishing = false;
    let Some(receipt) = receipt else {
        pool.resume(None);
        return Err("Storage acknowledgement did not complete.".into());
    };
    // Admission remained sealed, and the receipt names the exact observed
    // failure generation. Errors after editing resumes stay unacknowledged.
    pool.resume(Some(receipt.failure_sequence));
    Ok(())
}

pub(crate) fn request_app_close(app: &AppHandle) {
    {
        let state = app.state::<CloseState>();
        let mut state = state.0.lock();
        if state.approved {
            return;
        }
        state.requested = true;
    }
    // The native gate remains closed even before the listener is ready.
    // Repeated window-close requests can notify a subsequently ready UI.
    let _ = app.emit(CLOSE_REQUESTED_EVENT, ());
}

#[tauri::command]
pub fn app_close_requested(state: State<CloseState>) -> bool {
    state.0.lock().requested
}

#[tauri::command]
pub async fn finish_app_close(app: AppHandle, state: State<'_, CloseState>) -> Result<(), String> {
    finish_with(&state, storage_worker()?, || app.exit(0)).await
}

#[tauri::command]
pub async fn cancel_app_close(
    state: State<'_, CloseState>,
    acknowledge_failures: Option<bool>,
) -> Result<(), String> {
    cancel_with(
        &state,
        storage_worker()?,
        acknowledge_failures.unwrap_or(false),
    )
    .await
}

#[cfg(test)]
mod tests;

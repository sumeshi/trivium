//! Extract a bounded exact value ranking into an ordinary temporary CSV case.
mod aggregate;
mod model;
mod output;
mod scan;

#[cfg(test)]
mod tests;

pub use model::{ExtractPayload, ExtractScope};

use crate::{error::AppError, jobs::JobContext, models::ProjectSummary, store::ProjectsStore};

pub fn extract_values(
    store: &ProjectsStore,
    payload: ExtractPayload,
    job: &JobContext,
) -> Result<ProjectSummary, AppError> {
    let table = scan::build(store, payload, aggregate::Limits::default(), job)?;
    output::open_csv(store, table, job)
}

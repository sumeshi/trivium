//! Transient, exact extraction over projected source batches.
mod aggregate;
mod cache;
mod model;
mod output;
mod scan;
mod sources;

#[cfg(test)]
mod cache_tests;
#[cfg(test)]
mod output_tests;
#[cfg(test)]
mod tests;

pub use cache::release_extract_result;
pub use model::*;
pub use output::{
    export_extract, export_prepared_extract, prepare_extract_export, PreparedExtractExport,
};
pub use sources::extract_value_sources;

use crate::{error::AppError, jobs::JobContext, store::ProjectsStore};

pub fn extract_values(
    store: &ProjectsStore,
    payload: ExtractPayload,
    job: &JobContext,
) -> Result<ExtractResult, AppError> {
    let snapshot = scan::build(store, payload, aggregate::Limits::default(), job)?;
    job.check_cancelled()?;
    cache::insert(snapshot)
}

pub fn extract_values_page(
    store: &ProjectsStore,
    payload: ExtractPagePayload,
) -> Result<ExtractPage, AppError> {
    let snapshot = cache::get(store, payload.result_id)?;
    let limit = page_limit(payload.limit)?;
    let rows = snapshot
        .values
        .iter()
        .skip(payload.offset)
        .take(limit)
        .cloned()
        .collect();
    Ok(ExtractPage {
        rows,
        total: snapshot.values.len(),
        offset: payload.offset,
    })
}

fn page_limit(limit: usize) -> Result<usize, AppError> {
    if limit == 0 {
        return Err(AppError::Message(
            "Choose an extraction page size between 1 and 100.".into(),
        ));
    }
    Ok(limit.min(100))
}

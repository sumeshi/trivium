use crate::{error::AppError, formats::CompiledFormat, jobs::JobContext, store::ProjectsStore};

use super::{
    cache,
    scan::{self, Visitor},
    ExtractSource, ExtractSources, ExtractSourcesPayload,
};

struct Sources<'a> {
    value: &'a str,
    skip: usize,
    limit: usize,
    rows: Vec<ExtractSource>,
    has_more: bool,
}
impl Visitor for Sources<'_> {
    fn available(&self) -> usize {
        8 * 1024 * 1024
    }
    fn occurrence(
        &mut self,
        row: usize,
        column: &str,
        value: &str,
        start: usize,
        end: usize,
        _transient: usize,
        _matcher: &CompiledFormat,
    ) -> Result<bool, AppError> {
        if value != self.value {
            return Ok(true);
        }
        if self.skip > 0 {
            self.skip -= 1;
            return Ok(true);
        }
        if self.rows.len() == self.limit {
            self.has_more = true;
            return Ok(false);
        }
        self.rows.push(ExtractSource {
            row_index: row,
            column: column.into(),
            start,
            end,
        });
        Ok(true)
    }
}

pub fn extract_value_sources(
    store: &ProjectsStore,
    payload: ExtractSourcesPayload,
    job: &JobContext,
) -> Result<ExtractSources, AppError> {
    job.check_cancelled()?;
    let snapshot = cache::get(store, payload.result_id)?;
    if !snapshot
        .values
        .iter()
        .any(|value| value.value == payload.value)
    {
        return Err(AppError::Message(
            "Value not found in this extraction result.".into(),
        ));
    }
    let mut visitor = Sources {
        value: &payload.value,
        skip: payload.offset,
        limit: super::page_limit(payload.limit)?,
        rows: Vec::new(),
        has_more: false,
    };
    scan::walk(
        &snapshot.path,
        &snapshot.summary.columns,
        snapshot.total_rows,
        snapshot.ids.as_deref().map(Vec::as_slice),
        &snapshot.matcher,
        &mut visitor,
        job,
    )?;
    snapshot.validate(store)?;
    job.check_cancelled()?;
    Ok(ExtractSources {
        rows: visitor.rows,
        offset: payload.offset,
        has_more: visitor.has_more,
    })
}

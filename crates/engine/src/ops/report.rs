use super::{
    analysis_data::AnalysisSource,
    query::{materialize_page_rows, QuerySpec},
};
use crate::{
    analysis::stats::StatsAccumulator,
    error::AppError,
    export::{
        atomic::{
            ensure_destination_is_not_managed, ensure_destination_is_not_source, write_atomically,
        },
        ranks::RowRanks,
        sorted_runs::ExternalSorter,
    },
    jobs::JobContext,
    project_io::{ProjectBatchReader, ProjectColumns, INTERNAL_ROW_ID},
    store::ProjectsStore,
};
use anyhow::Context;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Write, path::PathBuf};
use uuid::Uuid;
mod render;

#[derive(Debug, Deserialize)]
pub struct BuildReportPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    #[serde(default)]
    pub search: Option<String>,
    /// Display columns. For old callers this also remains the search scope
    /// when `searchColumns` is omitted.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "searchColumns")]
    pub search_columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default, rename = "bookmarkedOnly")]
    pub bookmarked_only: Option<bool>,
    #[serde(default, rename = "sortKey")]
    pub sort_key: Option<String>,
    #[serde(default, rename = "sortDirection")]
    pub sort_direction: Option<String>,
    #[serde(default)]
    pub condition: Option<crate::condition::Node>,
    pub destination: String,
}

#[derive(Debug, Serialize)]
pub struct ReportMeta {
    pub total_rows: usize,
    /// Same value as `query_project_rows(...).total_filtered_rows`.
    pub hit_rows: usize,
    pub flagged_in_matches: usize,
}

/// Write an atomic report without retaining the source frame or all HTML rows.
pub fn build_report(
    store: &ProjectsStore,
    payload: BuildReportPayload,
) -> Result<ReportMeta, AppError> {
    build_report_with_job(store, payload, &JobContext::none())
}

pub fn build_report_with_job(
    store: &ProjectsStore,
    payload: BuildReportPayload,
    job: &JobContext,
) -> Result<ReportMeta, AppError> {
    let spec = QuerySpec {
        search: payload.search.clone(),
        search_columns: payload
            .search_columns
            .clone()
            .or_else(|| payload.columns.clone()),
        condition: payload.condition.clone(),
        flag_filter: payload.flag_filter.clone(),
        bookmarked_only: payload.bookmarked_only,
        sort_key: payload.sort_key.clone(),
        sort_direction: payload.sort_direction.clone(),
    };
    let source = AnalysisSource::open(store, payload.project_id, &spec, job)?;
    let display = match &payload.columns {
        Some(columns) => source.columns(columns, "display columns")?,
        None => source.target.columns.clone(),
    };
    let mut projection = if display.is_empty() {
        vec![source
            .target
            .columns
            .first()
            .cloned()
            .unwrap_or_else(|| INTERNAL_ROW_ID.into())]
    } else {
        display.clone()
    };
    let mut unique = std::collections::HashSet::new();
    projection.retain(|column| unique.insert(column.clone()));
    let mut reader = ProjectBatchReader::open(&source.path, &projection).map_err(AppError::from)?;
    let empty = reader.empty_frame().map_err(AppError::from)?;
    let mut stats = display
        .iter()
        .map(|column| {
            StatsAccumulator::new(
                column,
                empty
                    .column(column)
                    .expect("projected report column")
                    .dtype(),
            )
        })
        .collect::<Vec<_>>();
    let annotated = source
        .target
        .ids
        .iter()
        .copied()
        .filter(|id| {
            source
                .target
                .flags
                .get(id)
                .is_some_and(|entry| entry.is_annotated())
        })
        .collect::<Vec<_>>();
    let project_columns = ProjectColumns::from_meta_and_names(&source.meta, &source.target.columns);
    let display_meta = project_columns
        .columns()
        .iter()
        .filter(|column| display.contains(&column.id))
        .cloned()
        .collect::<Vec<_>>();
    let destination = PathBuf::from(&payload.destination);
    let validate = || {
        ensure_destination_is_not_source(&destination, &source.meta)?;
        ensure_destination_is_not_managed(&destination, store)
    };
    write_atomically(&destination, job, validate, |workspace, writer| {
        writer
            .write_all(
                render::prefix(
                    &source.meta,
                    payload.search.as_deref(),
                    payload.condition.as_ref(),
                    source.target.total_rows,
                    source.target.ids.len(),
                    source.target.annotated_filtered,
                )
                .as_bytes(),
            )
            .context("failed to write report header")?;
        let ordered = annotated.windows(2).all(|ids| ids[0] < ids[1]);
        let mut ranks = (!ordered)
            .then(|| {
                RowRanks::new(
                    source.target.total_rows,
                    &annotated,
                    &workspace.join("ranks"),
                    job,
                )
            })
            .transpose()?;
        let mut ordinal = 0;
        let mut sorter = (!ordered)
            .then(|| ExternalSorter::new(&workspace.join("rows")))
            .transpose()?;
        if annotated.is_empty() {
            writer
                .write_all(b"<p>No annotated rows in the current filter.</p>\n")
                .context("failed to write empty report")?;
        } else {
            writer
                .write_all(render::rows_header(&display).as_bytes())
                .context("failed to write report table")?;
        }
        let mut written = 0;
        while let Some(batch) = reader.next_with_job(job) {
            let batch = batch.map_err(AppError::from)?;
            for (column, acc) in display.iter().zip(&mut stats) {
                acc.observe(
                    batch.frame.column(column).expect("projected report column"),
                    0..batch.frame.height(),
                    job,
                )?;
            }
            let mut local = Vec::new();
            let mut ids = Vec::new();
            let mut positions = Vec::new();
            for row in 0..batch.frame.height() {
                let id = batch.offset + row;
                let rank = if let Some(ranks) = &mut ranks {
                    ranks.get(id, job)?
                } else if annotated.get(ordinal) == Some(&id) {
                    let rank = ordinal;
                    ordinal += 1;
                    Some(rank)
                } else {
                    None
                };
                if let Some(rank) = rank {
                    local.push(row as IdxSize);
                    ids.push(batch.offset + row);
                    positions.push(rank);
                }
            }
            if ids.is_empty() {
                continue;
            }
            let frame = batch
                .frame
                .take(&IdxCa::from_vec("", local))
                .context("failed to select report rows")?;
            let rows = materialize_page_rows(
                &frame,
                &ids,
                &display_meta,
                &source.target.flags,
                &HashMap::new(),
                false,
            )?;
            for (row, rank) in rows.iter().zip(positions) {
                let html = render::row(&display, row);
                if let Some(sorter) = &mut sorter {
                    sorter.push(rank as u64, html.as_bytes(), job)?;
                } else {
                    writer
                        .write_all(html.as_bytes())
                        .context("failed to write report row")?;
                }
                written += 1;
            }
        }
        if written != annotated.len() {
            return Err(AppError::Message("Missing annotated report rows.".into()));
        }
        if let Some(sorter) = sorter {
            let mut rows = sorter.finish(job)?;
            while rows.write_next(writer, false, job)?.is_some() {}
        }
        if !annotated.is_empty() {
            writer
                .write_all(b"</tbody></table>\n")
                .context("failed to finish report table")?;
        }
        let stats = stats
            .into_iter()
            .map(|acc| acc.finish(1, job))
            .collect::<Result<Vec<_>, _>>()?;
        writer
            .write_all(render::suffix(&stats).as_bytes())
            .context("failed to finish report")?;
        Ok(())
    })?;
    Ok(ReportMeta {
        total_rows: source.target.total_rows,
        hit_rows: source.target.ids.len(),
        flagged_in_matches: source.target.annotated_filtered,
    })
}

use super::{
    annotations,
    atomic::{
        ensure_destination_is_not_managed, ensure_destination_is_not_source, write_atomically,
    },
    ranks::RowRanks,
    record_sink::RecordSink,
    sorted_runs::ExternalSorter,
};
use crate::{
    error::AppError,
    jobs::JobContext,
    ops::{analysis_data::AnalysisSource, query::QuerySpec},
    project_io::{ProjectColumns, INTERNAL_ROW_ID},
    store::ProjectsStore,
};
use anyhow::Context;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, io::Write, path::PathBuf, sync::Arc};
use uuid::Uuid;

/// Export selection.
///
/// `scope = "filtered"` (default) exports the rows selected by the flattened
/// `QuerySpec`; `"all"` exports every row in the requested sort order;
/// `"selected"` intersects `rowIndices` with the `QuerySpec` result and keeps
/// the result order. Input/output column restriction is `columns`; search scope
/// is `QuerySpec.searchColumns`.
///
/// `includeAnnotations = true` (default) adds the independent `trivium-*`
/// columns. `false` writes only data columns.
#[derive(Debug, Deserialize)]
pub struct ExportProjectPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(flatten)]
    pub spec: crate::ops::query::QuerySpec,
    /// Output only these base columns (derived trivium columns are kept).
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Required when `scope = "selected"`. Duplicates are ignored and ids are
    /// emitted in the applied `QuerySpec` order.
    #[serde(default, rename = "rowIndices")]
    pub row_indices: Option<Vec<usize>>,
    /// Whether to add the derived annotation columns.
    #[serde(default, rename = "includeAnnotations")]
    pub include_annotations: Option<bool>,
}

/// Result of one export.
#[derive(Debug, Clone, Serialize)]
pub struct ExportResult {
    /// Number of data rows written (excluding the header).
    pub rows: usize,
    /// Header labels in output order.
    pub columns: Vec<String>,
    /// Non-fatal notes, such as an annotation column renamed to avoid a
    /// collision with a data column.
    pub warnings: Vec<String>,
}

/// Export projected source batches, preserving the shared query's display order.
pub fn export_project(
    store: &ProjectsStore,
    payload: ExportProjectPayload,
) -> Result<ExportResult, AppError> {
    export_project_with_job(store, payload, &JobContext::none())
}

pub fn export_project_with_job(
    store: &ProjectsStore,
    payload: ExportProjectPayload,
    job: &JobContext,
) -> Result<ExportResult, AppError> {
    let scope = payload.scope.as_deref().unwrap_or("filtered");
    let all_spec;
    let spec = if scope == "all" {
        all_spec = QuerySpec {
            sort_key: payload.spec.sort_key.clone(),
            sort_direction: payload.spec.sort_direction.clone(),
            ..Default::default()
        };
        &all_spec
    } else {
        &payload.spec
    };
    job.report_progress(0, Some(3), Some("Resolving export rows".into()));
    let mut source = AnalysisSource::open(store, payload.project_id, spec, job)?;
    source.target.ids = export_ids(
        scope,
        &payload,
        source.target.total_rows,
        &source.target.ids,
    )?;
    let destination = PathBuf::from(&payload.destination);
    let validate = || {
        ensure_destination_is_not_source(&destination, &source.meta)?;
        ensure_destination_is_not_managed(&destination, store)
    };
    let columns = match &payload.columns {
        Some(requested) => requested
            .iter()
            .filter(|name| source.target.columns.contains(name))
            .cloned()
            .collect::<Vec<_>>(),
        None => source.target.columns.clone(),
    };
    let include_annotations = payload.include_annotations.unwrap_or(true);
    if columns.is_empty() && !include_annotations {
        return Err(AppError::Message(
            "Select at least one output column.".into(),
        ));
    }
    let project_columns = ProjectColumns::from_meta_and_names(&source.meta, &source.target.columns);
    let data_headers: Vec<String> = columns
        .iter()
        .map(|name| project_columns.label(name))
        .collect();
    let mut warnings = Vec::new();
    let plan = if include_annotations {
        annotations::plan_annotation_columns(&data_headers, &columns, &mut warnings)
    } else {
        Vec::new()
    };
    let headers = plan
        .iter()
        .map(|column| column.header.clone())
        .chain(data_headers)
        .collect::<Vec<_>>();
    let projection = if columns.is_empty() {
        vec![source
            .target
            .columns
            .first()
            .cloned()
            .unwrap_or_else(|| INTERNAL_ROW_ID.to_string())]
    } else {
        columns.clone()
    };
    // Validate duplicate/internal column names before creating an output transaction.
    let reader = source.selected_reader(&projection)?;
    let source_order = source.target.ids.windows(2).all(|ids| ids[0] < ids[1]);
    let rows = write_atomically(&destination, job, validate, |workspace, writer| {
        write_headers(writer, &headers)?;
        job.report_progress(1, Some(3), Some("Writing CSV".into()));
        let mut reader = reader;
        let mut ordinal = 0usize;
        let mut ranks = (!source_order)
            .then(|| {
                RowRanks::new(
                    source.target.total_rows,
                    &source.target.ids,
                    &workspace.join("ranks"),
                    job,
                )
            })
            .transpose()?;
        let mut sorter = (!source_order)
            .then(|| ExternalSorter::new(&workspace.join("records")))
            .transpose()?;
        let mut written = 0usize;
        while let Some(batch) = reader.next_with_job(job) {
            let batch = batch.map_err(AppError::from)?;
            let mut local = Vec::new();
            let mut ids = Vec::new();
            let mut batch_ranks = Vec::new();
            for row in 0..batch.frame.height() {
                let id = batch.offset + row;
                let rank = if let Some(ranks) = &mut ranks {
                    ranks.get(id, job)?
                } else {
                    while ordinal < source.target.ids.len() && source.target.ids[ordinal] < id {
                        ordinal += 1;
                    }
                    if source.target.ids.get(ordinal) == Some(&id) {
                        let rank = ordinal;
                        ordinal += 1;
                        Some(rank)
                    } else {
                        None
                    }
                };
                if let Some(rank) = rank {
                    local.push(row as IdxSize);
                    ids.push(id);
                    batch_ranks.push(rank);
                }
            }
            if ids.is_empty() {
                continue;
            }
            let selected = batch
                .frame
                .take(&IdxCa::from_vec("", local))
                .context("failed to select export batch")?;
            let mut output = if include_annotations {
                annotations::annotation_series(
                    annotations::build_annotations(&ids, &source.target.flags),
                    &plan,
                )
            } else {
                Vec::new()
            };
            output.extend(columns.iter().map(|name| {
                selected
                    .column(name)
                    .expect("projected export column")
                    .clone()
            }));
            let mut frame = DataFrame::new(output).context("failed to build export batch")?;
            if let Some(sorter) = &mut sorter {
                let mut sink = RecordSink::new(sorter, &batch_ranks, job);
                CsvWriter::new(&mut sink)
                    .include_header(false)
                    .finish(&mut frame)
                    .context("failed to encode sorted CSV batch")?;
                if !sink.complete() {
                    return Err(AppError::Message("Incomplete encoded CSV batch.".into()));
                }
            } else {
                CsvWriter::new(&mut *writer)
                    .include_header(false)
                    .finish(&mut frame)
                    .context("failed to write CSV batch")?;
            }
            written += ids.len();
            job.check_cancelled()?;
        }
        if written != source.target.ids.len() {
            return Err(AppError::Message(
                "Export rows changed while reading source data.".into(),
            ));
        }
        if let Some(sorter) = sorter {
            job.report_progress(2, Some(3), Some("Ordering CSV".into()));
            let mut records = sorter.finish(job)?;
            let mut expected = 0u64;
            while let Some(rank) = records.write_next(writer, false, job)? {
                if rank != expected {
                    return Err(AppError::Message("Invalid export row order.".into()));
                }
                expected += 1;
            }
            if expected as usize != written {
                return Err(AppError::Message("Missing sorted export rows.".into()));
            }
        }
        Ok(written)
    })?;
    job.report_progress(3, Some(3), Some("Export complete".into()));
    Ok(ExportResult {
        rows,
        columns: headers,
        warnings,
    })
}

fn export_ids(
    scope: &str,
    payload: &ExportProjectPayload,
    total: usize,
    ids: &Arc<Vec<usize>>,
) -> Result<Arc<Vec<usize>>, AppError> {
    match scope {
        "all" | "filtered" => Ok(ids.clone()),
        "selected" => {
            let requested = payload
                .row_indices
                .as_ref()
                .ok_or_else(|| AppError::Message("Selected export requires row indices.".into()))?;
            if requested.is_empty() {
                return Err(AppError::Message(
                    "Select at least one row to export.".into(),
                ));
            }
            if let Some(invalid) = requested.iter().find(|id| **id >= total) {
                return Err(AppError::Message(format!(
                    "Selected row id {invalid} is out of range."
                )));
            }
            let mut remaining: HashSet<usize> = requested.iter().copied().collect();
            let result = ids
                .iter()
                .copied()
                .filter(|id| remaining.remove(id))
                .collect::<Vec<_>>();
            if !remaining.is_empty() {
                return Err(AppError::Message(
                    "Selected rows are no longer in the current result. Select again.".into(),
                ));
            }
            Ok(Arc::new(result))
        }
        other => Err(AppError::Message(format!("Unknown export scope: {other}."))),
    }
}

fn write_headers(writer: &mut impl Write, headers: &[String]) -> Result<(), AppError> {
    let mut csv = csv::Writer::from_writer(writer);
    csv.write_record(headers)
        .context("failed to write CSV header")?;
    csv.flush().context("failed to flush CSV header")?;
    Ok(())
}

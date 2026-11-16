use std::{fs, path::Path, sync::Arc, time::SystemTime};

use crate::{
    error::AppError,
    formats::CompiledFormat,
    jobs::JobContext,
    ops::{
        analysis_data::{AnalysisSource, SelectedRows},
        query::QuerySpec,
    },
    project_io::ProjectBatchReader,
    store::ProjectsStore,
    value_utils::bounded_text::present_text,
};

use super::{
    aggregate::{limit_error, Accumulator, ExtractValue, Limits},
    ExtractPayload, ExtractScope,
};

const MAX_METADATA_BYTES: usize = 512 * 1024;
const MAX_COLUMNS: usize = 4096;

pub(super) struct ExtractedTable {
    pub values: Vec<ExtractValue>,
    pub filename: String,
    pub description: String,
}

pub(super) fn build(
    store: &ProjectsStore,
    payload: ExtractPayload,
    limits: Limits,
    job: &JobContext,
) -> Result<ExtractedTable, AppError> {
    job.check_cancelled()?;
    if payload.columns.is_empty() || payload.columns.len() > MAX_COLUMNS {
        return Err(AppError::Message(
            "Choose between 1 and 4,096 data columns to extract.".into(),
        ));
    }
    validate_metadata_size(&payload)?;
    let matcher = CompiledFormat::new(&payload.format)?;
    let path = store.project_dir(&payload.project_id).join("data.parquet");
    let before = revision(&path)?;
    // Ranking membership is independent of the source table's display order.
    let mut spec = match payload.scope {
        ExtractScope::All => QuerySpec::default(),
        ExtractScope::Filtered => payload.spec,
    };
    spec.sort_key = None;
    spec.sort_direction = None;
    let source = AnalysisSource::open(store, payload.project_id, &spec, job)?;
    let columns = source.columns(&payload.columns, "extraction columns")?;
    let mut unique = std::collections::HashSet::new();
    if columns.iter().any(|column| !unique.insert(column)) {
        return Err(AppError::Message(
            "Choose each extraction column only once.".into(),
        ));
    }
    let total_rows = source.target.total_rows;
    let source_rows = source.target.ids.len();
    let name = source
        .meta
        .source_file_name
        .as_deref()
        .unwrap_or(&source.meta.name);
    let filename = format!(
        "extract-{}-{}.csv",
        payload.format.id,
        safe_name(&source.meta.name)
    );
    let description = format!(
        "{} extracted from {} ({} source rows)",
        payload.format.name, name, source_rows
    );
    let ids = (source_rows != total_rows).then(|| Arc::clone(&source.target.ids));
    drop(source);
    let mut accumulator = Accumulator::new(limits);
    walk(
        &path,
        &columns,
        total_rows,
        ids.as_deref().map(Vec::as_slice),
        &matcher,
        &mut accumulator,
        job,
    )?;
    if before != revision(&path)? {
        return Err(AppError::Message(
            "The source data changed. Run extraction again.".into(),
        ));
    }
    let values = accumulator.finish(job)?;
    Ok(ExtractedTable {
        values,
        filename,
        description,
    })
}

fn revision(path: &Path) -> Result<(u64, SystemTime), AppError> {
    let metadata = fs::metadata(path)
        .map_err(|error| AppError::Message(format!("Could not read extraction source: {error}")))?;
    Ok((
        metadata.len(),
        metadata
            .modified()
            .map_err(|error| AppError::Message(error.to_string()))?,
    ))
}

fn safe_name(name: &str) -> String {
    let mut safe = String::new();
    for character in name.chars() {
        let character = if character.is_control() || r#"<>:"/\|?*"#.contains(character) {
            '_'
        } else {
            character
        };
        // Leave room for the format ID and extension on filesystems whose
        // component limit is measured in bytes, including Japanese filenames.
        if safe.len() + character.len_utf8() > 80 {
            break;
        }
        safe.push(character);
    }
    let name = safe.trim_end_matches([' ', '.']);
    if name.is_empty() {
        "source".into()
    } else {
        name.into()
    }
}

fn walk(
    path: &Path,
    columns: &[String],
    total_rows: usize,
    ids: Option<&[usize]>,
    matcher: &CompiledFormat,
    accumulator: &mut Accumulator,
    job: &JobContext,
) -> Result<(), AppError> {
    let mut reader = match ids {
        Some(ids) => ProjectBatchReader::for_rows(path, columns, ids),
        None => ProjectBatchReader::open(path, columns),
    }
    .map_err(AppError::from)?;
    let selected = ids.map(|ids| SelectedRows::new(total_rows, ids));
    let expected = ids.map_or(total_rows, <[usize]>::len);
    let mut read_rows = 0;
    let mut checked = 0usize;
    job.report_progress(0, Some(expected as u64), Some("Extracting values".into()));
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        let series = columns
            .iter()
            .map(|column| batch.frame.column(column))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| AppError::Other(error.into()))?;
        for local in 0..batch.frame.height() {
            if local.is_multiple_of(256) {
                job.check_cancelled()?;
            }
            let row = batch.offset + local;
            if selected
                .as_ref()
                .is_some_and(|selected| !selected.contains(row))
            {
                continue;
            }
            for series in &series {
                job.check_cancelled()?;
                let value = series
                    .get(local)
                    .map_err(|error| AppError::Other(error.into()))?;
                let Some(text) =
                    present_text(&value, accumulator.available()).map_err(|_| limit_error())?
                else {
                    continue;
                };
                let transient = match &text {
                    std::borrow::Cow::Owned(value) => value.capacity(),
                    _ => 0,
                };
                for found in matcher.occurrences(&text) {
                    if checked.is_multiple_of(256) {
                        job.check_cancelled()?;
                    }
                    checked = checked.wrapping_add(1);
                    accumulator.observe(row, found, transient, matcher)?;
                }
            }
            read_rows += 1;
        }
        job.report_progress(
            read_rows as u64,
            Some(expected as u64),
            Some("Extracting values".into()),
        );
    }
    job.check_cancelled()?;
    if read_rows != expected {
        return Err(AppError::Message(
            "Extraction source rows changed while scanning.".into(),
        ));
    }
    Ok(())
}

fn validate_metadata_size(payload: &ExtractPayload) -> Result<(), AppError> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            if self.0 > MAX_METADATA_BYTES {
                return Err(std::io::Error::other("extraction request too large"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(&mut Counter(0), payload).map_err(|_| {
        AppError::Message(
            "Extraction request exceeds 512 KiB. Reduce its columns or conditions.".into(),
        )
    })
}

#[cfg(test)]
mod filename_tests {
    use super::safe_name;

    #[test]
    fn generated_csv_names_bound_utf8_bytes_and_remove_path_separators() {
        let name = safe_name(&"調査🔍".repeat(50));
        assert!(name.len() <= 80 && !name.is_empty());
        assert!("調査🔍".repeat(50).starts_with(&name));
        assert_eq!(safe_name("host/path\\log:one. "), "host_path_log_one");
        assert_eq!(safe_name("..."), "source");
    }
}

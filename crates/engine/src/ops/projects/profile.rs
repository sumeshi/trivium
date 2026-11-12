use std::{collections::HashMap, fs, path::Path, time::UNIX_EPOCH};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    analysis::{infer::series_kind_with_job, ColumnKind},
    error::AppError,
    jobs::JobContext,
    project_io::{ProjectBatchReader, ProjectColumns},
    storage::{
        compute_column_max_chars, load_setting_bytes, save_setting, COLUMN_WIDTH_SAMPLE_ROWS,
    },
};

const PROFILE_KEY: &str = "column_profile";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Signature {
    bytes: u64,
    modified_nanos: String,
    rows: usize,
    schema: Vec<String>,
}

impl Signature {
    fn read(path: &Path, rows: usize, schema: &[String]) -> Result<Self, AppError> {
        let metadata = fs::metadata(path).map_err(anyhow::Error::from)?;
        let modified = metadata.modified().map_err(anyhow::Error::from)?;
        let nanos = modified
            .duration_since(UNIX_EPOCH)
            .map_err(|error| AppError::Message(format!("Invalid data modification time: {error}")))?
            .as_nanos()
            .to_string();
        Ok(Self {
            bytes: metadata.len(),
            modified_nanos: nanos,
            rows,
            schema: schema.to_vec(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ColumnProfile {
    signature: Signature,
    pub kinds: HashMap<String, String>,
    pub widths: HashMap<String, usize>,
}

/// Merge complete batch classifications without losing mixed-type evidence.
/// Empty text/null batches contribute no type; datetime mixed with numbers is
/// text, while integer mixed with floating point remains numeric.
fn merge(left: ColumnKind, right: ColumnKind) -> ColumnKind {
    use ColumnKind::*;
    match (left, right) {
        (Null, other) | (other, Null) => other,
        (Int, Float) | (Float, Int) => Float,
        (left, right) if left == right => left,
        _ => String,
    }
}

#[derive(Default)]
pub(super) struct KindAccumulator(HashMap<String, ColumnKind>);

impl KindAccumulator {
    pub fn observe(
        &mut self,
        frame: &DataFrame,
        columns: &[String],
        job: &JobContext,
    ) -> Result<(), AppError> {
        for name in columns {
            if self.0.get(name) == Some(&ColumnKind::String) {
                continue;
            }
            if let Ok(series) = frame.column(name) {
                let kind = series_kind_with_job(series, job)?;
                self.0
                    .entry(name.clone())
                    .and_modify(|old| *old = merge(*old, kind))
                    .or_insert(kind);
            }
        }
        Ok(())
    }

    pub fn finish(mut self, columns: &[String]) -> HashMap<String, String> {
        for name in columns {
            self.0.entry(name.clone()).or_insert(ColumnKind::Null);
        }
        self.0
            .into_iter()
            .map(|(name, kind)| (name, kind.as_str().to_string()))
            .collect()
    }
}

pub(super) fn save_import_profile(
    project_dir: &Path,
    rows: usize,
    schema: &[String],
    kinds: HashMap<String, String>,
    widths: HashMap<String, usize>,
) -> Result<(), AppError> {
    let signature = Signature::read(&project_dir.join("data.parquet"), rows, schema)?;
    let profile = ColumnProfile {
        signature,
        kinds,
        widths,
    };
    save_setting(
        project_dir,
        PROFILE_KEY,
        &serde_json::to_value(profile).map_err(anyhow::Error::from)?,
    )
    .map_err(AppError::from)
}

/// New imports already have a profile. Old projects build it once through
/// projected bounded batches, then use footer/stat validation on later opens.
pub(super) fn load_profile(
    project_dir: &Path,
    rows: usize,
    schema: &[String],
    columns: &ProjectColumns,
    job: &JobContext,
) -> Result<ColumnProfile, AppError> {
    job.check_cancelled()?;
    let path = project_dir.join("data.parquet");
    let signature = Signature::read(&path, rows, schema)?;
    let ids = columns.ids();
    let cached = load_setting_bytes(project_dir, PROFILE_KEY).map_err(AppError::from)?;
    let had_profile = cached.is_some();
    if let Some(bytes) = cached {
        if let Ok(profile) = serde_json::from_slice::<ColumnProfile>(&bytes) {
            if profile.signature == signature
                && ids
                    .iter()
                    .all(|id| profile.kinds.contains_key(id) && profile.widths.contains_key(id))
            {
                return Ok(profile);
            }
        }
    }
    // Preserve the existing leading-sample width estimate when migrating a
    // legacy project. A changed Parquet signature requires fresh estimates.
    let cached_widths = if had_profile {
        None
    } else {
        load_setting_bytes(project_dir, "column_max_chars")
            .map_err(AppError::from)?
            .and_then(|bytes| serde_json::from_slice::<HashMap<String, usize>>(&bytes).ok())
            .filter(|widths| ids.iter().all(|id| widths.contains_key(id)))
    };
    let mut remaining_width_rows = if cached_widths.is_some() {
        0
    } else {
        COLUMN_WIDTH_SAMPLE_ROWS
    };
    let mut widths = cached_widths.unwrap_or_else(|| {
        ids.iter()
            .map(|id| (id.clone(), id.chars().count()))
            .collect()
    });
    let mut inferred = KindAccumulator::default();
    let mut reader = ProjectBatchReader::open(&path, &ids).map_err(AppError::from)?;
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        inferred.observe(&batch.frame, &ids, job)?;
        if remaining_width_rows > 0 {
            let sample = batch
                .frame
                .slice(0, remaining_width_rows.min(batch.frame.height()));
            for (id, width) in compute_column_max_chars(&sample) {
                widths
                    .entry(id)
                    .and_modify(|previous| *previous = (*previous).max(width))
                    .or_insert(width);
            }
            remaining_width_rows -= sample.height();
        }
    }
    job.check_cancelled()?;
    let profile = ColumnProfile {
        signature,
        kinds: inferred.finish(&ids),
        widths,
    };
    save_setting(
        project_dir,
        PROFILE_KEY,
        &serde_json::to_value(&profile).map_err(anyhow::Error::from)?,
    )
    .map_err(AppError::from)?;
    Ok(profile)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classification_merge_preserves_mixed_type_evidence() {
        use ColumnKind::*;
        assert_eq!(merge(Null, Datetime), Datetime);
        assert_eq!(merge(Int, Float), Float);
        assert_eq!(merge(Datetime, Int), String);
        assert_eq!(merge(Float, Datetime), String);
        assert_eq!(merge(String, Int), String);
    }
}

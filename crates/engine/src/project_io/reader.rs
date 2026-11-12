use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{anyhow, Context, Result};
use polars::{
    export::arrow::{array::new_empty_array, datatypes::ArrowSchema},
    prelude::{DataFrame, Series},
};
use polars_parquet::read::{infer_schema, read_metadata, RowGroupDeserializer, RowGroupReader};

use crate::jobs::JobContext;

use super::read_stats::{record_batch, record_group, record_io};

pub const PROJECT_BATCH_ROWS: usize = 8192;

struct TrackedFile(File);

impl Read for TrackedFile {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = self.0.read(bytes)?;
        record_io(count);
        Ok(count)
    }
}

impl Seek for TrackedFile {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(position)
    }
}

/// A bounded frame and its original, zero-based file-row offset.
#[derive(Debug)]
pub struct ProjectBatch {
    pub offset: usize,
    pub frame: DataFrame,
}

/// Projected, row-group-selective Parquet iteration, retaining one compressed
/// row group and at most one decoded 8,192-row batch inside this reader.
///
/// No synthetic row-id column or value statistics are needed, so legacy files
/// and files without statistics use exactly the same selection path.
pub struct ProjectBatchReader {
    inner: RowGroupReader<TrackedFile>,
    schema: ArrowSchema,
    groups: Vec<(usize, usize, u64)>,
    group_index: usize,
    active: Option<RowGroupDeserializer>,
    offset: usize,
    total_rows: usize,
    finished: bool,
}

impl ProjectBatchReader {
    pub fn open(path: &Path, columns: &[String]) -> Result<Self> {
        Self::select(path, columns, None, |_| true)
    }

    /// Select groups intersecting the half-open original row range.
    pub fn for_range(path: &Path, columns: &[String], start: usize, end: usize) -> Result<Self> {
        Self::select(path, columns, Some(end), |range| {
            start < end && range.0 < end && start < range.1
        })
    }

    /// Select groups containing requested original IDs; ordering and duplicates
    /// are intentionally left to the caller gathering its result.
    pub fn for_rows(path: &Path, columns: &[String], rows: &[usize]) -> Result<Self> {
        let mut sorted = rows.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        Self::select(path, columns, None, |range| {
            let index = sorted.partition_point(|row| *row < range.0);
            sorted.get(index).is_some_and(|row| *row < range.1)
        })
    }

    fn select(
        path: &Path,
        columns: &[String],
        range_end: Option<usize>,
        mut include: impl FnMut((usize, usize)) -> bool,
    ) -> Result<Self> {
        let mut file = TrackedFile(File::open(path)?);
        let metadata = read_metadata(&mut file).context("failed to read Parquet footer")?;
        let full_schema = infer_schema(&metadata).context("failed to infer Parquet schema")?;
        let fields = columns
            .iter()
            .map(|name| {
                full_schema
                    .fields
                    .iter()
                    .find(|field| field.name == *name)
                    .cloned()
                    .ok_or_else(|| anyhow!("Column not found: {name}"))
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = ArrowSchema {
            fields,
            metadata: full_schema.metadata,
        };
        let mut groups = Vec::new();
        let mut selected = Vec::new();
        let mut offset = 0;
        for group in &metadata.row_groups {
            let end = offset + group.num_rows();
            if !schema.fields.is_empty() && include((offset, end)) {
                let bytes = group
                    .columns()
                    .iter()
                    .filter(|column| columns.contains(&column.descriptor().path_in_schema[0]))
                    .map(|column| column.byte_range().1)
                    .sum();
                groups.push((offset, end, bytes));
                selected.push(group.clone());
            }
            offset = end;
        }
        // RowGroupReader's limit is relative to the selected groups, not the
        // original file. Stop the final decoder at the requested range end.
        let limit = range_end.map(|end| {
            groups
                .iter()
                .map(|(start, stop, _)| end.min(*stop).saturating_sub(*start))
                .sum()
        });
        Ok(Self {
            inner: RowGroupReader::new(
                file,
                schema.clone(),
                selected,
                Some(PROJECT_BATCH_ROWS),
                limit,
                None,
            ),
            schema,
            groups,
            group_index: 0,
            active: None,
            offset: 0,
            total_rows: metadata.num_rows,
            finished: false,
        })
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// End offset of the row group that supplied the most recent batch.
    pub fn current_row_group_end(&self) -> usize {
        self.group_index
            .checked_sub(1)
            .map(|index| self.groups[index].1)
            .unwrap_or(0)
    }

    /// Drop unread decoded batches when the caller has consumed every desired
    /// ID from this group. The next call starts the next selected row group.
    pub fn skip_current_row_group(&mut self) {
        self.active = None;
    }

    /// An empty frame retaining the requested column order and logical dtypes.
    pub fn empty_frame(&self) -> Result<DataFrame> {
        let columns = self
            .schema
            .fields
            .iter()
            .map(|field| Series::try_from((field, new_empty_array(field.data_type.clone()))))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(DataFrame::new(columns)?)
    }

    /// Checks cancellation before every row-group I/O and decoded batch.
    pub fn next_with_job(&mut self, job: &JobContext) -> Option<Result<ProjectBatch>> {
        self.next_impl(Some(job))
    }

    fn next_impl(&mut self, job: Option<&JobContext>) -> Option<Result<ProjectBatch>> {
        if self.finished {
            return None;
        }
        match self.read_next(job) {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }

    fn read_next(&mut self, job: Option<&JobContext>) -> Result<Option<ProjectBatch>> {
        loop {
            if let Some(job) = job {
                job.check_cancelled()?;
            }
            if let Some(active) = &mut self.active {
                if let Some(chunk) = active.next() {
                    let chunk = chunk.context("failed to decode Parquet batch")?;
                    record_batch(chunk.len());
                    let columns = self
                        .schema
                        .fields
                        .iter()
                        .zip(chunk.into_arrays())
                        .map(|(field, array)| Series::try_from((field, array)))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let frame = DataFrame::new(columns)?;
                    let offset = self.offset;
                    self.offset += frame.height();
                    return Ok(Some(ProjectBatch { offset, frame }));
                }
                self.active = None;
            }
            let Some(group) = self.inner.next() else {
                return Ok(None);
            };
            let group = group.context("failed to read projected Parquet row group")?;
            let (offset, _, bytes) = self.groups[self.group_index];
            self.group_index += 1;
            record_group(bytes);
            self.offset = offset;
            self.active = Some(group);
        }
    }
}

impl Iterator for ProjectBatchReader {
    type Item = Result<ProjectBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl(None)
    }
}

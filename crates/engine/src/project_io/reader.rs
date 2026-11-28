use std::{
    borrow::Cow,
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{anyhow, Context, Result};
use polars::prelude::{ArrowSchema, DataFrame, DataType, IntoSeries, Series, StringChunked};
use polars_buffer::Buffer;
use polars_parquet::parquet::read::PageMetaData;
use polars_parquet::read::{
    column_iter_to_arrays, infer_schema, read_metadata, BasicDecompressor, Filter, PageReader,
    RowGroupMetadata,
};

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

struct SelectedGroup {
    start: usize,
    end: usize,
    decode_rows: usize,
    metadata: RowGroupMetadata,
}

struct CompressedColumn {
    metadata: PageMetaData,
    bytes: Buffer<u8>,
}

struct ActiveGroup {
    /// One entry per projected Arrow field, including all leaves of nested fields.
    columns: Vec<Vec<CompressedColumn>>,
    next_row: usize,
    decode_rows: usize,
    total_rows: usize,
}

/// Projected, row-group-selective Parquet iteration, retaining one compressed
/// row group and at most one decoded 8,192-row batch inside this reader.
///
/// Row-group selection uses file offsets without decoding row IDs or values.
/// Flat project columns are decoded with a range filter, never by slicing a
/// decoded full row group. Parquet dictionaries and individual decompressed
/// pages are decoder workspace in addition to that output batch.
pub struct ProjectBatchReader {
    file: TrackedFile,
    schema: ArrowSchema,
    groups: Vec<SelectedGroup>,
    group_index: usize,
    active: Option<ActiveGroup>,
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
    /// are intentionally left to the caller gathering its result. Decoding in
    /// the final selected group stops after the highest requested ID.
    pub fn for_rows(path: &Path, columns: &[String], rows: &[usize]) -> Result<Self> {
        let mut sorted = Cow::Borrowed(rows);
        if !rows.is_sorted() {
            let sorted = sorted.to_mut();
            sorted.sort_unstable();
            sorted.dedup();
        }
        let end = sorted.last().map(|row| row.saturating_add(1));
        Self::select(path, columns, end, |range| {
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
        let schema = columns
            .iter()
            .map(|name| {
                full_schema
                    .get(name)
                    .cloned()
                    .map(|field| (field.name.clone(), field))
                    .ok_or_else(|| anyhow!("Column not found: {name}"))
            })
            .collect::<Result<ArrowSchema>>()?;
        let mut groups = Vec::new();
        let mut offset = 0;
        for group in &metadata.row_groups {
            let end = offset + group.num_rows();
            if !schema.is_empty() && include((offset, end)) {
                groups.push(SelectedGroup {
                    start: offset,
                    end,
                    decode_rows: range_end.map_or(group.num_rows(), |limit| {
                        limit.min(end).saturating_sub(offset)
                    }),
                    metadata: group.clone(),
                });
            }
            offset = end;
        }
        Ok(Self {
            file,
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
            .map(|index| self.groups[index].end)
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
            .iter_values()
            .map(|field| {
                Series::new_empty(field.name.clone(), &DataType::from_arrow_field(field)).into()
            })
            .collect();
        Ok(DataFrame::new(0, columns)?)
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
                if active.next_row < active.decode_rows {
                    let start = active.next_row;
                    let end = start
                        .saturating_add(PROJECT_BATCH_ROWS)
                        .min(active.decode_rows);
                    let columns = self
                        .schema
                        .iter_values()
                        .zip(&active.columns)
                        .map(|(field, chunks)| {
                            if let Some(job) = job {
                                job.check_cancelled()?;
                            }
                            // Recreate page iterators over shared compressed buffers. Range
                            // pushdown skips preceding flat pages without decoding their
                            // values, and only materializes this batch's selected rows.
                            let (pages, types): (Vec<_>, Vec<_>) = chunks
                                .iter()
                                .map(|chunk| {
                                    let pages = PageReader::new_with_page_meta(
                                        Cursor::new(chunk.bytes.clone()),
                                        chunk.metadata.clone(),
                                        Vec::new(),
                                        usize::MAX,
                                    );
                                    (
                                        BasicDecompressor::new(pages, Vec::new()),
                                        &chunk.metadata.descriptor.primitive_type,
                                    )
                                })
                                .unzip();
                            let (arrays, _) = column_iter_to_arrays(
                                pages,
                                types,
                                field.clone(),
                                Some(Filter::new_ranged(start, end)),
                            )
                            .context("failed to decode Parquet batch")?;
                            let series = Series::try_from((field, arrays))?;
                            // Partial StringView batches can retain the whole dictionary.
                            // Compact their buffers directly; full physical groups already
                            // fit the batch bound and need no second copy of their text.
                            let series = if start != 0 || end != active.total_rows {
                                if let Ok(strings) = series.str() {
                                    StringChunked::from_chunk_iter(
                                        field.name.clone(),
                                        strings.downcast_iter().map(|array| array.clone().gc()),
                                    )
                                    .into_series()
                                } else {
                                    series
                                }
                            } else {
                                series
                            };
                            Ok(series.into())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let frame = DataFrame::new(end - start, columns)?;
                    record_batch(frame.height());
                    active.next_row = end;
                    let offset = self.offset;
                    self.offset += frame.height();
                    return Ok(Some(ProjectBatch { offset, frame }));
                }
                self.active = None;
            }
            let Some(group) = self.groups.get(self.group_index) else {
                return Ok(None);
            };
            let mut bytes_read = 0;
            let columns = self
                .schema
                .iter_values()
                .map(|field| {
                    group
                        .metadata
                        .columns_under_root_iter(field.name.as_str())
                        .ok_or_else(|| anyhow!("Column not found in row group: {}", field.name))?
                        .map(|column| {
                            if let Some(job) = job {
                                job.check_cancelled()?;
                            }
                            let range = column.byte_range();
                            let length = usize::try_from(range.end - range.start)?;
                            let mut bytes = vec![0; length];
                            self.file.seek(SeekFrom::Start(range.start))?;
                            self.file
                                .read_exact(&mut bytes)
                                .context("failed to read projected Parquet column")?;
                            bytes_read += length as u64;
                            Ok(CompressedColumn {
                                metadata: column.into(),
                                bytes: bytes.into(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            self.group_index += 1;
            record_group(bytes_read);
            self.offset = group.start;
            self.active = Some(ActiveGroup {
                columns,
                next_row: 0,
                decode_rows: group.decode_rows,
                total_rows: group.end - group.start,
            });
        }
    }
}

impl Iterator for ProjectBatchReader {
    type Item = Result<ProjectBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl(None)
    }
}

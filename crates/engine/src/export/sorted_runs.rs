//! Byte- and row-bounded external ordering for already-encoded output records.
use crate::{error::AppError, jobs::JobContext};
use anyhow::Context;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};
const RUN_BYTES: usize = 8 * 1024 * 1024;
const RUN_ROWS: usize = 8192;
const FAN_IN: usize = 32;

pub(crate) struct ExternalSorter {
    directory: PathBuf,
    runs: Vec<PathBuf>,
    bytes: Vec<u8>,
    entries: Vec<(u64, usize, usize)>,
}
impl ExternalSorter {
    pub fn new(directory: &Path) -> Result<Self, AppError> {
        std::fs::create_dir(directory).context("failed to create sort workspace")?;
        Ok(Self {
            directory: directory.into(),
            runs: Vec::new(),
            bytes: Vec::new(),
            entries: Vec::new(),
        })
    }
    pub fn push(&mut self, key: u64, bytes: &[u8], job: &JobContext) -> Result<(), AppError> {
        // A single oversized encoded record becomes a run directly. Do not
        // duplicate it into the ordinary run buffer or retain its allocation.
        if bytes.len() > RUN_BYTES {
            self.flush(job)?;
            let path = self.directory.join(format!("run-{}", self.runs.len()));
            let mut output =
                BufWriter::new(File::create(&path).context("failed to create oversized run")?);
            write_header(&mut output, key, bytes.len() as u64)?;
            for chunk in bytes.chunks(1024 * 1024) {
                job.check_cancelled()?;
                output
                    .write_all(chunk)
                    .context("failed to write oversized record")?;
            }
            output.flush().context("failed to flush oversized run")?;
            self.runs.push(path);
            return Ok(());
        }
        if !self.entries.is_empty()
            && (self.bytes.len().saturating_add(bytes.len()) > RUN_BYTES
                || self.entries.len() >= RUN_ROWS)
        {
            self.flush(job)?;
        }
        let start = self.bytes.len();
        let needed = start + bytes.len();
        if needed > self.bytes.capacity() {
            let mut capacity = self.bytes.capacity().max(64 * 1024);
            while capacity < needed {
                capacity = (capacity * 2).min(RUN_BYTES);
            }
            self.bytes.reserve_exact(capacity - self.bytes.len());
        }
        self.bytes.extend_from_slice(bytes);
        self.entries.push((key, start, bytes.len()));
        if self.bytes.len() >= RUN_BYTES {
            self.flush(job)?;
        }
        Ok(())
    }
    fn flush(&mut self, job: &JobContext) -> Result<(), AppError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        job.check_cancelled()?;
        self.entries.sort_unstable_by_key(|entry| entry.0);
        let path = self.directory.join(format!("run-{}", self.runs.len()));
        let mut output = BufWriter::new(File::create(&path).context("failed to create sort run")?);
        for (position, &(key, start, len)) in self.entries.iter().enumerate() {
            if position % 1024 == 0 {
                job.check_cancelled()?;
            }
            write_header(&mut output, key, len as u64)?;
            output
                .write_all(&self.bytes[start..start + len])
                .context("failed to write sort run")?;
        }
        output.flush().context("failed to flush sort run")?;
        self.runs.push(path);
        self.bytes.clear();
        self.entries.clear();
        Ok(())
    }
    pub fn finish(mut self, job: &JobContext) -> Result<SortedRecords, AppError> {
        self.flush(job)?;
        let mut pass = 0;
        while self.runs.len() > FAN_IN {
            let mut next = Vec::new();
            for (index, group) in self.runs.chunks(FAN_IN).enumerate() {
                job.check_cancelled()?;
                let path = self.directory.join(format!("merge-{pass}-{index}"));
                let mut output =
                    BufWriter::new(File::create(&path).context("failed to create merged run")?);
                let mut records = SortedRecords::open(group)?;
                while records.write_next(&mut output, true, job)?.is_some() {}
                output.flush().context("failed to flush merged run")?;
                drop(records);
                for path in group {
                    std::fs::remove_file(path).context("failed to remove consumed sort run")?;
                }
                next.push(path);
            }
            self.runs = next;
            pass += 1;
        }
        SortedRecords::open(&self.runs)
    }
}

fn write_header(output: &mut impl Write, key: u64, len: u64) -> Result<(), AppError> {
    output
        .write_all(&key.to_le_bytes())
        .context("failed to write sort key")?;
    output
        .write_all(&len.to_le_bytes())
        .context("failed to write sort length")?;
    Ok(())
}
fn read_header(input: &mut impl Read) -> Result<Option<(u64, u64)>, AppError> {
    let mut first = [0u8; 1];
    if input.read(&mut first).context("failed to read sort run")? == 0 {
        return Ok(None);
    }
    let mut key = [0u8; 8];
    key[0] = first[0];
    input
        .read_exact(&mut key[1..])
        .context("truncated sort key")?;
    let mut len = [0u8; 8];
    input
        .read_exact(&mut len)
        .context("truncated sort length")?;
    Ok(Some((u64::from_le_bytes(key), u64::from_le_bytes(len))))
}

/// Only run headers are retained in the heap. Even very large CSV records are
/// copied using a fixed-sized IO buffer rather than one allocation per run.
pub(crate) struct SortedRecords {
    readers: Vec<BufReader<File>>,
    heap: BinaryHeap<Reverse<(u64, usize, u64)>>,
}
impl SortedRecords {
    fn open(paths: &[PathBuf]) -> Result<Self, AppError> {
        let mut result = Self {
            readers: Vec::new(),
            heap: BinaryHeap::new(),
        };
        for (index, path) in paths.iter().enumerate() {
            let mut reader = BufReader::new(File::open(path).context("failed to open sort run")?);
            if let Some((key, len)) = read_header(&mut reader)? {
                result.heap.push(Reverse((key, index, len)));
            }
            result.readers.push(reader);
        }
        Ok(result)
    }
    pub fn write_next(
        &mut self,
        output: &mut impl Write,
        header: bool,
        job: &JobContext,
    ) -> Result<Option<u64>, AppError> {
        job.check_cancelled()?;
        let Some(Reverse((key, index, len))) = self.heap.pop() else {
            return Ok(None);
        };
        if header {
            write_header(output, key, len)?;
        }
        let copied = io::copy(&mut (&mut self.readers[index]).take(len), output)
            .context("failed to merge sort record")?;
        if copied != len {
            return Err(AppError::Message("Truncated sorted record.".into()));
        }
        if let Some((key, len)) = read_header(&mut self.readers[index])? {
            self.heap.push(Reverse((key, index, len)));
        }
        Ok(Some(key))
    }
    pub fn next_rank(&mut self, job: &JobContext) -> Result<Option<(usize, usize)>, AppError> {
        let mut bytes = Vec::with_capacity(8);
        let Some(key) = self.write_next(&mut bytes, false, job)? else {
            return Ok(None);
        };
        let array: [u8; 8] = bytes
            .try_into()
            .map_err(|_| AppError::Message("Invalid rank record.".into()))?;
        Ok(Some((key as usize, u64::from_le_bytes(array) as usize)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_record_is_not_copied_into_the_run_buffer() {
        let path = std::env::temp_dir().join(format!("trivium-oversized-{}", uuid::Uuid::new_v4()));
        let job = JobContext::none();
        let mut sorter = ExternalSorter::new(&path).unwrap();
        let record = vec![b'x'; RUN_BYTES + 1];
        sorter.push(0, &record, &job).unwrap();
        assert_eq!(sorter.bytes.capacity(), 0);
        assert!(sorter.entries.is_empty());
        let mut records = sorter.finish(&job).unwrap();
        let mut output = Vec::new();
        assert_eq!(
            records.write_next(&mut output, false, &job).unwrap(),
            Some(0)
        );
        assert_eq!(output, record);
        drop(records);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn multiple_merge_passes_preserve_every_rank_and_raw_record() {
        let path = std::env::temp_dir().join(format!("trivium-runs-{}", uuid::Uuid::new_v4()));
        let job = JobContext::none();
        let mut sorter = ExternalSorter::new(&path).unwrap();
        let count = RUN_ROWS * (FAN_IN + 1) + 3;
        for key in (0..count).rev() {
            sorter
                .push(key as u64, &(key as u64).to_le_bytes(), &job)
                .unwrap();
        }
        let mut records = sorter.finish(&job).unwrap();
        for key in 0..count {
            assert_eq!(records.next_rank(&job).unwrap(), Some((key, key)));
        }
        assert_eq!(records.next_rank(&job).unwrap(), None);
        drop(records);
        std::fs::remove_dir_all(path).unwrap();
    }
}

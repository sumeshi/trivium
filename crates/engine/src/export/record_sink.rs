//! Split Polars' exact CSV encoding into records, including embedded newlines.
use super::sorted_runs::ExternalSorter;
use crate::jobs::JobContext;
use std::io::{self, Write};
pub(crate) struct RecordSink<'a> {
    sorter: &'a mut ExternalSorter,
    ranks: &'a [usize],
    job: &'a JobContext,
    record: Vec<u8>,
    quoted: bool,
    written: usize,
}
impl<'a> RecordSink<'a> {
    pub fn new(sorter: &'a mut ExternalSorter, ranks: &'a [usize], job: &'a JobContext) -> Self {
        Self {
            sorter,
            ranks,
            job,
            record: Vec::new(),
            quoted: false,
            written: 0,
        }
    }
    pub fn complete(&self) -> bool {
        self.record.is_empty() && !self.quoted && self.written == self.ranks.len()
    }
}
impl Write for RecordSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for &byte in bytes {
            self.record.push(byte);
            if byte == b'"' {
                self.quoted = !self.quoted;
            }
            if byte == b'\n' && !self.quoted {
                let rank = self
                    .ranks
                    .get(self.written)
                    .ok_or_else(|| io::Error::other("CSV writer emitted unexpected row"))?;
                self.sorter
                    .push(*rank as u64, &self.record, self.job)
                    .map_err(io::Error::other)?;
                self.record.clear();
                self.written += 1;
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

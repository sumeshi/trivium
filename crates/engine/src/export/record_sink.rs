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
        let mut start = 0;
        for (index, &byte) in bytes.iter().enumerate() {
            if byte == b'"' {
                self.quoted = !self.quoted;
            } else if byte == b'\n' && !self.quoted {
                let rank = self
                    .ranks
                    .get(self.written)
                    .ok_or_else(|| io::Error::other("CSV writer emitted unexpected row"))?;
                let segment = &bytes[start..=index];
                if self.record.is_empty() {
                    // Polars normally supplies whole records together. Pass
                    // their exact bytes through without a second row buffer.
                    self.sorter
                        .push(*rank as u64, segment, self.job)
                        .map_err(io::Error::other)?;
                } else {
                    self.record.extend_from_slice(segment);
                    self.sorter
                        .push(*rank as u64, &self.record, self.job)
                        .map_err(io::Error::other)?;
                    self.record.clear();
                }
                self.written += 1;
                start = index + 1;
            }
        }
        self.record.extend_from_slice(&bytes[start..]);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quoted_newlines_and_escaped_quotes_survive_every_chunk_boundary() {
        let rows: [&[u8]; 3] = [
            b"\"a\"\"b\nc\",1\n",
            b"\"\",2\n",
            b"plain,\"quote\"\"end\"\n",
        ];
        let encoded = rows.concat();
        let expected = [rows[1], rows[2], rows[0]].concat();
        let job = JobContext::none();
        for chunk_size in 1..=encoded.len() {
            let directory =
                std::env::temp_dir().join(format!("trivium-records-{}", uuid::Uuid::new_v4()));
            let mut sorter = ExternalSorter::new(&directory).unwrap();
            let mut sink = RecordSink::new(&mut sorter, &[2, 0, 1], &job);
            for chunk in encoded.chunks(chunk_size) {
                sink.write_all(chunk).unwrap();
            }
            assert!(sink.complete(), "chunk size {chunk_size}");
            let mut sorted = sorter.finish(&job).unwrap();
            let mut output = Vec::new();
            while sorted
                .write_next(&mut output, false, &job)
                .unwrap()
                .is_some()
            {}
            assert_eq!(output, expected, "chunk size {chunk_size}");
            drop(sorted);
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}

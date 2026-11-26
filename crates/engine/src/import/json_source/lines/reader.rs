//! Read only aligned newline boundaries and never retain more than one bounded
//! encoded JSONL record. Oversized records are discarded up to the next known
//! boundary without allocating their remainder.
use std::io::{BufRead, BufReader, Read};

use crate::{error::AppError, jobs::JobContext};

const READ_BYTES: usize = 64 * 1024;

pub(super) struct RawLine {
    pub bytes: Vec<u8>,
    pub oversized: bool,
    pub incomplete_unit: bool,
}

pub(super) struct Lines<R> {
    reader: BufReader<R>,
    width: usize,
    newline: [u8; 4],
    limit: usize,
}

impl<R: Read> Lines<R> {
    pub fn new(reader: R, width: usize, big_endian: bool, limit: usize) -> Self {
        let mut newline = [0; 4];
        newline[if big_endian { width - 1 } else { 0 }] = b'\n';
        Self {
            reader: BufReader::with_capacity(READ_BYTES, reader),
            width,
            newline,
            limit,
        }
    }

    pub fn next(&mut self, job: &JobContext, recover: bool) -> Result<Option<RawLine>, AppError> {
        let mut bytes = Vec::new();
        let mut oversized = false;
        let mut seen = false;
        let mut unit = [0u8; 4];
        let mut pending = 0;
        loop {
            job.check_cancelled()?;
            let available = self
                .reader
                .fill_buf()
                .map_err(|error| AppError::Message(format!("Failed to read file: {error}")))?;
            if available.is_empty() {
                if !seen {
                    return Ok(None);
                }
                if pending > 0 && !oversized {
                    if bytes.len().saturating_add(pending) > self.limit {
                        oversized = true;
                    } else {
                        bytes.extend_from_slice(&unit[..pending]);
                    }
                }
                return Ok(Some(RawLine {
                    bytes,
                    oversized,
                    incomplete_unit: pending != 0,
                }));
            }
            seen = true;
            let mut used = 0;
            let mut ended = false;
            if self.width == 1 {
                used = available
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(available.len(), |i| i + 1);
                ended = available[used - 1] == b'\n';
                if !oversized {
                    if bytes.len().saturating_add(used) > self.limit {
                        oversized = true;
                    } else {
                        bytes.extend_from_slice(&available[..used]);
                    }
                }
            } else {
                for &byte in available {
                    unit[pending] = byte;
                    pending += 1;
                    used += 1;
                    if pending == self.width {
                        pending = 0;
                        if !oversized {
                            if bytes.len().saturating_add(self.width) > self.limit {
                                oversized = true;
                            } else {
                                bytes.extend_from_slice(&unit[..self.width]);
                            }
                        }
                        if unit[..self.width] == self.newline[..self.width] {
                            ended = true;
                            break;
                        }
                    }
                }
            }
            self.reader.consume(used);
            if oversized && !recover {
                return Ok(Some(RawLine {
                    bytes,
                    oversized,
                    incomplete_unit: false,
                }));
            }
            if ended {
                return Ok(Some(RawLine {
                    bytes,
                    oversized,
                    incomplete_unit: false,
                }));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_lines_discard_without_retaining_their_remainder() {
        let data = [b'a'; 100_000]
            .into_iter()
            .chain(*b"\nlast\n")
            .collect::<Vec<_>>();
        let mut lines = Lines::new(data.as_slice(), 1, false, 16);
        let line = lines.next(&JobContext::none(), true).unwrap().unwrap();
        assert!(line.oversized);
        assert!(line.bytes.len() <= 16);
        assert_eq!(
            lines
                .next(&JobContext::none(), true)
                .unwrap()
                .unwrap()
                .bytes,
            b"last\n"
        );
        assert!(lines.next(&JobContext::none(), true).unwrap().is_none());
    }

    #[test]
    fn wide_newlines_require_aligned_complete_units_and_keep_partial_eof() {
        let bytes = [0, 0x0a, b'a', 0, 0x0a, 0, b'z'];
        let mut lines = Lines::new(bytes.as_slice(), 2, false, 100);
        let line = lines.next(&JobContext::none(), true).unwrap().unwrap();
        assert_eq!(line.bytes, &bytes[..6]);
        assert!(!line.incomplete_unit);
        let line = lines.next(&JobContext::none(), true).unwrap().unwrap();
        assert_eq!(line.bytes, [b'z']);
        assert!(line.incomplete_unit);
    }

    #[test]
    fn unicode_units_may_cross_short_reads_without_changing_line_boundaries() {
        struct OneByte<'a>(&'a [u8]);
        impl Read for OneByte<'_> {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                if output.is_empty() || self.0.is_empty() {
                    return Ok(0);
                }
                output[0] = self.0[0];
                self.0 = &self.0[1..];
                Ok(1)
            }
        }
        for (width, big_endian, source, expected) in [
            (
                2,
                true,
                &[0, b'a', 0, b'\n', 0, b'b'][..],
                &[0, b'a', 0, b'\n'][..],
            ),
            (
                4,
                false,
                &[b'a', 0, 0, 0, b'\n', 0, 0, 0, b'b', 0, 0, 0][..],
                &[b'a', 0, 0, 0, b'\n', 0, 0, 0][..],
            ),
        ] {
            let mut lines = Lines::new(OneByte(source), width, big_endian, 100);
            let first = lines.next(&JobContext::none(), true).unwrap().unwrap();
            assert_eq!(first.bytes, expected);
            assert!(!first.incomplete_unit);
            let last = lines.next(&JobContext::none(), true).unwrap().unwrap();
            assert!(!last.incomplete_unit);
            assert_eq!(last.bytes.len(), width);
        }
    }
}

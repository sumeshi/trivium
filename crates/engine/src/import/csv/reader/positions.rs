//! csv-core counts LF only. Keep physical starts for its buffered record ordinals.
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use crate::error::AppError;

#[derive(Clone, Default)]
pub(crate) struct PhysicalPositions(Rc<RefCell<Starts>>);

#[derive(Default)]
struct Starts {
    next_record: u64,
    queued: VecDeque<(u64, usize)>,
    last: Option<(u64, usize)>,
}

impl PhysicalPositions {
    /// Called only at the first byte of a nonempty CSV record. Quoted line
    /// breaks and skipped empty lines do not allocate position entries.
    pub(crate) fn start(&self, line: usize) {
        let mut starts = self.0.borrow_mut();
        let record = starts.next_record;
        starts.next_record += 1;
        starts.queued.push_back((record, line));
    }

    /// Entries are drained as the CSV parser returns records. Its 64KiB
    /// prefetch bounds this queue; a batch's pending record reuses `last`.
    pub(crate) fn line(&self, position: Option<&csv::Position>) -> Result<usize, AppError> {
        let record = position.ok_or_else(missing)?.record();
        let mut starts = self.0.borrow_mut();
        if let Some((previous, line)) = starts.last {
            if previous == record {
                return Ok(line);
            }
        }
        while starts
            .queued
            .front()
            .is_some_and(|(next, _)| *next < record)
        {
            starts.queued.pop_front();
        }
        if starts
            .queued
            .front()
            .is_some_and(|(next, _)| *next == record)
        {
            let start = starts.queued.pop_front().unwrap();
            starts.last = Some(start);
            Ok(start.1)
        } else {
            Err(missing())
        }
    }

    /// Do not retain csv-core's LF-only line number in user-facing reasons.
    pub(crate) fn error(&self, error: &csv::Error) -> String {
        match error.kind() {
            csv::ErrorKind::Utf8 { pos, err } => self.line(pos.as_ref()).map(|line| {
                format!(
                    "Invalid UTF-8 in CSV record at line {line}, field {}, byte {}.",
                    err.field() + 1,
                    err.valid_up_to()
                )
            }),
            csv::ErrorKind::UnequalLengths {
                pos,
                expected_len,
                len,
            } => self.line(pos.as_ref()).map(|line| {
                format!("CSV record at line {line} has {len} fields; expected {expected_len}.")
            }),
            _ => return error.to_string(),
        }
        .unwrap_or_else(|_| error.to_string())
    }
}

fn missing() -> AppError {
    AppError::Message("Could not determine the CSV source record's physical line.".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumed_starts_are_discarded_and_only_the_pending_record_is_cached() {
        let starts = PhysicalPositions::default();
        for record in 0..100_000 {
            starts.start(record as usize + 1);
            let mut position = csv::Position::new();
            position.set_record(record);
            assert_eq!(starts.line(Some(&position)).unwrap(), record as usize + 1);
            assert_eq!(starts.line(Some(&position)).unwrap(), record as usize + 1);
            assert!(starts.0.borrow().queued.is_empty());
        }
    }

    #[test]
    fn quoted_physical_line_breaks_retain_one_position_instead_of_an_entry_per_line() {
        let starts = PhysicalPositions::default();
        let mut bytes = vec![b'"'];
        bytes.extend(std::iter::repeat_n(b'\r', 100_000));
        bytes.push(b'"');
        let input = super::super::quotes::QuoteCheckedReader::new(
            std::io::Cursor::new(bytes),
            b',',
            None,
            starts.clone(),
        );
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(input);
        let record = reader.records().next().unwrap().unwrap();
        assert_eq!(starts.0.borrow().queued.len(), 1);
        assert_eq!(starts.line(record.position()).unwrap(), 1);
        assert!(starts.0.borrow().queued.is_empty());
    }
}

//! Limits are charged as bytes enter a parser, before a whole record allocates.
use std::{
    cell::Cell,
    fmt,
    io::{self, Read},
    rc::Rc,
};

use crate::jobs::{self, JobContext, JobId};

pub(crate) const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const CHECKPOINT_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(crate) struct RecordTooLarge {
    pub(crate) limit: usize,
    pub(crate) source_line: Option<usize>,
}

impl fmt::Display for RecordTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Record exceeds the {} byte decoded input limit",
            self.limit
        )?;
        if let Some(line) = self.source_line {
            write!(f, " starting on line {line}")?;
        }
        write!(f, ". The unread remainder was not parsed.")
    }
}
impl std::error::Error for RecordTooLarge {}

pub(crate) fn limit_error(limit: usize, source_line: Option<usize>) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        RecordTooLarge { limit, source_line },
    )
}

pub(crate) fn check_job(id: Option<JobId>) -> io::Result<()> {
    if id.is_some_and(jobs::is_cancelled) {
        Err(io::Error::other("Cancelled."))
    } else {
        Ok(())
    }
}

/// JSON's reader is deliberately wrapped *outside* its buffer. Serde may
/// look ahead, but cannot charge bytes from an unvisited later array element.
#[derive(Clone)]
pub(crate) struct RecordBudget(Rc<BudgetState>);

struct BudgetState {
    bytes: Cell<usize>,
    checkpoint: Cell<usize>,
    exceeded: Cell<bool>,
    limit: usize,
    job: Option<JobId>,
}

impl RecordBudget {
    pub(crate) fn new(job: &JobContext) -> Self {
        Self::with_limit(job, MAX_RECORD_BYTES)
    }

    pub(crate) fn with_limit(job: &JobContext, limit: usize) -> Self {
        Self(Rc::new(BudgetState {
            bytes: Cell::new(0),
            checkpoint: Cell::new(0),
            exceeded: Cell::new(false),
            limit,
            job: job.id(),
        }))
    }

    pub(crate) fn reset(&self) {
        self.0.bytes.set(0);
        self.0.exceeded.set(false);
    }

    pub(crate) fn exceeded(&self) -> bool {
        self.0.exceeded.get()
    }

    pub(crate) fn reader<R: Read>(&self, inner: R) -> RecordReader<R> {
        RecordReader {
            inner,
            budget: self.clone(),
        }
    }
}

pub(crate) struct RecordReader<R> {
    inner: R,
    budget: RecordBudget,
}

impl<R: Read> Read for RecordReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let state = &self.budget.0;
        if state.checkpoint.get() == 0 {
            check_job(state.job)?;
        }
        let remaining = state.limit.saturating_sub(state.bytes.get());
        // Read one extra byte only to distinguish an exact-sized record at
        // EOF from an oversized record. It is never handed to the parser.
        let amount = buffer
            .len()
            .min(remaining.saturating_add(1))
            .min(CHECKPOINT_BYTES);
        let count = self.inner.read(&mut buffer[..amount])?;
        if count > remaining {
            state.exceeded.set(true);
            return Err(limit_error(state.limit, None));
        }
        state.bytes.set(state.bytes.get() + count);
        let checkpoint = state.checkpoint.get() + count;
        state.checkpoint.set(if checkpoint >= CHECKPOINT_BYTES {
            0
        } else {
            checkpoint
        });
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn exact_input_limit_is_allowed_and_excess_never_reaches_the_parser() {
        let budget = RecordBudget::with_limit(&JobContext::none(), 8);
        let mut reader = budget.reader(Cursor::new(b"12345678"));
        let mut text = Vec::new();
        reader.read_to_end(&mut text).unwrap();
        assert_eq!(text, b"12345678");
        assert!(!budget.exceeded());
        let budget = RecordBudget::with_limit(&JobContext::none(), 8);
        let mut reader = budget.reader(Cursor::new(b"123456789tail"));
        let mut buffer = [0; 8];
        assert_eq!(reader.read(&mut buffer).unwrap(), 8);
        assert!(reader
            .read(&mut buffer)
            .unwrap_err()
            .to_string()
            .contains("8 byte"));
        assert!(budget.exceeded());
    }

    #[test]
    fn array_element_budget_resets_without_rewinding_input() {
        let budget = RecordBudget::with_limit(&JobContext::none(), 4);
        let mut reader = budget.reader(Cursor::new(b"abcdefgh"));
        let mut buffer = [0; 4];
        reader.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"abcd");
        budget.reset();
        reader.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"efgh");
    }

    #[test]
    fn cancellation_is_checked_inside_one_unfinished_record() {
        use crate::jobs::{queue_job, request_cancel, JobKind};
        struct CancelOnRead<R> {
            inner: R,
            job: JobId,
        }
        impl<R: Read> Read for CancelOnRead<R> {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let count = self.inner.read(buffer)?;
                request_cancel(self.job);
                Ok(count)
            }
        }
        let guard = queue_job(JobKind::Import, None).unwrap();
        let id = guard.id();
        let job = JobContext::with_job(guard, None);
        let budget = RecordBudget::new(&job);
        let mut reader = budget.reader(CancelOnRead {
            inner: io::repeat(b'x').take(1024 * 1024),
            job: id,
        });
        let mut received = Vec::new();
        assert_eq!(
            reader.read_to_end(&mut received).unwrap_err().to_string(),
            "Cancelled."
        );
        assert!(
            !received.is_empty(),
            "cancellation happened during, not before, reading"
        );
        assert!(received.len() <= 2 * CHECKPOINT_BYTES);
        assert!(
            !budget.exceeded(),
            "cancellation is distinct from oversize recovery"
        );
    }
}

//! The CSV crate accepts an open quoted field at EOF. Recovery must identify
//! that unknown tail instead of fabricating a final merged record.
use super::positions::PhysicalPositions;
use crate::{
    import::record_limit::{
        check_job, limit_error, RecordTooLarge, CHECKPOINT_BYTES, MAX_RECORD_BYTES,
    },
    jobs::JobId,
};
use std::{
    fmt,
    io::{self, Read},
};

#[derive(Debug)]
struct UnclosedQuote {
    line: usize,
    record_line: usize,
}

impl fmt::Display for UnclosedQuote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unterminated quoted CSV field starting on line {} (record starting on line {}); the remaining record boundary is unknown.", self.line, self.record_line)
    }
}
impl std::error::Error for UnclosedQuote {}

pub(crate) fn unreadable_tail(error: &csv::Error) -> Option<usize> {
    let csv::ErrorKind::Io(error) = error.kind() else {
        return None;
    };
    let error = error.get_ref()?;
    error
        .downcast_ref::<UnclosedQuote>()
        .map(|error| error.record_line)
        .or_else(|| {
            error
                .downcast_ref::<RecordTooLarge>()
                .and_then(|error| error.source_line)
        })
}

enum State {
    Start,
    Plain,
    Quoted,
    AfterQuote,
}

pub(super) struct QuoteCheckedReader<R> {
    inner: R,
    separator: u8,
    state: State,
    line: usize,
    quote_line: usize,
    previous_cr: bool,
    bom_prefix: usize,
    record_bytes: usize,
    record_line: usize,
    record_fields: usize,
    limit: usize,
    job: Option<JobId>,
    pending: Option<io::Error>,
    stopped: bool,
    positions: PhysicalPositions,
    active_record: bool,
}

impl<R: Read> QuoteCheckedReader<R> {
    pub(super) fn new(
        inner: R,
        separator: u8,
        job: Option<JobId>,
        positions: PhysicalPositions,
    ) -> Self {
        Self::with_positions_limit(inner, separator, job, MAX_RECORD_BYTES, positions)
    }

    #[cfg(test)]
    fn with_limit(inner: R, separator: u8, job: Option<JobId>, limit: usize) -> Self {
        Self::with_positions_limit(inner, separator, job, limit, PhysicalPositions::default())
    }

    fn with_positions_limit(
        inner: R,
        separator: u8,
        job: Option<JobId>,
        limit: usize,
        positions: PhysicalPositions,
    ) -> Self {
        Self {
            inner,
            separator,
            state: State::Start,
            line: 1,
            quote_line: 1,
            previous_cr: false,
            bom_prefix: 0,
            record_bytes: 0,
            record_line: 1,
            record_fields: 1,
            limit,
            job,
            pending: None,
            stopped: false,
            positions,
            active_record: false,
        }
    }

    fn observe(&mut self, byte: u8) -> io::Result<()> {
        if self.bom_prefix < 3 {
            if byte == [0xEF, 0xBB, 0xBF][self.bom_prefix] {
                self.bom_prefix += 1;
                return Ok(());
            }
            self.finish_prefix();
        }
        let newline = matches!(byte, b'\r' | b'\n');
        if !newline && !self.active_record {
            self.start_record();
        }
        let record_end = newline && !matches!(self.state, State::Quoted);
        if byte == self.separator && !matches!(self.state, State::Quoted) {
            let maximum =
                super::super::MAX_CSV_COLUMNS + crate::annotation_columns::ANNOTATION_COLUMNS.len();
            if self.record_fields >= maximum {
                return Err(io::Error::new(io::ErrorKind::InvalidData, format!(
                    "CSV record starting on line {} exceeds the limit of {} data columns: at least {} data columns remain even after allowing a complete leading Trivium annotation block. No columns were discarded.",
                    self.record_line, super::super::MAX_CSV_COLUMNS, super::super::MAX_CSV_COLUMNS + 1
                )));
            }
            self.record_fields += 1;
        }
        if !record_end {
            if self.record_bytes >= self.limit {
                return Err(limit_error(self.limit, Some(self.record_line)));
            }
            self.record_bytes += 1;
        }
        self.state = match self.state {
            State::Start if byte == b'"' => {
                self.quote_line = self.line;
                State::Quoted
            }
            State::Start if byte == self.separator || newline => State::Start,
            State::Start => State::Plain,
            State::Plain if byte == self.separator || newline => State::Start,
            State::Plain => State::Plain,
            State::Quoted if byte == b'"' => State::AfterQuote,
            State::Quoted => State::Quoted,
            State::AfterQuote if byte == b'"' => State::Quoted,
            State::AfterQuote if byte == self.separator || newline => State::Start,
            State::AfterQuote => State::Plain,
        };
        if byte == b'\r' || (byte == b'\n' && !self.previous_cr) {
            self.line += 1;
        }
        self.previous_cr = byte == b'\r';
        if record_end {
            self.record_bytes = 0;
            self.record_line = self.line;
            self.record_fields = 1;
            self.active_record = false;
        }
        Ok(())
    }

    fn start_record(&mut self) {
        self.active_record = true;
        self.record_line = self.line;
        self.positions.start(self.line);
    }

    fn finish_prefix(&mut self) {
        if (1..3).contains(&self.bom_prefix) {
            self.state = State::Plain;
            self.record_bytes += self.bom_prefix;
            self.start_record();
        }
        self.bom_prefix = 3;
    }
}

impl<R: Read> Read for QuoteCheckedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if let Some(error) = self.pending.take() {
            self.stopped = true;
            return Err(error);
        }
        if self.stopped {
            return Ok(0);
        }
        check_job(self.job)?;
        let amount = buffer.len().min(CHECKPOINT_BYTES);
        let read = self.inner.read(&mut buffer[..amount])?;
        if read == 0 {
            self.finish_prefix();
        }
        if read == 0 && matches!(self.state, State::Quoted) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                UnclosedQuote {
                    line: self.quote_line,
                    record_line: self.record_line,
                },
            ));
        }
        for (position, byte) in buffer[..read].iter().enumerate() {
            if let Err(error) = self.observe(*byte) {
                if position == 0 {
                    self.stopped = true;
                    return Err(error);
                }
                self.pending = Some(error);
                return Ok(position);
            }
        }
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn limit_error_is_deferred_so_complete_prefix_records_are_not_lost() {
        let source = b"a,b\n1,ok\n2,ok\n3,\"a much larger unfinished\ntail\n";
        let input = QuoteCheckedReader::with_limit(Cursor::new(source), b',', None, 12);
        let mut reader = csv::Reader::from_reader(input);
        let mut rows = reader.records();
        assert_eq!(
            rows.next().unwrap().unwrap().iter().collect::<Vec<_>>(),
            ["1", "ok"]
        );
        assert_eq!(
            rows.next().unwrap().unwrap().iter().collect::<Vec<_>>(),
            ["2", "ok"]
        );
        let error = rows.next().unwrap().unwrap_err();
        assert_eq!(unreadable_tail(&error), Some(4));
        assert!(error.to_string().contains("12 byte"));
        assert!(rows.next().is_none());
    }

    #[test]
    fn newline_inside_a_quoted_cell_does_not_reset_its_record_budget() {
        let input = QuoteCheckedReader::with_limit(
            Cursor::new(b"a\r\n\"123\n456\"\r\nb\r\n"),
            b',',
            None,
            8,
        );
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(input);
        let mut rows = reader.records();
        assert_eq!(&rows.next().unwrap().unwrap()[0], "a");
        let error = rows.next().unwrap().unwrap_err();
        assert_eq!(unreadable_tail(&error), Some(2));
    }

    #[test]
    fn exact_sized_records_and_crlf_boundaries_remain_valid() {
        let input =
            QuoteCheckedReader::with_limit(Cursor::new(b"a,b\r\nc,d\r\ne,f"), b',', None, 3);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(input);
        let rows = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(&rows[2][1], "f");
    }

    #[test]
    fn excessive_empty_fields_fail_before_the_parser_allocates_their_slots() {
        let maximum = super::super::super::MAX_CSV_COLUMNS
            + crate::annotation_columns::ANNOTATION_COLUMNS.len();
        let source = vec![b','; maximum];
        let input = QuoteCheckedReader::new(
            Cursor::new(source),
            b',',
            None,
            PhysicalPositions::default(),
        );
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(input);
        let error = reader.records().next().unwrap().unwrap_err();
        assert!(error.to_string().contains("4096 data columns"));
        assert_eq!(
            unreadable_tail(&error),
            None,
            "a hard schema limit must not be salvaged as a valid prefix"
        );
    }
}

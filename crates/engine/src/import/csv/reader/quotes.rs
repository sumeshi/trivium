//! The CSV crate accepts an open quoted field at EOF. Recovery must identify
//! that unknown tail instead of fabricating a final merged record.
use std::{
    fmt,
    io::{self, Read},
};

#[derive(Debug)]
struct UnclosedQuote {
    line: usize,
}

impl fmt::Display for UnclosedQuote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unterminated quoted CSV field starting on line {}; the remaining record boundary is unknown.", self.line)
    }
}
impl std::error::Error for UnclosedQuote {}

pub(crate) fn unclosed_quote(error: &csv::Error) -> Option<usize> {
    let csv::ErrorKind::Io(error) = error.kind() else {
        return None;
    };
    error
        .get_ref()?
        .downcast_ref::<UnclosedQuote>()
        .map(|error| error.line)
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
}

impl<R: Read> QuoteCheckedReader<R> {
    pub(super) fn new(inner: R, separator: u8) -> Self {
        Self {
            inner,
            separator,
            state: State::Start,
            line: 1,
            quote_line: 1,
            previous_cr: false,
            bom_prefix: 0,
        }
    }

    fn observe(&mut self, byte: u8) {
        if self.bom_prefix < 3 {
            if byte == [0xEF, 0xBB, 0xBF][self.bom_prefix] {
                self.bom_prefix += 1;
                return;
            }
            if self.bom_prefix > 0 {
                self.state = State::Plain;
            }
            self.bom_prefix = 3;
        }
        let newline = matches!(byte, b'\r' | b'\n');
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
    }
}

impl<R: Read> Read for QuoteCheckedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let read = self.inner.read(buffer)?;
        if read == 0 && matches!(self.state, State::Quoted) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                UnclosedQuote {
                    line: self.quote_line,
                },
            ));
        }
        for byte in &buffer[..read] {
            self.observe(*byte);
        }
        Ok(read)
    }
}

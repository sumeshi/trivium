use std::io::{self, Read};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Encoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

/// Reads the leading BOM bytes and returns the encoding plus the byte offset
/// where the payload starts.
pub(crate) fn detect_bom<R: Read>(reader: &mut R) -> io::Result<(Encoding, u64)> {
    let mut prefix = [0u8; 3];
    let mut length = 0;
    while length < prefix.len() {
        match reader.read(&mut prefix[length..]) {
            Ok(0) => break,
            Ok(read) => length += read,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    if length >= 2 && prefix[0] == 0xFF && prefix[1] == 0xFE {
        return Ok((Encoding::Utf16Le, 2));
    }
    if length >= 2 && prefix[0] == 0xFE && prefix[1] == 0xFF {
        return Ok((Encoding::Utf16Be, 2));
    }
    if length >= 3 && prefix == [0xEF, 0xBB, 0xBF] {
        return Ok((Encoding::Utf8, 3));
    }
    Ok((Encoding::Utf8, 0))
}

/// Streaming UTF-16 to UTF-8 adapter.
///
/// The adapter reads one 16-bit unit at a time, so an odd byte, a surrogate
/// pair, or a CSV record may cross any underlying buffer boundary. A high
/// surrogate is held until the following unit arrives; a lone surrogate or a
/// trailing odd byte is an error rather than a replacement character.
pub(crate) struct Utf16Reader<R> {
    inner: R,
    big_endian: bool,
    high_surrogate: Option<u16>,
    output: Vec<u8>,
    output_pos: usize,
    eof: bool,
}

impl<R: Read> Utf16Reader<R> {
    pub(crate) fn new(inner: R, big_endian: bool) -> Self {
        Self {
            inner,
            big_endian,
            high_surrogate: None,
            output: Vec::with_capacity(4),
            output_pos: 0,
            eof: false,
        }
    }

    fn read_byte(&mut self) -> io::Result<Option<u8>> {
        let mut byte = [0u8; 1];
        loop {
            match self.inner.read(&mut byte) {
                Ok(0) => return Ok(None),
                Ok(_) => return Ok(Some(byte[0])),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
    }

    fn read_unit(&mut self) -> io::Result<Option<u16>> {
        let Some(first) = self.read_byte()? else {
            return Ok(None);
        };
        let Some(second) = self.read_byte()? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid UTF-16 data: odd byte length.",
            ));
        };
        Ok(Some(if self.big_endian {
            u16::from_be_bytes([first, second])
        } else {
            u16::from_le_bytes([first, second])
        }))
    }

    fn decode_next_char(&mut self) -> io::Result<bool> {
        if self.eof {
            return Ok(false);
        }
        let codepoint = if let Some(high) = self.high_surrogate.take() {
            match self.read_unit()? {
                Some(low) if (0xDC00..=0xDFFF).contains(&low) => {
                    0x10000 + (((high as u32 - 0xD800) << 10) | (low as u32 - 0xDC00))
                }
                Some(_) | None => return Err(unpaired_surrogate()),
            }
        } else {
            match self.read_unit()? {
                None => {
                    self.eof = true;
                    return Ok(false);
                }
                Some(unit) if (0xD800..=0xDBFF).contains(&unit) => {
                    self.high_surrogate = Some(unit);
                    return self.decode_next_char();
                }
                Some(unit) if (0xDC00..=0xDFFF).contains(&unit) => {
                    return Err(unpaired_surrogate());
                }
                Some(unit) => unit as u32,
            }
        };
        let ch = char::from_u32(codepoint).ok_or_else(unpaired_surrogate)?;
        let mut buffer = [0u8; 4];
        self.output.clear();
        self.output_pos = 0;
        self.output
            .extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
        Ok(true)
    }
}

fn unpaired_surrogate() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid UTF-16 data: unpaired surrogate.",
    )
}

impl<R: Read> Read for Utf16Reader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let mut written = 0;
        while written < out.len() {
            if self.output_pos >= self.output.len()
                && !self.decode_next_char()? {
                    break;
                }
            let available = self.output.len() - self.output_pos;
            let amount = available.min(out.len() - written);
            out[written..written + amount]
                .copy_from_slice(&self.output[self.output_pos..self.output_pos + amount]);
            self.output_pos += amount;
            written += amount;
        }
        Ok(written)
    }
}

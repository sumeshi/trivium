//! Presets are small settings files, not another path for reading whole logs.
use std::{
    io::{self, Read, Write},
    path::Path,
};

use anyhow::Context;
use serde::Serialize;

use crate::{error::AppError, source_file::open_regular};

const MAX_PRESET_BYTES: usize = 32 * 1024 * 1024;

fn limit_error() -> AppError {
    AppError::Message("Preset file exceeds the 32 MiB size limit.".into())
}

pub(super) fn read(path: &Path) -> Result<Vec<u8>, AppError> {
    let file =
        open_regular(path).with_context(|| format!("failed to read preset {}", path.display()))?;
    if file.metadata().context("failed to inspect preset")?.len() > MAX_PRESET_BYTES as u64 {
        return Err(limit_error());
    }
    read_limited(file)
}

fn read_limited(reader: impl Read) -> Result<Vec<u8>, AppError> {
    let mut data = Vec::new();
    // A file can grow after metadata is checked. Do not read it all first.
    reader
        .take(MAX_PRESET_BYTES as u64 + 1)
        .read_to_end(&mut data)
        .context("failed to read preset")?;
    if data.len() > MAX_PRESET_BYTES {
        return Err(limit_error());
    }
    Ok(data)
}

pub(super) fn write(value: &impl Serialize, output: &mut impl Write) -> Result<(), AppError> {
    let mut writer = LimitedWriter {
        output,
        remaining: MAX_PRESET_BYTES,
        full: false,
    };
    serde_json::to_writer_pretty(&mut writer, value).map_err(|error| {
        if writer.full {
            limit_error()
        } else {
            anyhow::Error::new(error)
                .context("failed to write preset")
                .into()
        }
    })
}

struct LimitedWriter<'a, W> {
    output: &'a mut W,
    remaining: usize,
    full: bool,
}

impl<W: Write> Write for LimitedWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            self.full = true;
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "preset size limit",
            ));
        }
        let written = self.output.write(bytes)?;
        self.remaining -= written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_growing_source_is_read_only_until_one_byte_beyond_the_limit() {
        let mut remaining = io::repeat(b' ').take(MAX_PRESET_BYTES as u64 + 50);
        assert!(read_limited(&mut remaining)
            .unwrap_err()
            .to_string()
            .contains("32 MiB"));
        assert_eq!(remaining.limit(), 49);
    }

    #[test]
    fn writer_accepts_the_exact_budget_and_never_writes_a_chunk_beyond_it() {
        let mut bytes = Vec::new();
        let mut writer = LimitedWriter {
            output: &mut bytes,
            remaining: 4,
            full: false,
        };
        writer.write_all(b"ab").unwrap();
        writer.write_all(b"cd").unwrap();
        assert!(writer.write_all(b"e").is_err());
        assert!(writer.full);
        assert_eq!(bytes, b"abcd");
    }
}

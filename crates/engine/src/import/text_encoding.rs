//! Decode a staged source without changing the bytes covered by its source hash.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use encoding_rs::{DecoderResult, Encoding};

use super::options::TextEncoding;
use crate::{error::AppError, jobs::JobContext};

const BUFFER_BYTES: usize = 64 * 1024;

pub struct PreparedSource {
    pub path: PathBuf,
    pub encoding: String,
    pub bom: bool,
    /// The caller removes this owned UTF-8 copy before publishing the project.
    /// It starts with one synthetic UTF-8 BOM, consumed once by the parser.
    pub decoded: bool,
}

/// Auto recognizes Unicode BOMs only. BOM-less legacy encodings require an
/// explicit selection; malformed code units are never replaced with U+FFFD.
/// UTF-8 is passed through so recovery can reject an individual bad record.
pub fn prepare_source(
    source: &Path,
    decoded_destination: &Path,
    requested: TextEncoding,
    job: &JobContext,
) -> Result<PreparedSource, AppError> {
    job.check_cancelled()?;
    let mut input = crate::source_file::open_regular(source)
        .map_err(|error| failure("open the staged source", error))?;
    let (encoding, skip, bom) = resolve_encoding(&mut input, requested)?;
    let prepared = PreparedSource {
        path: source.to_path_buf(),
        encoding: label(encoding).to_owned(),
        bom,
        decoded: false,
    };
    if encoding == TextEncoding::Utf8 {
        return Ok(prepared);
    }

    input
        .seek(SeekFrom::Start(skip))
        .map_err(|error| failure("seek the staged source", error))?;
    // create_new ensures a failed decode never removes a pre-existing file.
    let mut cleanup = DecodedCopy(None);
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(decoded_destination)
        .map_err(|error| failure("create the decoded import copy", error))?;
    cleanup.0 = Some(decoded_destination.to_path_buf());
    // Protect a literal leading U+FEFF in the decoded payload. Both parsers
    // consume one UTF-8 marker, so provide a marker rather than sacrificing
    // the first source character when it happens to have the same bytes.
    output
        .write_all(b"\xef\xbb\xbf")
        .map_err(|error| failure("write the decoded import copy", error))?;
    let result = match encoding {
        TextEncoding::Utf32Le | TextEncoding::Utf32Be => {
            decode_utf32(&mut input, &mut output, encoding, skip, job)
        }
        _ => decode_legacy(&mut input, &mut output, encoding, skip, job),
    };
    // Windows cannot delete an open output file on the error path.
    drop(output);
    result?;
    job.check_cancelled()?;
    let path = cleanup.keep();
    Ok(PreparedSource {
        path,
        decoded: true,
        ..prepared
    })
}

/// Inspect a staged JSONL source without decoding the entire file. Selected
/// recovery decodes its independently bounded physical lines in both passes.
pub fn inspect_source(
    source: &Path,
    requested: TextEncoding,
    job: &JobContext,
) -> Result<PreparedSource, AppError> {
    job.check_cancelled()?;
    let mut input = crate::source_file::open_regular(source)
        .map_err(|error| failure("open the staged source", error))?;
    let (encoding, _, bom) = resolve_encoding(&mut input, requested)?;
    Ok(PreparedSource {
        path: source.to_owned(),
        encoding: label(encoding).to_owned(),
        bom,
        decoded: false,
    })
}

pub(crate) fn resolve_encoding(
    input: &mut File,
    requested: TextEncoding,
) -> Result<(TextEncoding, u64, bool), AppError> {
    let (marked, skip) =
        detect_bom(input).map_err(|error| failure("read the source encoding", error))?;
    let encoding = match (requested, marked) {
        (TextEncoding::Auto, marked) => marked.unwrap_or(TextEncoding::Utf8),
        (selected, Some(marked)) if selected != marked => {
            return Err(AppError::Message(format!(
                "Selected encoding {} conflicts with the {} byte-order mark. Use Auto or the matching encoding.",
                label(selected), label(marked)
            )));
        }
        (selected, _) => selected,
    };
    Ok((encoding, skip, marked.is_some()))
}

struct DecodedCopy(Option<PathBuf>);

impl DecodedCopy {
    fn keep(mut self) -> PathBuf {
        self.0.take().unwrap()
    }
}

impl Drop for DecodedCopy {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = fs::remove_file(path);
        }
    }
}

fn detect_bom(input: &mut File) -> io::Result<(Option<TextEncoding>, u64)> {
    let mut prefix = [0; 4];
    let mut length = 0;
    while length < prefix.len() {
        match input.read(&mut prefix[length..]) {
            Ok(0) => break,
            Ok(read) => length += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    let found = if length == 4 && prefix == [0xff, 0xfe, 0, 0] {
        (Some(TextEncoding::Utf32Le), 4)
    } else if length == 4 && prefix == [0, 0, 0xfe, 0xff] {
        (Some(TextEncoding::Utf32Be), 4)
    } else if length >= 3 && prefix[..3] == [0xef, 0xbb, 0xbf] {
        (Some(TextEncoding::Utf8), 3)
    } else if length >= 2 && prefix[..2] == [0xff, 0xfe] {
        (Some(TextEncoding::Utf16Le), 2)
    } else if length >= 2 && prefix[..2] == [0xfe, 0xff] {
        (Some(TextEncoding::Utf16Be), 2)
    } else {
        (None, 0)
    };
    Ok(found)
}

pub(crate) fn label(encoding: TextEncoding) -> &'static str {
    match encoding {
        TextEncoding::Auto => "auto",
        TextEncoding::Utf8 => "utf-8",
        TextEncoding::Utf16Le => "utf-16le",
        TextEncoding::Utf16Be => "utf-16be",
        TextEncoding::Utf32Le => "utf-32le",
        TextEncoding::Utf32Be => "utf-32be",
        TextEncoding::Windows1252 => "windows-1252",
        TextEncoding::ShiftJis => "shift-jis",
    }
}

pub(crate) fn decoder_encoding(encoding: TextEncoding) -> &'static Encoding {
    match encoding {
        TextEncoding::Utf16Le => encoding_rs::UTF_16LE,
        TextEncoding::Utf16Be => encoding_rs::UTF_16BE,
        TextEncoding::Windows1252 => encoding_rs::WINDOWS_1252,
        TextEncoding::ShiftJis => encoding_rs::SHIFT_JIS,
        _ => unreachable!("Unicode UTF-8/32 are handled before this decoder"),
    }
}

fn decode_legacy(
    input: &mut File,
    output: &mut File,
    encoding: TextEncoding,
    initial_offset: u64,
    job: &JobContext,
) -> Result<(), AppError> {
    let mut decoder = decoder_encoding(encoding).new_decoder_without_bom_handling();
    let mut source = [0; BUFFER_BYTES];
    let mut decoded = [0; BUFFER_BYTES];
    let mut byte_offset = initial_offset;
    loop {
        job.check_cancelled()?;
        let count = read_chunk(input, &mut source)?;
        let mut consumed = 0;
        loop {
            job.check_cancelled()?;
            let (result, read, written) = decoder.decode_to_utf8_without_replacement(
                &source[consumed..count],
                &mut decoded,
                count == 0,
            );
            consumed += read;
            if let DecoderResult::Malformed(..) = result {
                return Err(invalid_encoding(encoding, byte_offset + consumed as u64));
            }
            output
                .write_all(&decoded[..written])
                .map_err(|error| failure("write the decoded import copy", error))?;
            match result {
                DecoderResult::InputEmpty => break,
                DecoderResult::OutputFull => {}
                DecoderResult::Malformed(..) => unreachable!(),
            }
        }
        byte_offset += count as u64;
        job.report_progress(byte_offset, None, Some("Decoding source".into()));
        if count == 0 {
            return Ok(());
        }
    }
}

fn decode_utf32(
    input: &mut File,
    output: &mut File,
    encoding: TextEncoding,
    initial_offset: u64,
    job: &JobContext,
) -> Result<(), AppError> {
    let mut source = [0; BUFFER_BYTES];
    let mut decoded = Vec::with_capacity(BUFFER_BYTES);
    let mut remaining = 0;
    let mut byte_offset = initial_offset;
    loop {
        job.check_cancelled()?;
        let count = read_chunk(input, &mut source[remaining..BUFFER_BYTES])?;
        if count == 0 {
            return if remaining == 0 {
                Ok(())
            } else {
                Err(invalid_encoding(encoding, byte_offset))
            };
        }
        let available = count + remaining;
        let complete = available - available % 4;
        decoded.clear();
        for (position, &bytes) in source[..complete].as_chunks::<4>().0.iter().enumerate() {
            let codepoint = if encoding == TextEncoding::Utf32Be {
                u32::from_be_bytes(bytes)
            } else {
                u32::from_le_bytes(bytes)
            };
            let ch = char::from_u32(codepoint)
                .ok_or_else(|| invalid_encoding(encoding, byte_offset + position as u64 * 4))?;
            decoded.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
        }
        output
            .write_all(&decoded)
            .map_err(|error| failure("write the decoded import copy", error))?;
        remaining = available - complete;
        source.copy_within(complete..available, 0);
        byte_offset += complete as u64;
        job.report_progress(byte_offset, None, Some("Decoding source".into()));
    }
}

fn read_chunk(input: &mut File, buffer: &mut [u8]) -> Result<usize, AppError> {
    loop {
        match input.read(buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result.map_err(|error| failure("read the staged source", error)),
        }
    }
}

fn invalid_encoding(encoding: TextEncoding, offset: u64) -> AppError {
    AppError::Message(format!(
        "Invalid {} data near source byte {}. No replacement characters were inserted.",
        label(encoding),
        offset,
    ))
}

fn failure(operation: &str, error: io::Error) -> AppError {
    AppError::Message(format!("Failed to {operation}: {error}"))
}

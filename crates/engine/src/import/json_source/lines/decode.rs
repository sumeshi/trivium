use std::borrow::Cow;

use encoding_rs::DecoderResult;

use crate::{
    error::AppError,
    import::{
        options::TextEncoding,
        text_encoding::{decoder_encoding, label},
    },
    jobs::JobContext,
};

pub(super) enum DecodeError {
    Record(String),
    Fatal(AppError),
}

impl From<AppError> for DecodeError {
    fn from(error: AppError) -> Self {
        Self::Fatal(error)
    }
}

pub(super) fn decode<'a>(
    bytes: &'a [u8],
    encoding: TextEncoding,
    limit: usize,
    job: &JobContext,
) -> Result<Cow<'a, str>, DecodeError> {
    if encoding == TextEncoding::Utf8 {
        return std::str::from_utf8(bytes)
            .map(Cow::Borrowed)
            .map_err(|error| DecodeError::Record(format!("Invalid UTF-8: {error}")));
    }
    let mut text = String::new();
    if matches!(encoding, TextEncoding::Utf32Le | TextEncoding::Utf32Be) {
        let (units, remainder) = bytes.as_chunks::<4>();
        if !remainder.is_empty() {
            return Err(invalid(encoding));
        }
        let mut encoded = [0; 4];
        for (position, unit) in units.iter().enumerate() {
            if position.is_multiple_of(16 * 1024) {
                job.check_cancelled()?;
            }
            let value = if encoding == TextEncoding::Utf32Be {
                u32::from_be_bytes(*unit)
            } else {
                u32::from_le_bytes(*unit)
            };
            let ch = char::from_u32(value).ok_or_else(|| invalid(encoding))?;
            append(&mut text, ch.encode_utf8(&mut encoded), limit)?;
        }
    } else {
        let mut decoder = decoder_encoding(encoding).new_decoder_without_bom_handling();
        let mut output = [0u8; 64 * 1024];
        let mut consumed = 0;
        loop {
            job.check_cancelled()?;
            let (result, read, written) =
                decoder.decode_to_utf8_without_replacement(&bytes[consumed..], &mut output, true);
            consumed += read;
            if matches!(result, DecoderResult::Malformed(..)) {
                return Err(invalid(encoding));
            }
            // encoding_rs only writes complete, valid UTF-8 sequences.
            let decoded = std::str::from_utf8(&output[..written]).map_err(|_| invalid(encoding))?;
            append(&mut text, decoded, limit)?;
            match result {
                DecoderResult::InputEmpty => break,
                DecoderResult::OutputFull => {}
                DecoderResult::Malformed(..) => unreachable!(),
            }
        }
    }
    Ok(Cow::Owned(text))
}

fn invalid(encoding: TextEncoding) -> DecodeError {
    DecodeError::Record(format!(
        "Invalid {} data. No replacement characters were inserted.",
        label(encoding)
    ))
}

fn append(text: &mut String, part: &str, limit: usize) -> Result<(), DecodeError> {
    if text.len().saturating_add(part.len()) > limit {
        return Err(DecodeError::Record(format!(
            "Decoded JSONL record exceeds the {limit}-byte record limit."
        )));
    }
    text.push_str(part);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decoded_growth_has_a_separate_limit() {
        let result = decode(
            &[0x80; 10],
            TextEncoding::Windows1252,
            20,
            &JobContext::none(),
        );
        assert!(
            matches!(result, Err(DecodeError::Record(message)) if message.contains("record limit"))
        );
    }
}

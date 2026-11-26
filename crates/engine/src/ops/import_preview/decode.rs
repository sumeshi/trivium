use encoding_rs::DecoderResult;

use super::{ImportPreview, MAX_PREVIEW_BYTES};
use crate::{
    error::AppError,
    import::{options::TextEncoding, text_encoding::decoder_encoding},
    jobs::JobContext,
};

fn append(text: &mut String, decoded: &str, preview: &mut ImportPreview) -> bool {
    let mut end = (MAX_PREVIEW_BYTES - text.len()).min(decoded.len());
    while !decoded.is_char_boundary(end) {
        end -= 1;
    }
    text.push_str(&decoded[..end]);
    if end != decoded.len() {
        preview.truncated = true;
        return false;
    }
    true
}

pub(super) fn prefix(
    bytes: &[u8],
    encoding: TextEncoding,
    complete: bool,
    preview: &mut ImportPreview,
    job: &JobContext,
) -> Result<(String, bool), AppError> {
    if encoding == TextEncoding::Utf8 {
        return Ok(match std::str::from_utf8(bytes) {
            Ok(text) => (text.into(), complete),
            Err(error) => {
                if error.error_len().is_some() || complete {
                    preview.warning(format!("Invalid UTF-8 near sample byte {}. Preview stops before invalid characters.", error.valid_up_to()));
                }
                (
                    std::str::from_utf8(&bytes[..error.valid_up_to()])
                        .unwrap()
                        .into(),
                    false,
                )
            }
        });
    }
    let mut text = String::new();
    if matches!(encoding, TextEncoding::Utf32Le | TextEncoding::Utf32Be) {
        let (units, remainder) = bytes.as_chunks::<4>();
        for (index, &unit) in units.iter().enumerate() {
            if index.is_multiple_of(4096) {
                job.check_cancelled()?;
            }
            let value = if encoding == TextEncoding::Utf32Be {
                u32::from_be_bytes(unit)
            } else {
                u32::from_le_bytes(unit)
            };
            let Some(ch) = char::from_u32(value) else {
                preview
                    .warning("Invalid UTF-32 code point. Preview stops before invalid characters.");
                return Ok((text, false));
            };
            if !append(&mut text, ch.encode_utf8(&mut [0; 4]), preview) {
                return Ok((text, false));
            }
        }
        if !remainder.is_empty() {
            if complete {
                preview.warning("Incomplete UTF-32 code unit in the source sample.");
            }
            return Ok((text, false));
        }
        return Ok((text, complete));
    }
    let mut decoder = decoder_encoding(encoding).new_decoder_without_bom_handling();
    let mut offset = 0;
    let mut output = [0u8; 16 * 1024];
    loop {
        job.check_cancelled()?;
        let (status, read, written) =
            decoder.decode_to_utf8_without_replacement(&bytes[offset..], &mut output, complete);
        offset += read;
        if !append(
            &mut text,
            std::str::from_utf8(&output[..written]).unwrap(),
            preview,
        ) {
            return Ok((text, false));
        }
        match status {
            DecoderResult::InputEmpty => return Ok((text, complete)),
            DecoderResult::OutputFull => {}
            DecoderResult::Malformed(..) => {
                preview.warning(format!("Invalid {} data in the source sample. Preview stops before invalid characters.", preview.encoding));
                return Ok((text, false));
            }
        }
    }
}

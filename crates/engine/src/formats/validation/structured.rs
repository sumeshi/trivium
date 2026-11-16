use base64::{engine::general_purpose, Engine};

pub(super) fn is_base64(value: &str) -> bool {
    let raw = value.trim_end_matches('=');
    let padding = value.len() - raw.len();
    if raw.is_empty()
        || padding > 2
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"+/".contains(&byte))
    {
        return false;
    }
    let last = match raw.as_bytes()[raw.len() - 1] {
        byte @ b'A'..=b'Z' => byte - b'A',
        byte @ b'a'..=b'z' => byte - b'a' + 26,
        byte @ b'0'..=b'9' => byte - b'0' + 52,
        b'+' => 62,
        _ => 63,
    };
    match raw.len() % 4 {
        0 => padding == 0,
        2 => (padding == 0 || padding == 2) && last & 15 == 0,
        3 => (padding == 0 || padding == 1) && last & 3 == 0,
        _ => false,
    }
}

pub(super) fn decode_base64(value: &str) -> Option<Vec<u8>> {
    if value.is_empty() || value.len() > 64 * 1024 || !is_base64(value) {
        return None;
    }
    general_purpose::STANDARD
        .decode(value)
        .or_else(|_| general_purpose::STANDARD_NO_PAD.decode(value))
        .ok()
}

pub(super) fn powershell(value: &str) -> bool {
    let Some(bytes) = decode_base64(value) else {
        return false;
    };
    if !bytes.len().is_multiple_of(2) || bytes.len() < 4 {
        return false;
    }
    let units: Vec<_> = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .copied()
        .map(u16::from_le_bytes)
        .collect();
    let Ok(text) = String::from_utf16(&units) else {
        return false;
    };
    // EncodedCommand is UTF-16LE. Decoding here checks structure only; nothing
    // is executed and callers receive the original Base64 token, not this text.
    text.chars().any(|character| !character.is_whitespace())
        && !text
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\t' | '\r' | '\n'))
}

pub(super) fn jwt(value: &str) -> bool {
    let mut parts = value.split('.');
    let (Some(header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    if header.is_empty()
        || payload.is_empty()
        || ![header, payload, signature].iter().all(|part| {
            part.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        })
    {
        return false;
    }
    let decode = |part| general_purpose::URL_SAFE_NO_PAD.decode(part).ok();
    let (Some(header), Some(payload), Some(_)) =
        (decode(header), decode(payload), decode(signature))
    else {
        return false;
    };
    let (Ok(header), Ok(payload)) = (
        serde_json::from_slice::<serde_json::Value>(&header),
        serde_json::from_slice::<serde_json::Value>(&payload),
    ) else {
        return false;
    };
    header.as_object().is_some_and(|header| {
        header
            .get("alg")
            .and_then(|value| value.as_str())
            .is_some_and(|algorithm| {
                !algorithm.is_empty()
                    && if algorithm == "none" {
                        signature.is_empty()
                    } else {
                        !signature.is_empty()
                    }
            })
    }) && payload.is_object()
}

pub(super) fn pem(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("-----BEGIN ") else {
        return false;
    };
    let Some((label, contents)) = rest.split_once("-----") else {
        return false;
    };
    if ![
        "CERTIFICATE",
        "X509 CERTIFICATE",
        "PUBLIC KEY",
        "PRIVATE KEY",
        "ENCRYPTED PRIVATE KEY",
        "RSA PRIVATE KEY",
        "RSA PUBLIC KEY",
        "EC PRIVATE KEY",
        "DSA PRIVATE KEY",
        "OPENSSH PRIVATE KEY",
        "CERTIFICATE REQUEST",
        "X509 CRL",
    ]
    .contains(&label)
    {
        return false;
    }
    let ending = format!("-----END {label}-----");
    let Some(body) = contents.strip_suffix(&ending) else {
        return false;
    };
    // Encrypted legacy PEM headers need a dedicated parser; do not silently
    // treat their text as key bytes. Ordinary PKCS8 encrypted blocks work here.
    if !body.starts_with(['\n', '\r']) || !body.ends_with(['\n', '\r']) {
        return false;
    }
    let compact: String = body
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    let Some(bytes) = decode_base64(&compact) else {
        return false;
    };
    if label == "OPENSSH PRIVATE KEY" {
        return bytes.starts_with(b"openssh-key-v1\0");
    }
    der_sequence(&bytes)
}

fn der_sequence(bytes: &[u8]) -> bool {
    if bytes.len() < 3 || bytes[0] != 0x30 {
        return false;
    }
    let (header, length) = if bytes[1] < 128 {
        (2, usize::from(bytes[1]))
    } else {
        let width = usize::from(bytes[1] & 0x7f);
        if width == 0 || width > 4 || bytes.len() < 2 + width || bytes[2] == 0 {
            return false;
        }
        let length = bytes[2..2 + width]
            .iter()
            .fold(0usize, |length, byte| (length << 8) | usize::from(*byte));
        if length < 128 {
            return false;
        }
        (2 + width, length)
    };
    length > 0 && length <= bytes.len() && header + length == bytes.len()
}

fn wire_string<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let length = u32::from_be_bytes(input.get(..4)?.try_into().ok()?) as usize;
    let value = input.get(4..4usize.checked_add(length)?)?;
    *input = &input[4 + length..];
    Some(value)
}

pub(super) fn ssh(value: &str) -> bool {
    let mut parts = value.split_ascii_whitespace();
    let (Some(kind), Some(encoded), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    let Some(bytes) = decode_base64(encoded) else {
        return false;
    };
    let mut remaining = bytes.as_slice();
    if wire_string(&mut remaining) != Some(kind.as_bytes()) {
        return false;
    }
    let valid = match kind {
        "ssh-ed25519" => wire_string(&mut remaining).is_some_and(|key| key.len() == 32),
        "ssh-rsa" => {
            let exponent = wire_string(&mut remaining);
            let modulus = wire_string(&mut remaining);
            exponent.is_some_and(|key| !key.is_empty() && key.len() <= 8)
                && modulus.is_some_and(|key| (128..=2049).contains(&key.len()))
        }
        "ecdsa-sha2-nistp256" | "ecdsa-sha2-nistp384" | "ecdsa-sha2-nistp521" => {
            let curve = kind
                .strip_prefix("ecdsa-sha2-")
                .expect("matched ECDSA type");
            let curve_matches = wire_string(&mut remaining) == Some(curve.as_bytes());
            let length = match curve {
                "nistp256" => 65,
                "nistp384" => 97,
                _ => 133,
            };
            curve_matches
                && wire_string(&mut remaining).is_some_and(|key| key.len() == length && key[0] == 4)
        }
        _ => false,
    };
    valid && remaining.is_empty()
}

pub(super) fn bitlocker(value: &str) -> bool {
    let mut count = 0;
    for part in value.split('-') {
        if part.len() != 6
            || !part.bytes().all(|b| b.is_ascii_digit())
            || !part
                .parse::<u32>()
                .is_ok_and(|number| number <= 720_885 && number.is_multiple_of(11))
        {
            return false;
        }
        count += 1;
    }
    count == 8
}

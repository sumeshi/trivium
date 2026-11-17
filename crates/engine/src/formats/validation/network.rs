use std::{
    borrow::Cow,
    net::{Ipv4Addr, Ipv6Addr},
};

/// Refanging belongs only to validation. Matchers still return the raw capture.
fn plain_value(value: &str) -> Cow<'_, str> {
    if is_defanged(value) {
        Cow::Owned(refang(value))
    } else {
        Cow::Borrowed(value)
    }
}

pub(super) fn ipv4_like(value: &str) -> bool {
    let value = plain_value(value);
    if value.contains('/') {
        cidr(&value, false)
    } else if value.contains(':') {
        ipv4_endpoint(&value)
    } else {
        value.parse::<Ipv4Addr>().is_ok()
    }
}

pub(super) fn ipv6_like(value: &str) -> bool {
    if let Some(inner) = value.strip_prefix('[') {
        inner.strip_suffix(']').is_some_and(ipv6) || ipv6_endpoint(value)
    } else if value.contains('/') {
        cidr(value, true)
    } else {
        // A final hexadecimal segment is an address component, not a port.
        ipv6(value)
    }
}

pub(super) fn domain_like(value: &str) -> bool {
    domain(&plain_value(value))
}

pub(super) fn http_url_like(value: &str) -> bool {
    http_url(&plain_value(value))
}

pub(super) fn ipv6(value: &str) -> bool {
    let address = if let Some((address, zone)) = value.split_once('%') {
        if zone.is_empty()
            || zone.len() > 64
            || !zone
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        {
            return false;
        }
        address
    } else {
        value
    };
    address.parse::<Ipv6Addr>().is_ok()
}

pub(super) fn cidr(value: &str, v6: bool) -> bool {
    let Some((address, prefix)) = value.split_once('/') else {
        return false;
    };
    if prefix.is_empty() || !prefix.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let Ok(prefix) = prefix.parse::<u8>() else {
        return false;
    };
    if v6 {
        prefix <= 128 && address.parse::<Ipv6Addr>().is_ok()
    } else {
        prefix <= 32 && address.parse::<Ipv4Addr>().is_ok()
    }
}

fn port(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) && value.parse::<u16>().is_ok()
}

pub(super) fn endpoint(value: &str) -> bool {
    ipv4_endpoint(value) || ipv6_endpoint(value)
}

fn ipv6_endpoint(value: &str) -> bool {
    value.strip_prefix('[').is_some_and(|value| {
        value
            .split_once("]:")
            .is_some_and(|(host, service)| ipv6(host) && port(service))
    })
}

fn ipv4_endpoint(value: &str) -> bool {
    value
        .rsplit_once(':')
        .is_some_and(|(host, service)| host.parse::<Ipv4Addr>().is_ok() && port(service))
}

fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

pub(super) fn domain(value: &str) -> bool {
    let value = value.strip_suffix('.').unwrap_or(value);
    if value.len() > 253 || !value.contains('.') || !value.split('.').all(label) {
        return false;
    }
    let tail = value.rsplit('.').next().unwrap_or_default();
    (tail.len() >= 2 && tail.bytes().all(|b| b.is_ascii_alphabetic()))
        || (tail.len() > 4 && tail.to_ascii_lowercase().starts_with("xn--"))
}

pub(super) fn email(value: &str) -> bool {
    if value.len() > 254 {
        return false;
    }
    let Some((local, host)) = value.rsplit_once('@') else {
        return false;
    };
    local.len() <= 64
        && !local.is_empty()
        && !local.starts_with('.')
        && !local.ends_with('.')
        && !local.contains("..")
        && local
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".!#$%&'*+-/=?^_`{|}~".contains(&b))
        && domain(host)
        && !host.ends_with('.')
}

pub(super) fn http_url(value: &str) -> bool {
    if value
        .bytes()
        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return false;
    }
    let Some((scheme, rest)) = value.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let authority = if let Some((user, host)) = authority.rsplit_once('@') {
        if user.is_empty() || user.contains('@') {
            return false;
        }
        host
    } else {
        authority
    };
    let valid = if let Some(inner) = authority.strip_prefix('[') {
        let Some((host, suffix)) = inner.split_once(']') else {
            return false;
        };
        ipv6(host) && (suffix.is_empty() || suffix.strip_prefix(':').is_some_and(port))
    } else {
        let (host, suffix) = authority
            .split_once(':')
            .map_or((authority, None), |(host, suffix)| (host, Some(suffix)));
        let numeric = host.bytes().all(|b| b.is_ascii_digit() || b == b'.');
        let valid_host = if numeric {
            host.parse::<Ipv4Addr>().is_ok()
        } else {
            label(host) || domain(host)
        };
        valid_host && suffix.is_none_or(port)
    };
    if !valid {
        return false;
    }
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1..index + 3]
                    .iter()
                    .all(u8::is_ascii_hexdigit)
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

pub(super) fn mac(value: &str) -> bool {
    if value.len() > 17 {
        return false;
    }
    if value.contains('.') {
        let pieces: Vec<_> = value.split('.').collect();
        return pieces.len() == 3
            && pieces
                .iter()
                .all(|part| part.len() == 4 && part.bytes().all(|b| b.is_ascii_hexdigit()));
    }
    let delimiter = if value.contains(':') { ':' } else { '-' };
    let pieces: Vec<_> = value.split(delimiter).collect();
    pieces.len() == 6
        && pieces
            .iter()
            .all(|part| part.len() == 2 && part.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub(super) fn sid(value: &str) -> bool {
    if value.len() > 184 {
        return false;
    }
    let pieces: Vec<_> = value.split('-').collect();
    pieces.len() >= 4
        && pieces.len() <= 18
        && pieces[0].eq_ignore_ascii_case("s")
        && pieces[1] == "1"
        && pieces[2].bytes().all(|b| b.is_ascii_digit())
        && pieces[2]
            .parse::<u64>()
            .is_ok_and(|value| value < (1_u64 << 48))
        && pieces[3..].iter().all(|part| {
            !part.is_empty()
                && part.bytes().all(|b| b.is_ascii_digit())
                && part.parse::<u32>().is_ok()
        })
}

pub(super) fn uuid(value: &str) -> bool {
    let value = value
        .strip_prefix('{')
        .and_then(|inner| inner.strip_suffix('}'))
        .unwrap_or(value);
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

pub(super) fn cve(value: &str) -> bool {
    let mut parts = value.split('-');
    let (Some(prefix), Some(year), Some(id), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    prefix.eq_ignore_ascii_case("cve")
        && year.len() == 4
        && year.bytes().all(|b| b.is_ascii_digit())
        && year.parse::<u16>().is_ok_and(|year| year >= 1999)
        && id.len() >= 4
        && id.bytes().all(|b| b.is_ascii_digit())
}

/// Validation only: source spelling and captured offsets are never changed.
pub(super) fn is_defanged(value: &str) -> bool {
    ["[.]", "(.)", "{.}", "[:]"]
        .iter()
        .any(|token| value.contains(token))
        || value
            .get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("hxxp://"))
        || value
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("hxxps://"))
}

pub(super) fn refang(value: &str) -> String {
    let restored = value
        .replace("[.]", ".")
        .replace("(.)", ".")
        .replace("{.}", ".")
        .replace("[:]", ":");
    if restored
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("hxxps"))
    {
        format!("https{}", &restored[5..])
    } else if restored
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("hxxp"))
    {
        format!("http{}", &restored[4..])
    } else {
        restored
    }
}

//! Bounded, local structure checks for candidate formats. A positive result is
//! not a claim about ownership, reachability, a signature, or an algorithm.
mod network;
mod structured;
#[cfg(test)]
mod tests;

use super::FormatValidation;

pub(super) fn accepts(kind: FormatValidation, value: &str) -> bool {
    use FormatValidation::*;
    if value.is_empty() {
        return false;
    }
    // Generic encodings can be arbitrarily long. Their syntax is checked with
    // constant memory, so search still finds oversized values and extraction's
    // existing 64 KiB value limit reports an error rather than silently skipping.
    if value.len() > 64 * 1024 && !matches!(kind, Base64 | Hex) {
        return false;
    }
    match kind {
        Ipv4Like => network::ipv4_like(value),
        Ipv6Like => network::ipv6_like(value),
        DomainLike => network::domain_like(value),
        HttpUrlLike => network::http_url_like(value),
        Ipv6 => network::ipv6(value),
        Ipv4Cidr => network::cidr(value, false),
        Ipv6Cidr => network::cidr(value, true),
        IpPort => network::endpoint(value),
        HttpUrl => network::http_url(value),
        Domain => network::domain(value),
        Email => network::email(value),
        Mac => network::mac(value),
        WindowsSid => network::sid(value),
        Uuid => network::uuid(value),
        Cve => network::cve(value),
        DefangedIpv4 | DefangedUrl | DefangedDomain => {
            if !network::is_defanged(value) {
                return false;
            }
            let restored = network::refang(value);
            match kind {
                DefangedIpv4 => restored.parse::<std::net::Ipv4Addr>().is_ok(),
                DefangedUrl => network::http_url(&restored),
                _ => network::domain(&restored),
            }
        }
        PowershellEncoded => structured::powershell(value),
        Jwt => structured::jwt(value),
        Pem => structured::pem(value),
        SshPublicKey => structured::ssh(value),
        BitlockerRecoveryKey => structured::bitlocker(value),
        Base64 => structured::is_base64(value),
        Hex => value.len().is_multiple_of(2) && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
    }
}

use base64::{engine::general_purpose, Engine};
use serde_json::json;

use super::{accepts, network, structured};
use crate::formats::{CompiledFormat, FormatDefinition, FormatValidation as V};

#[test]
fn network_structures_reject_impossible_values_without_rewriting_valid_spelling() {
    for value in ["::1", "2001:db8::1", "fe80::1%eth0", "::ffff:192.0.2.1"] {
        assert!(accepts(V::Ipv6, value), "{value}");
    }
    for value in [
        "2001:db8:::1",
        "1:2:3:4:5:6:7:8:9",
        "12345::1",
        "::ffff:999.1.1.1",
        "fe80::1%",
    ] {
        assert!(!accepts(V::Ipv6, value), "{value}");
    }
    for (kind, valid, invalid) in [
        (V::Ipv4Cidr, "192.0.2.5/24", "192.0.2.5/33"),
        (V::Ipv6Cidr, "2001:db8::1/128", "2001:db8::/129"),
        (V::IpPort, "[::1]:443", "::1:443"),
        (V::IpPort, "192.0.2.1:0", "192.0.2.1:65536"),
        (
            V::HttpUrl,
            "https://[2001:db8::1]:8443/a?x=1&y=2#part",
            "https://[2001:::1]/",
        ),
        (V::Domain, "host.longtechnology", "host.123"),
        (V::Domain, "xn--bcher-kva.example", "-host.example"),
        (
            V::Email,
            "o'brien+review@example.technology",
            "a..b@example.com",
        ),
        (V::Mac, "AA:BB:CC:DD:EE:FF", "AA:BB-CC:DD:EE:FF"),
        (V::Mac, "aabb.ccdd.eeff", "aabb.ccdd.eeff.1122"),
        (
            V::WindowsSid,
            "S-1-5-21-123-456-789-1001",
            "S-1-5-4294967296",
        ),
        (
            V::Uuid,
            "{00112233-4455-6677-8899-AABBCCDDEEFF}",
            "00112233-4455-6677-8899-AABBCCDDEEFF0",
        ),
        (V::Cve, "cve-2025-123456", "CVE-1800-1234"),
    ] {
        assert!(accepts(kind, valid), "{kind:?}: {valid}");
        assert!(!accepts(kind, invalid), "{kind:?}: {invalid}");
    }
    assert!(!network::http_url("https://host.example:99999/a"));
    assert!(!network::http_url("https://host.example/%zz"));
    assert!(!network::domain(&format!("{}.example", "a".repeat(64))));
}

#[test]
fn defanged_validation_does_not_accept_regular_values_or_normalize_the_capture() {
    for (kind, value, restored) in [
        (V::DefangedIpv4, "192[.]0[.]2[.]1", "192.0.2.1"),
        (V::DefangedDomain, "one[.]example", "one.example"),
        (
            V::DefangedUrl,
            "hxxps://host[.]example/path",
            "https://host.example/path",
        ),
    ] {
        assert!(accepts(kind, value));
        assert_eq!(network::refang(value), restored);
        assert!(!accepts(kind, restored));
    }
    assert!(!accepts(V::DefangedIpv4, "999[.]0[.]0[.]1"));
}

#[test]
fn validation_is_part_of_frozen_matching_and_preserves_adjacent_raw_utf8_spans() {
    let definition: FormatDefinition = serde_json::from_value(json!({
        "schemaVersion":1,"id":"candidate","name":"IPv6 candidate", "validation":"ipv6",
        "pattern":"(?:\\A|[ ,])(?P<value>[0-9A-Fa-f:]+)(?:[ ,]|\\z)", "valueGroup":"value"
    }))
    .unwrap();
    let frozen: FormatDefinition =
        serde_json::from_slice(&serde_json::to_vec(&definition).unwrap()).unwrap();
    assert_eq!(definition, frozen);
    let compiled = CompiledFormat::new(&frozen).unwrap();
    let text = "日本 2001:db8:::1,2001:db8::2,::1";
    let found: Vec<_> = compiled.occurrences(text).collect();
    assert_eq!(
        found.iter().map(|hit| hit.value).collect::<Vec<_>>(),
        ["2001:db8::2", "::1"]
    );
    for hit in found {
        assert_eq!(&text[hit.start..hit.end], hit.value);
    }
    assert!(compiled.has_match(text, Some("2001:db8::2")));
    assert!(!compiled.has_match("2001:db8:::1", None));
    assert!(serde_json::from_value::<FormatDefinition>(json!({"schemaVersion":1,"id":"test", "name":"Test", "pattern":".+", "validation":"run-a-command"})).is_err());
}

fn jwt(header: serde_json::Value, payload: serde_json::Value) -> String {
    let encode =
        |value| general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).unwrap());
    format!("{}.{}.c2ln", encode(header), encode(payload))
}

#[test]
fn encoded_candidates_are_checked_but_never_executed_or_cryptographically_asserted() {
    let token = jwt(json!({"alg":"HS256"}), json!({"sub":"test"}));
    assert!(accepts(V::Jwt, &token));
    assert!(!accepts(V::Jwt, &jwt(json!({"alg":"none"}), json!({}))));
    let unsigned = jwt(json!({"alg":"none"}), json!({"sub":"test"}));
    assert!(
        accepts(V::Jwt, unsigned.strip_suffix("c2ln").unwrap()),
        "unsecured JWTs are useful investigation candidates too"
    );
    assert!(!accepts(V::Jwt, &jwt(json!({"alg":"HS256"}), json!([]))));
    assert!(!accepts(V::Jwt, "e30.e30.not-a-signature=="));
    let bytes: Vec<_> = "Write-Output '日本'"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let encoded = general_purpose::STANDARD.encode(bytes);
    assert!(accepts(V::PowershellEncoded, &encoded));
    assert!(!accepts(
        V::PowershellEncoded,
        &general_purpose::STANDARD.encode([0, 0xd8])
    ));
    assert!(structured::decode_base64("Zm9vYmFy").is_some());
    assert!(
        structured::decode_base64("Zh==").is_none(),
        "reject noncanonical trailing bits"
    );
    assert!(!accepts(V::Base64, &"A".repeat(65_537)));
    assert!(
        accepts(V::Base64, &"A".repeat(65_540)),
        "large generic values do not require a decoded allocation"
    );
    assert!(
        accepts(V::Hex, &"a".repeat(65_538)),
        "extraction owns its existing oversized-value error"
    );
    assert!(!accepts(V::Hex, "aabbc"));
}

#[test]
fn pem_ssh_and_bitlocker_candidates_require_their_internal_structure() {
    let pem = "-----BEGIN CERTIFICATE-----\nMAMCAQA=\n-----END CERTIFICATE-----";
    assert!(accepts(V::Pem, pem));
    assert!(!accepts(
        V::Pem,
        &pem.replace("END CERTIFICATE", "END PRIVATE KEY")
    ));
    assert!(!accepts(V::Pem, &pem.replace("MAMCAQA=", "MAMCAQ==")));
    let wire = |length: u32| {
        let mut wire = Vec::new();
        for value in [
            b"ssh-ed25519".as_slice(),
            vec![7; length as usize].as_slice(),
        ] {
            wire.extend_from_slice(&(value.len() as u32).to_be_bytes());
            wire.extend_from_slice(value);
        }
        general_purpose::STANDARD.encode(wire)
    };
    assert!(accepts(
        V::SshPublicKey,
        &format!("ssh-ed25519 {}", wire(32))
    ));
    assert!(!accepts(
        V::SshPublicKey,
        &format!("ssh-ed25519 {}", wire(31))
    ));
    assert!(!accepts(V::SshPublicKey, &format!("ssh-rsa {}", wire(32))));
    let valid = "000011-000022-000033-000044-000055-000066-000077-000088";
    assert!(accepts(V::BitlockerRecoveryKey, valid));
    assert!(!accepts(
        V::BitlockerRecoveryKey,
        &valid.replace("000011", "000012")
    ));
    assert!(!accepts(
        V::BitlockerRecoveryKey,
        &valid.replace("000011", "720896")
    ));
}

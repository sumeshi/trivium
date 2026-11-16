use serde::{Deserialize, Serialize};

/// A portable snapshot. Saved investigations retain the definition they applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FormatDefinition {
    pub schema_version: u32,
    pub id: String,
    pub name: String,
    pub pattern: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub value_group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub validation: Option<FormatValidation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub classifier: Option<FormatClassifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub examples: Option<Vec<FormatExample>>,
}

/// Optional local structure checks, shared by search and extraction. These
/// checks never normalize the captured spelling, access a network, or execute it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "kebab-case")]
pub enum FormatValidation {
    Ipv6,
    Ipv4Cidr,
    Ipv6Cidr,
    IpPort,
    HttpUrl,
    Domain,
    Email,
    Mac,
    WindowsSid,
    Uuid,
    Cve,
    DefangedIpv4,
    DefangedUrl,
    DefangedDomain,
    PowershellEncoded,
    Jwt,
    Pem,
    SshPublicKey,
    BitlockerRecoveryKey,
    Base64,
    Hex,
}

/// Exact raw values expected from one source cell, including adjacent matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(deny_unknown_fields)]
pub struct FormatExample {
    pub text: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(deny_unknown_fields)]
pub struct FormatClassifier {
    pub kind: String,
    pub rules: Vec<CidrRule>,
    pub default: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(deny_unknown_fields)]
pub struct CidrRule {
    pub cidr: String,
    pub label: String,
}

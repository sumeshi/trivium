use std::{collections::HashSet, net::Ipv4Addr};

use super::FormatClassifier;
use crate::error::AppError;

#[derive(Debug)]
pub(super) struct Classifier {
    rules: Vec<(u32, u32, String)>,
    default: String,
}

fn valid_label(label: &str) -> bool {
    !label.trim().is_empty() && label.len() <= 64
}

impl Classifier {
    pub(super) fn new(definition: &FormatClassifier) -> Result<Self, AppError> {
        if definition.kind != "ipv4-cidr" {
            return Err(AppError::Message(
                "Unsupported format classifier; use ipv4-cidr.".into(),
            ));
        }
        if !valid_label(&definition.default) || definition.rules.len() > 512 {
            return Err(AppError::Message(
                "Invalid classifier label or too many CIDR rules (maximum 512).".into(),
            ));
        }
        let mut seen = HashSet::new();
        let mut rules = Vec::new();
        for rule in &definition.rules {
            let invalid =
                || AppError::Message(format!("Invalid or duplicate format CIDR: {}", rule.cidr));
            let (address, prefix) = rule.cidr.split_once('/').ok_or_else(invalid)?;
            if prefix.is_empty() || !prefix.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }
            let prefix: u32 = prefix.parse().map_err(|_| invalid())?;
            if prefix > 32 || !valid_label(&rule.label) {
                return Err(invalid());
            }
            let address: Ipv4Addr = address.parse().map_err(|_| invalid())?;
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            let network = u32::from(address);
            if network & mask != network || !seen.insert((network, mask)) {
                return Err(invalid());
            }
            rules.push((network, mask, rule.label.clone()));
        }
        // More-specific rules override broader ranges, independent of JSON order.
        rules.sort_by_key(|(_, mask, _)| std::cmp::Reverse(mask.count_ones()));
        Ok(Self {
            rules,
            default: definition.default.clone(),
        })
    }

    pub(super) fn classify(&self, raw: &str) -> &str {
        let Ok(address) = raw.parse::<Ipv4Addr>() else {
            return "Unclassified";
        };
        let address = u32::from(address);
        self.rules
            .iter()
            .find(|(network, mask, _)| address & mask == *network)
            .map_or(self.default.as_str(), |(_, _, label)| label.as_str())
    }
}

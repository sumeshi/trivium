use std::collections::HashMap;

use crate::flags::normalize_flag_value;
use crate::ioc::IocRowOutcome;
use crate::models::FlagEntry;

/// Per-row flag after user-flag precedence. A non-empty user flag wins;
/// an empty user flag (including memo-only entries) leaves the IOC flag.
pub fn final_flags(
    height: usize,
    flags: &HashMap<usize, FlagEntry>,
    ioc_outcomes: &[IocRowOutcome],
) -> Vec<String> {
    let mut user_flag_vec: Vec<String> = vec![String::new(); height];
    for (idx, entry) in flags.iter() {
        if *idx < height {
            user_flag_vec[*idx] = normalize_flag_value(&entry.flag);
        }
    }
    (0..height)
        .map(|i| {
            if !user_flag_vec[i].is_empty() {
                user_flag_vec[i].clone()
            } else {
                ioc_outcomes[i].flag.clone()
            }
        })
        .collect()
}

/// Appends matched `[tag]` tokens that are not already present in the memo.
pub fn append_ioc_tags(memo: &mut String, tags: &[String]) {
    for tag in tags {
        if !memo.contains(tag) {
            if !memo.is_empty() && !memo.ends_with(' ') {
                memo.push(' ');
            }
            memo.push_str(tag);
        }
    }
}

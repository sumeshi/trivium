use std::collections::HashMap;

use crate::flags::normalize_flag_value;
use crate::models::FlagEntry;

/// Per-row flag from user annotations only.
///
/// Automatic IOC flags are intentionally not merged here: IOC evaluation is
/// not part of the normal query, export, bulk, or analysis paths. The IOC
/// evaluation module is kept for an explicit future operation.
pub fn user_flags(height: usize, flags: &HashMap<usize, FlagEntry>) -> Vec<String> {
    let mut user_flag_vec: Vec<String> = vec![String::new(); height];
    for (idx, entry) in flags.iter() {
        if *idx < height {
            user_flag_vec[*idx] = normalize_flag_value(&entry.flag);
        }
    }
    user_flag_vec
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

//! Column stats sent to the UI are bounded (T25).
//!
//! `rareMax` is a frequency threshold, not a row limit. A column where every
//! value is unique would otherwise send every value to the UI.

use polars::prelude::{NamedFrom, Series};

use trivium_engine::analysis::stats::summarize_series;

#[test]
fn top_and_rare_lists_are_capped() {
    let values: Vec<String> = (0..50).map(|index| format!("value-{index}")).collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    let series = Series::new("token", &refs);

    let stats = summarize_series("token", &series, 100);

    assert_eq!(stats.unique, 50);
    assert_eq!(stats.top.len(), 20);
    assert_eq!(stats.rare.len(), 20);
    assert!(
        stats.rare.iter().all(|item| item.count == 1),
        "the cap must keep rare values, not promote frequent ones"
    );
}

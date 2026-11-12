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

#[test]
fn bounded_ranking_retains_historical_count_then_lexical_order() {
    let mut counts = std::collections::HashMap::new();
    let mut values = Vec::new();
    for index in 0..100 {
        let value = format!("value-{index:03}");
        let count = index % 7 + 1;
        counts.insert(value.clone(), count as u64);
        for _ in 0..count {
            values.push(Some(value.clone()));
        }
    }
    values.extend([None, Some(String::new())]);
    let stats = summarize_series("token", &Series::new("token", values), 3);
    let mut expected = counts.into_iter().collect::<Vec<_>>();
    expected.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let actual = stats
        .top
        .iter()
        .map(|item| (item.value.clone(), item.count))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        expected.iter().take(20).cloned().collect::<Vec<_>>()
    );
    let actual = stats
        .rare
        .iter()
        .map(|item| (item.value.clone(), item.count))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        expected
            .into_iter()
            .filter(|item| item.1 <= 3)
            .take(20)
            .collect::<Vec<_>>()
    );
    assert_eq!(stats.missing, 2);
}

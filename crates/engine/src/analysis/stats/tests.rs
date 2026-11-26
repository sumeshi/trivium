use super::*;
use polars::prelude::{AnyValue, NamedFrom};

fn accumulator(distinct: usize, bytes: usize) -> StatsAccumulator {
    StatsAccumulator::with_limits("value", &DataType::String, StatsLimits { distinct, bytes })
}
fn observe(stats: &mut StatsAccumulator, values: &[Option<&str>]) -> Result<(), AppError> {
    let series = Series::new("value", values);
    stats.observe(&series, 0..series.len(), &JobContext::none())
}

#[test]
fn distinct_boundary_counts_repeats_and_missing_exactly_but_rejects_partial_results() {
    let mut stats = accumulator(2, 4096);
    observe(
        &mut stats,
        &[Some("a"), Some("b"), Some("a"), None, Some("")],
    )
    .unwrap();
    let result = stats.finish(1, &JobContext::none()).unwrap();
    assert_eq!(result.unique, 2);
    assert_eq!(result.missing, 2);
    assert_eq!(
        result.top,
        vec![
            ValueCount {
                value: "a".into(),
                count: 2
            },
            ValueCount {
                value: "b".into(),
                count: 1
            }
        ]
    );
    assert_eq!(
        result.rare,
        vec![ValueCount {
            value: "b".into(),
            count: 1
        }]
    );
    let mut stats = accumulator(2, 4096);
    let error = observe(&mut stats, &[Some("a"), Some("b"), Some("c")]).unwrap_err();
    assert!(error.to_string().contains("2 distinct values"));
    assert!(error.to_string().contains("Narrow the current filters"));
    assert!(
        stats.counts.is_empty(),
        "release the rejected partial table"
    );
    assert!(
        observe(&mut stats, &[Some("a")]).is_err(),
        "a failed accumulator stays failed"
    );
    assert!(
        stats.finish(1, &JobContext::none()).is_err(),
        "never present partial counts as exact"
    );
}

#[test]
fn terminal_string_classification_keeps_all_later_counts_and_budget_checks() {
    let mut stats = accumulator(4, 4096);
    observe(
        &mut stats,
        &[
            Some("ordinary text"),
            Some("7"),
            Some("1.5"),
            Some("2026-11-14T00:00:00Z"),
            Some("7"),
            None,
            Some(""),
        ],
    )
    .unwrap();
    let result = stats.finish(1, &JobContext::none()).unwrap();
    assert_eq!(result.kind, "string");
    assert_eq!((result.unique, result.missing), (4, 2));
    assert_eq!(result.top.iter().map(|value| value.count).sum::<u64>(), 5);
    assert_eq!(result.top[0].value, "7");
    assert_eq!(result.top[0].count, 2);

    let mut distinct_limited = accumulator(1, 4096);
    observe(&mut distinct_limited, &[Some("ordinary text")]).unwrap();
    assert!(observe(&mut distinct_limited, &[Some("7")]).is_err());
    assert!(distinct_limited.counts.is_empty());

    let mut byte_limited = accumulator(10, 4096);
    observe(&mut byte_limited, &[Some("ordinary text")]).unwrap();
    let oversized = "7".repeat(4096);
    assert!(observe(&mut byte_limited, &[Some(&oversized)]).is_err());
    assert!(byte_limited.counts.is_empty());
}

#[test]
fn byte_boundary_includes_table_keys_and_output_copies_without_penalizing_repeats() {
    let mut probe = accumulator(100, usize::MAX);
    observe(&mut probe, &[Some("long repeated value")]).unwrap();
    let required = probe.accounted_bytes();
    assert!(required > "long repeated value".len() * 2 + std::mem::size_of::<StatsAccumulator>());
    let mut exact = accumulator(100, required);
    observe(
        &mut exact,
        &[Some("long repeated value"), Some("long repeated value")],
    )
    .unwrap();
    assert_eq!(exact.accounted_bytes(), required);
    assert_eq!(
        exact.finish(2, &JobContext::none()).unwrap().top[0].count,
        2
    );
    let mut short = accumulator(100, required - 1);
    assert!(observe(&mut short, &[Some("long repeated value")]).is_err());
    assert!(short.counts.is_empty());
    assert!(short.finish(1, &JobContext::none()).is_err());
}

#[test]
fn growing_table_is_budgeted_before_old_and_new_tables_coexist() {
    let mut probe = accumulator(100, usize::MAX);
    observe(&mut probe, &[Some("a"), Some("b"), Some("c"), Some("d")]).unwrap();
    let steady_state = probe.accounted_bytes();
    let mut stats = accumulator(100, steady_state);
    observe(&mut stats, &[Some("a"), Some("b"), Some("c")]).unwrap();
    assert!(
        observe(&mut stats, &[Some("d")]).is_err(),
        "the rehash peak exceeds the steady-state table"
    );
    assert!(stats.finish(1, &JobContext::none()).is_err());
}

#[test]
fn bounded_text_matches_scalar_and_list_search_text() {
    let list = Series::new("list", &[None, Some(""), Some("x"), None]);
    let values = [
        AnyValue::UInt64(u64::MAX),
        AnyValue::Float32(0.1),
        AnyValue::Float64(1.25),
        AnyValue::List(list),
    ];
    for value in values {
        let expected = crate::value_utils::anyvalue_to_search_string(&value);
        let actual = text::present_text(&value, 128)
            .unwrap()
            .map(|value| value.into_owned());
        assert_eq!(actual, expected);
    }
    let list = AnyValue::List(Series::new("list", &[Some("ab"), None, Some("cd")]));
    assert_eq!(text::present_text(&list, 5).unwrap().unwrap(), "ab,cd");
    assert!(text::present_text(&list, 4).is_err());
    let trailing_null = AnyValue::List(Series::new("list", &[Some("ab"), None]));
    assert_eq!(
        text::present_text(&trailing_null, 2).unwrap().unwrap(),
        "ab"
    );
}

#[test]
fn oversized_borrowed_value_is_rejected_before_key_allocation() {
    let mut stats = accumulator(10, 4096);
    let value = "x".repeat(4096);
    assert!(observe(&mut stats, &[Some(&value)]).is_err());
    assert_eq!(stats.counts.capacity(), 0);
    assert_eq!(stats.key_bytes, 0);
}

#[test]
fn formatted_numeric_repeats_fit_the_exact_budget_too() {
    let series = Series::new("value", &[17i64, 17]);
    let mut probe = StatsAccumulator::with_limits(
        "value",
        series.dtype(),
        StatsLimits {
            distinct: 2,
            bytes: usize::MAX,
        },
    );
    probe.observe(&series, 0..1, &JobContext::none()).unwrap();
    let mut stats = StatsAccumulator::with_limits(
        "value",
        series.dtype(),
        StatsLimits {
            distinct: 2,
            bytes: probe.accounted_bytes(),
        },
    );
    stats.observe(&series, 0..2, &JobContext::none()).unwrap();
    let result = stats.finish(2, &JobContext::none()).unwrap();
    assert_eq!(result.kind, "int");
    assert_eq!(result.top[0].count, 2);
    assert_eq!(result.top[0].value, "17");
}

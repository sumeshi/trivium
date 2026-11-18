use super::*;
use polars::prelude::NamedFrom;

#[test]
fn interval_budget_is_shared_and_never_returns_a_partial_analysis() {
    let series = Series::new("time", vec!["2026-11-18T00:00:00Z"; MAX_SPANS / 2 + 2]);
    let rows = (0..series.len()).collect::<Vec<_>>();
    // Each list fits separately, but their combined result would not.
    let error = analyze_series("time", &series, &rows, 0, 0, 60, 10).unwrap_err();
    assert!(error.contains("interval limit"), "{error}");
    let narrowed = analyze_series("time", &series, &rows[..3], 0, 0, 60, 10).unwrap();
    assert_eq!(narrowed.gaps.len(), 2);
    assert_eq!(narrowed.sessions.len(), 3);
    assert!(narrowed.bursts.is_empty());
}

#[test]
fn many_events_with_few_intervals_remain_analyzable() {
    let series = Series::new(
        "time",
        vec!["2026-11-18T00:00:00.123456789Z"; MAX_SPANS + 1],
    );
    let rows = (0..series.len()).collect::<Vec<_>>();
    let result = analyze_series("time", &series, &rows, 60, 30, 60, 10).unwrap();
    assert!(result.gaps.is_empty());
    assert_eq!(result.sessions.len(), 1);
    assert_eq!(result.bursts.len(), 1);
    assert_eq!(result.sessions[0].count, series.len() as u64);
    assert_eq!(result.bursts[0].count, series.len() as u64);
    assert_eq!(result.bursts[0].start, "2026-11-18T00:00:00.123456789Z");
}

#[test]
fn timestamp_labels_are_budgeted_before_cloning() {
    let mut budget = SpanBudget {
        count: 0,
        bytes: MAX_SPAN_BYTES - 4,
    };
    let mut spans = Vec::new();
    assert!(budget
        .push(&mut spans, "start", "end", 1)
        .unwrap_err()
        .contains("interval limit"));
    assert!(spans.is_empty());
    assert_eq!(budget.count, 0);
}

#[test]
fn burst_windows_cannot_build_an_unbounded_intermediate_list() {
    let job = crate::jobs::JobContext::none();
    let events = [
        Event {
            nanos: 0,
            text: "first".into(),
        },
        Event {
            nanos: 1,
            text: "second".into(),
        },
    ];
    let mut budget = SpanBudget {
        count: MAX_SPANS - 1,
        bytes: 0,
    };
    assert!(bursts(&events, 0, 0, &job, &mut budget)
        .unwrap_err()
        .contains("interval limit"));
}

use super::*;
use polars::prelude::NamedFrom;

#[test]
fn interval_budget_is_shared_and_never_returns_a_partial_analysis() {
    let series = Series::new(
        "time".into(),
        vec!["2026-11-18T00:00:00Z"; MAX_SPANS / 2 + 2],
    );
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
        "time".into(),
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
    let start = Event {
        nanos: 0,
        text: "start".into(),
    };
    let end = Event {
        nanos: 1,
        text: "end".into(),
    };
    assert!(budget
        .push(&mut spans, &start, &end, 1)
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

#[test]
fn intervals_publish_engine_normalized_bounds_and_exact_durations_with_original_labels() {
    for (values, nanos) in [
        (
            vec![
                "1969/12/31 23:59:59,999999999 UTC".to_string(),
                "Thu, 1 Jan 1970 09:00:00 +0900".to_string(),
                "1970.1.1t0:0:0.000000001z".to_string(),
            ],
            vec![-1_i64, 0, 1],
        ),
        (
            [i64::MIN, i64::MAX]
                .map(|nanos| chrono::DateTime::from_timestamp_nanos(nanos).to_rfc3339())
                .to_vec(),
            vec![i64::MIN, i64::MAX],
        ),
    ] {
        let series = Series::new("time".into(), &values);
        let rows = (0..values.len()).collect::<Vec<_>>();
        let result = analyze_series("time", &series, &rows, 0, u64::MAX, u64::MAX, 0).unwrap();
        assert_eq!(result.gaps.len(), values.len() - 1);
        assert_eq!(result.sessions.len(), 1);
        assert_eq!(result.bursts.len(), 1);
        for span in result
            .gaps
            .iter()
            .chain(&result.sessions)
            .chain(&result.bursts)
        {
            let first = values.iter().position(|text| text == &span.start).unwrap();
            let last = values.iter().position(|text| text == &span.end).unwrap();
            let wire = serde_json::to_value(span).unwrap();
            let canonical = |index| {
                chrono::DateTime::from_timestamp_nanos(nanos[index])
                    .to_rfc3339_opts(SecondsFormat::AutoSi, true)
            };
            assert_eq!(wire["start_utc"], canonical(first));
            assert_eq!(wire["end_utc"], canonical(last));
            assert_eq!(
                wire["duration_nanos"],
                (i128::from(nanos[last]) - i128::from(nanos[first])).to_string()
            );
        }
        for (row, text) in values.iter().enumerate() {
            assert_eq!(series.str().unwrap().get(row), Some(text.as_str()));
        }
    }
}

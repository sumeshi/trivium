use serde_json::json;
use trivium_engine::{
    annotation::AnnotationPatch,
    jobs::{JobInfo, JobKind},
    models::{Cell, UpdateFlagResponse},
    ops::rows::QuerySpec,
};

#[test]
fn omitted_and_null_patch_fields_preserve_their_distinct_meanings() {
    let missing: AnnotationPatch = serde_json::from_value(json!({})).unwrap();
    let cleared: AnnotationPatch =
        serde_json::from_value(json!({"memo":null,"bookmarked":false,"tags":[]})).unwrap();
    assert_eq!(missing.memo, None);
    assert_eq!(cleared.memo, Some(None));
    assert_eq!(cleared.bookmarked, Some(false));
    assert_eq!(cleared.tags, Some(vec![]));
    let query: QuerySpec = serde_json::from_value(
        json!({"condition":null,"searchColumns":[],"flagFilter":"safe|none"}),
    )
    .unwrap();
    assert_eq!(query.flag_filter.as_deref(), Some("safe|none"));
    assert_eq!(
        serde_json::to_value(query).unwrap()["searchColumns"],
        json!([])
    );
}

#[test]
fn counters_are_json_numbers_and_evidence_values_remain_exact_strings() {
    for text in ["18446744073709551615", "2026-11-11T00:00:00.123456789Z"] {
        let cell = Cell {
            text: Some(text.into()),
            kind: "string".into(),
            truncated: false,
            length: Some(text.chars().count()),
        };
        assert_eq!(serde_json::to_value(cell).unwrap()["text"], text);
    }
    let response = UpdateFlagResponse {
        entry: None,
        revision: 91,
        flagged_records: 2,
    };
    let value = serde_json::to_value(response).unwrap();
    assert_eq!(
        value,
        json!({"entry":null,"revision":91,"flaggedRecords":2})
    );
    let job = JobInfo {
        id: 3,
        kind: JobKind::Analysis,
        project_id: None,
        request_id: None,
    };
    assert_eq!(
        serde_json::to_value(job).unwrap(),
        json!({"id":3,"kind":"analysis","projectId":null})
    );
}

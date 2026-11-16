use super::{
    aggregate::Limits,
    cache, extract_values, release_extract_result, scan,
    tests::{serial, Case},
};
use crate::jobs::JobContext;

#[test]
fn rejected_cache_admission_does_not_discard_other_results() {
    let _lock = serial();
    let case = Case::csv();
    let first = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    let pinned = cache::get(&case.store, first.result_id).unwrap();
    let second = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    let mut large = scan::build(
        &case.store,
        case.payload(),
        Limits::default(),
        &JobContext::none(),
    )
    .unwrap();
    large.bytes = cache::MAX_CACHE_BYTES;
    assert!(cache::insert(large)
        .unwrap_err()
        .to_string()
        .contains("in use"));
    assert_eq!(case.values(second.result_id).len(), 3);
    drop(pinned);
    release_extract_result(first.result_id);
    release_extract_result(second.result_id);
}

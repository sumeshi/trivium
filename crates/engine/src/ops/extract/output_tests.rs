use std::{fs, sync::Arc};

use super::{
    aggregate::Limits,
    cache, export_prepared_extract, extract_values, prepare_extract_export, release_extract_result,
    scan,
    tests::{serial, Case},
    ExportExtractPayload,
};
use crate::jobs::JobContext;

#[test]
fn accepted_export_keeps_a_released_result_alive_while_queued_and_reclaims_it_afterwards() {
    let _serial = serial();
    let case = Case::csv();
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    let snapshot = cache::get(&case.store, result.result_id).unwrap();
    let lifetime = Arc::downgrade(&snapshot);
    drop(snapshot);
    let destination = case.output_path("queued-extract.csv");
    let prepared = prepare_extract_export(
        &case.store,
        ExportExtractPayload {
            result_id: result.result_id,
            destination: destination.to_string_lossy().into(),
            allow_overwrite: false,
        },
    )
    .unwrap();
    assert_eq!(prepared.project_id(), result.project_id);

    // The worker has not started. Closing the tab releases access for new
    // requests, while this accepted work still owns the same accounted data.
    assert!(release_extract_result(result.result_id));
    assert!(cache::get(&case.store, result.result_id).is_err());
    assert!(lifetime.upgrade().is_some());
    assert!(!destination.exists());
    let mut cache_filling_result = scan::build(
        &case.store,
        case.payload(),
        Limits::default(),
        &JobContext::none(),
    )
    .unwrap();
    cache_filling_result.bytes = cache::MAX_CACHE_BYTES;
    assert!(cache::insert(cache_filling_result)
        .unwrap_err()
        .to_string()
        .contains("in use"));

    let output = export_prepared_extract(&case.store, prepared, &JobContext::none()).unwrap();
    assert_eq!(output.rows, 3);
    let mut csv = csv::Reader::from_path(destination).unwrap();
    let rows = csv.records().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (&row[0], &row[1], &row[2], &row[3]))
            .collect::<Vec<_>>(),
        vec![
            ("10.0.0.1", "3", "1", "Private"),
            ("8.8.8.8", "2", "2", "Public"),
            ("127.0.0.1", "1", "1", "Special")
        ]
    );
    assert!(
        lifetime.upgrade().is_none(),
        "a closed result is reclaimed when the accepted export finishes"
    );
}

#[test]
fn abandoning_queued_export_reclaims_its_released_result_without_creating_output() {
    let _serial = serial();
    let case = Case::csv();
    let result = extract_values(&case.store, case.payload(), &JobContext::none()).unwrap();
    let snapshot = cache::get(&case.store, result.result_id).unwrap();
    let lifetime = Arc::downgrade(&snapshot);
    drop(snapshot);
    let destination = case.output_path("cancelled-extract.csv");
    let prepared = prepare_extract_export(
        &case.store,
        ExportExtractPayload {
            result_id: result.result_id,
            destination: destination.to_string_lossy().into(),
            allow_overwrite: false,
        },
    )
    .unwrap();
    release_extract_result(result.result_id);
    assert!(lifetime.upgrade().is_some());

    // Queue cancellation/admission failure drops the captured work without
    // entering its blocking worker. No temporary CSV or retained cache remains.
    drop(prepared);
    assert!(lifetime.upgrade().is_none());
    assert!(!destination.exists());
    assert!(
        prepare_extract_export(
            &case.store,
            ExportExtractPayload {
                result_id: result.result_id,
                destination: case.output_path("later.csv").to_string_lossy().into(),
                allow_overwrite: false,
            }
        )
        .is_err(),
        "closing still prevents new exports of that result"
    );
    assert_eq!(
        fs::read_dir(case.output_path(""))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with(".trivium-export-"))
            .count(),
        0
    );
}

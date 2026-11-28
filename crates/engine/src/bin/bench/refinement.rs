//! Compare fresh membership with an appended rule over cached search results.

use trivium_engine::{
    condition::{Condition, Node, Op},
    error::AppError,
    models::ProjectMeta,
    ops::rows::{clear_result_cache, locate_project_row, LocateRowPayload, QuerySpec},
    project_io::{project_read_stats, reset_project_read_counters},
    store::ProjectsStore,
};

use super::support::{out, Samples, ITERATIONS};

pub fn measure(store: &ProjectsStore, meta: &ProjectMeta) -> Result<(), AppError> {
    let user = meta
        .columns
        .iter()
        .find(|column| column.source == "user" || column.source == "/user")
        .ok_or_else(|| AppError::Message("refinement fixture has no user column".into()))?;
    // Generator fixtures place powershell in proc/msg; keep the ordinary all-column scope.
    let base = QuerySpec {
        search: Some("powershell".into()),
        ..Default::default()
    };
    let refined = QuerySpec {
        condition: Some(Node::Condition(Condition {
            column: user.id.clone(),
            op: Op::Eq,
            values: vec!["alice".into()],
            negated: false,
        })),
        ..base.clone()
    };
    let locate = |spec: &QuerySpec| {
        locate_project_row(
            store,
            LocateRowPayload {
                project_id: meta.id,
                row_index: 0,
                spec: spec.clone(),
            },
        )
    };
    out(
        "refinement_mode",
        "cold=empty result cache per trial; base search outside timing; OS cache retained".into(),
    );
    let mut cold = Samples::new("refinement_cold_locate");
    let mut cached = Samples::new("refinement_cached_locate");
    let (mut cold_bytes, mut cached_bytes, mut count, mut base_count) = (0, 0, 0, 0);
    for _ in 0..ITERATIONS {
        clear_result_cache();
        reset_project_read_counters();
        let fresh = cold.record(|| locate(&refined))?;
        cold_bytes += project_read_stats().column_bytes;

        // Remove the exact result; preparing the search-only base is outside timing.
        clear_result_cache();
        base_count = locate(&base)?.total_filtered_rows;
        reset_project_read_counters();
        let narrowed = cached.record(|| locate(&refined))?;
        cached_bytes += project_read_stats().column_bytes;
        assert_eq!(
            fresh, narrowed,
            "cached refinement changed membership/counts"
        );
        count = narrowed.total_filtered_rows;
    }
    cold.report();
    cached.report();
    out(
        "refinement_check",
        format!(
            "base_rows={base_count} filtered_rows={count} cold_column_bytes/trial={} refined_column_bytes/trial={}",
            cold_bytes / ITERATIONS as u64,
            cached_bytes / ITERATIONS as u64,
        ),
    );
    clear_result_cache();
    Ok(())
}

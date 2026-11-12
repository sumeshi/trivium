use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use trivium_engine::{error::AppError, jobs::JobContext, ops, store::ProjectsStore};

use super::protocol::Failure;

const METHODS: &[&str] = &[
    "hello",
    "import",
    "list",
    "load",
    "query",
    "locate",
    "cell",
    "update_flag",
    "bulk_preview",
    "bulk_apply",
    "bulk_undo",
    "export",
    "load_view",
    "save_view",
    "set_hidden_columns",
    "stats",
    "histogram",
    "time_analysis",
    "group_tree",
    "surround",
    "dupes",
    "diff",
    "report",
];

fn encode(result: impl Serialize) -> Result<Value, Failure> {
    serde_json::to_value(result).map_err(|error| Failure::new("encoding_failed", error.to_string()))
}

fn operation<P: DeserializeOwned, R: Serialize>(
    store: &ProjectsStore,
    params: Value,
    call: impl FnOnce(&ProjectsStore, P) -> Result<R, AppError>,
) -> Result<Value, Failure> {
    let payload = serde_json::from_value(params)
        .map_err(|error| Failure::new("invalid_params", error.to_string()))?;
    let result = call(store, payload)
        .map_err(|error| Failure::new("operation_failed", error.to_string()))?;
    encode(result)
}

pub(super) fn dispatch(
    store: &ProjectsStore,
    method: &str,
    params: Value,
) -> Result<Value, Failure> {
    match method {
        "hello" => Ok(
            json!({"protocolVersion":1,"engineVersion":env!("CARGO_PKG_VERSION"),"methods":METHODS}),
        ),
        "list" => {
            let result = ops::projects::list_projects(store)
                .map_err(|error| Failure::new("operation_failed", error.to_string()))?;
            encode(result)
        }
        "import" => operation(store, params, ops::projects::create_project),
        "load" => operation(store, params, |store, payload| {
            ops::projects::load_project_metadata_with_job(store, payload, &JobContext::none())
        }),
        "query" => operation(store, params, ops::rows::query_project_rows),
        "locate" => operation(store, params, ops::rows::locate_project_row),
        "cell" => operation(store, params, ops::cell::get_cell),
        "update_flag" => operation(store, params, ops::flags::update_flag),
        "bulk_preview" => operation(store, params, ops::bulk::bulk_preview),
        "bulk_apply" => operation(store, params, ops::bulk::bulk_update),
        "bulk_undo" => operation(store, params, ops::bulk::bulk_undo),
        "export" => operation(store, params, ops::export::export_project),
        "load_view" => operation(store, params, ops::view::load_view_state),
        "save_view" => operation(store, params, ops::view::save_view_state),
        "set_hidden_columns" => operation(store, params, ops::flags::set_hidden_columns),
        "stats" => operation(store, params, ops::stats::column_stats),
        "histogram" => operation(store, params, ops::histogram::histogram),
        "time_analysis" => operation(store, params, ops::time::time_analysis),
        "group_tree" => operation(store, params, ops::group::group_tree),
        "surround" => operation(store, params, ops::surround::surrounding_rows),
        "dupes" => operation(store, params, ops::dupes::duplicate_rows),
        "diff" => operation(store, params, ops::diff::diff_projects),
        "report" => operation(store, params, ops::report::build_report),
        _ => Err(Failure::new(
            "unknown_method",
            format!("Unknown method: {method}"),
        )),
    }
}

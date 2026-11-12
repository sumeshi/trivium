// Shim for replaying the unchanged common benchmark source on 95ee777.
use anyhow::Result;
use serde_json::{json, Value};
use trivium_engine::{
    ops::projects::{load_project, ProjectRequest},
    project_io::{project_full_reads, project_page_reads, reset_project_read_counters},
    store::ProjectsStore,
};
use uuid::Uuid;
pub fn kind() -> &'static str {
    "legacy API counters only (no actual RG instrumentation)"
}
pub fn reset() {
    reset_project_read_counters();
}
pub fn snapshot() -> Value {
    json!({"full_reads":project_full_reads(), "page_reads":project_page_reads(), "actual":null})
}
pub fn metadata_open(store: &ProjectsStore, id: Uuid) -> Result<()> {
    // This is the old app-open path; there is no metadata-only API.
    load_project(store, ProjectRequest { project_id: id })?;
    Ok(())
}

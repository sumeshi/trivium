use anyhow::Result;
use serde_json::{json, Value};
use trivium_engine::{
    jobs::JobContext,
    ops::projects::{load_project_with_job, ProjectRequest},
    project_io::{
        project_full_reads, project_page_reads, project_read_stats, reset_project_read_counters,
    },
    store::ProjectsStore,
};
use uuid::Uuid;

pub fn kind() -> &'static str {
    "bounded row-group bytes and decoded rows"
}
pub fn reset() {
    reset_project_read_counters();
}
pub fn snapshot() -> Value {
    json!({"full_reads":project_full_reads(), "page_reads":project_page_reads(), "actual":project_read_stats()})
}
pub fn metadata_open(store: &ProjectsStore, id: Uuid) -> Result<()> {
    load_project_with_job(
        store,
        ProjectRequest { project_id: id },
        &JobContext::none(),
    )?;
    Ok(())
}

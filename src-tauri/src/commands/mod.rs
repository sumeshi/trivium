mod bulk;
mod cell;
mod cross;
mod diff;
mod dupes;
mod export;
mod group;
mod histogram;
mod flags;
mod iocs;
mod job;
mod preset;
mod projects;
mod report;
mod rows;
mod rules;
mod stats;
mod surround;
mod time;
mod view;

// Public command functions, re-exported for `generate_handler!` in main.rs
// (which uses bare identifiers). The hidden `__cmd__*` / `__tauri_command_name_*`
// macros are `#[macro_export]`ed by the command macro at the crate root and are
// resolved textually by the handler — they must NOT be re-exported here:
// path-based re-exports of macro-expanded `#[macro_export]` macros are
// rejected by rustc (rust-lang/rust#52234).
pub use bulk::{bulk_preview, bulk_undo, bulk_update};
pub use cell::get_cell;
pub use cross::search_projects;
pub use diff::diff_projects;
pub use dupes::duplicate_rows;
pub use export::export_project;
pub use group::group_tree;
pub use histogram::histogram;
pub use flags::{set_hidden_columns, update_flag};
pub use iocs::{export_iocs, import_iocs, save_iocs};
pub use job::{active_jobs, cancel_job};
pub use preset::{load_preset, save_preset};
pub use projects::{create_project, delete_project, list_projects, load_project};
pub use report::build_report;
pub use rows::query_project_rows;
pub use rules::{color_rules, save_color_rules};
pub use stats::column_stats;
pub use surround::surrounding_rows;
pub use time::time_analysis;
pub use view::{load_view_state, save_view_state};

mod bulk;
mod cross;
mod diff;
mod dupes;
mod export;
mod group;
mod histogram;
mod flags;
mod iocs;
mod preset;
mod projects;
mod report;
mod rows;
mod rules;
mod stats;
mod surround;
mod time;

// v2: the `#[tauri::command]` macro emits `#[macro_export]` hidden macros,
// which live at the crate root — so they are re-exported from `crate::`.
// Public command functions are re-exported from their defining modules.
pub use crate::{
    __cmd__build_report, __cmd__bulk_preview, __cmd__bulk_undo, __cmd__bulk_update,
    __cmd__color_rules, __cmd__create_project, __cmd__delete_project, __cmd__diff_projects,
    __cmd__duplicate_rows, __cmd__export_iocs, __cmd__export_project, __cmd__group_tree,
    __cmd__histogram, __cmd__import_iocs, __cmd__list_projects, __cmd__load_preset,
    __cmd__load_project, __cmd__query_project_rows, __cmd__save_color_rules, __cmd__save_iocs,
    __cmd__save_preset, __cmd__search_projects, __cmd__set_hidden_columns,
    __cmd__column_stats, __cmd__surrounding_rows, __cmd__time_analysis, __cmd__update_flag,
    __tauri_command_name_build_report, __tauri_command_name_bulk_preview,
    __tauri_command_name_bulk_undo, __tauri_command_name_bulk_update,
    __tauri_command_name_color_rules, __tauri_command_name_column_stats,
    __tauri_command_name_create_project, __tauri_command_name_delete_project,
    __tauri_command_name_diff_projects, __tauri_command_name_duplicate_rows,
    __tauri_command_name_export_iocs, __tauri_command_name_export_project,
    __tauri_command_name_group_tree, __tauri_command_name_histogram,
    __tauri_command_name_import_iocs, __tauri_command_name_list_projects,
    __tauri_command_name_load_preset, __tauri_command_name_load_project,
    __tauri_command_name_query_project_rows, __tauri_command_name_save_color_rules,
    __tauri_command_name_save_iocs, __tauri_command_name_save_preset,
    __tauri_command_name_search_projects, __tauri_command_name_set_hidden_columns,
    __tauri_command_name_surrounding_rows, __tauri_command_name_time_analysis,
    __tauri_command_name_update_flag,
};

pub use bulk::{bulk_preview, bulk_undo, bulk_update};
pub use cross::search_projects;
pub use diff::diff_projects;
pub use dupes::duplicate_rows;
pub use export::export_project;
pub use group::group_tree;
pub use histogram::histogram;
pub use flags::{set_hidden_columns, update_flag};
pub use iocs::{export_iocs, import_iocs, save_iocs};
pub use preset::{load_preset, save_preset};
pub use projects::{create_project, delete_project, list_projects, load_project};
pub use report::build_report;
pub use rows::query_project_rows;
pub use rules::{color_rules, save_color_rules};
pub use stats::column_stats;
pub use surround::surrounding_rows;
pub use time::time_analysis;

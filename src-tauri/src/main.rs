#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows release builds

mod commands;
mod state;

// Re-exported command functions, used by `generate_handler!` via bare
// identifiers: the v2 command macro emits its hidden `__cmd__*` /
// `__tauri_command_name_*` macros with `#[macro_export]`, which places them
// at the crate root — so the handler macros resolve textually here, and a
// module-qualified path (`commands::...`) cannot find them.
use commands::*;

use tauri::Manager;

use crate::state::AppState;

fn main() {
    tauri::Builder::new()
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let state = AppState::new(app)?;
            app.manage(state);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_projects,
            create_project,
            delete_project,
            load_project,
            query_project_rows,
            locate_project_row,
            get_cell,
            cancel_job,
            active_jobs,
            save_iocs,
            import_iocs,
            export_iocs,
            update_flag,
            set_hidden_columns,
            export_project,
            column_stats,
            duplicate_rows,
            search_projects,
            diff_projects,
            histogram,
            group_tree,
            time_analysis,
            surrounding_rows,
            build_report,
            bulk_preview,
            bulk_update,
            bulk_undo,
            save_color_rules,
            color_rules,
            save_preset,
            load_preset,
            load_view_state,
            save_view_state
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows release builds

mod commands;
mod state;

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
            commands::list_projects,
            commands::create_project,
            commands::delete_project,
            commands::load_project,
            commands::query_project_rows,
            commands::save_iocs,
            commands::import_iocs,
            commands::export_iocs,
            commands::update_flag,
            commands::set_hidden_columns,
            commands::export_project,
            commands::column_stats,
            commands::duplicate_rows,
            commands::search_projects,
            commands::diff_projects,
            commands::histogram,
            commands::group_tree,
            commands::time_analysis,
            commands::surrounding_rows,
            commands::build_report,
            commands::bulk_preview,
            commands::bulk_update,
            commands::bulk_undo,
            commands::save_color_rules,
            commands::color_rules,
            commands::save_preset,
            commands::load_preset
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

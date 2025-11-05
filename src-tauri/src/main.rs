#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows release builds

mod commands;
mod error;
mod flags;
mod ioc;
mod models;
mod project_io;
mod search;
mod state;
mod storage;
mod value_utils;

use tauri::Manager;

use crate::state::AppState;

fn main() {
    tauri::Builder::new()
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
            commands::export_project
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

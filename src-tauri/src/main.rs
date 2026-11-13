#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows release builds

mod commands;
#[cfg(any(all(target_os = "windows", debug_assertions), test))]
mod native_test;
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
    #[cfg(all(target_os = "windows", debug_assertions))]
    automation_phase("before-builder");
    let context = tauri::generate_context!();
    #[cfg(all(target_os = "windows", debug_assertions))]
    let (context, native_window) =
        native_test::prepare(context).expect("invalid Windows native automation configuration");
    tauri::Builder::new()
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .setup(move |app| {
            #[cfg(all(target_os = "windows", debug_assertions))]
            automation_phase("setup-start");
            let state = AppState::new(app)?;
            #[cfg(all(target_os = "windows", debug_assertions))]
            automation_phase("state-ready");
            app.manage(state);
            #[cfg(all(target_os = "windows", debug_assertions))]
            if let Some(window) = native_window {
                window.create(app)?;
                automation_phase("window-ready");
            }
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
            cancel_request_jobs,
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
            validate_condition_values,
            load_view_state,
            save_view_state
        ])
        .run(context)
        .expect("error while running tauri application");
}

// Fixed phase names only; no case paths, values or environment contents.
#[cfg(all(target_os = "windows", debug_assertions))]
fn automation_phase(phase: &'static str) {
    if std::env::var("TAURI_WEBVIEW_AUTOMATION").as_deref() == Ok("true") {
        eprintln!("[trivium-native] {phase}");
    }
}

//! Invalid IOC severities must never silently become Safe or replace rules.

use std::{fs, path::PathBuf};

use trivium_engine::{
    ioc::{load_ioc_entries, read_ioc_csv, save_ioc_entries},
    models::IocEntry,
    ops::{
        iocs::{
            export_iocs, import_iocs, save_iocs, ExportIocsPayload, ImportIocsPayload,
            SaveIocsPayload,
        },
        projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
    },
    storage::{load_setting_bytes, save_setting_bytes},
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

fn rule(flag: &str) -> IocEntry {
    IocEntry {
        flag: flag.into(),
        tag: "review".into(),
        query: "powershell".into(),
        cell: None,
    }
}

fn setup() -> (PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-ioc-validation-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("source.csv");
    fs::write(&source, "command\npowershell\nnotepad\n").unwrap();
    let store = ProjectsStore::new(root.clone()).unwrap();
    let summary = create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    (root, store, summary.meta.id)
}

#[test]
fn csv_rejects_invalid_flags_with_the_physical_line_and_original_value() {
    let root = std::env::temp_dir().join(format!("trivium-ioc-csv-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let path = root.join("rules.csv");
    for (value, query) in [
        ("critcal", "powershell"),
        ("", "powershell"),
        ("unknown", ""),
        ("SAFE", "powershell"),
        (" Safe ", "powershell"),
        ("◯", "powershell"),
        ("?", "powershell"),
        ("✗", "powershell"),
        ("Suspicious", "powershell"),
    ] {
        // The tag in the valid preceding record spans two physical lines.
        fs::write(
            &path,
            format!("flag,tag,query,column,value\nsafe,\"multi\nline\",notepad,,\n{value},bad,{query},,\n"),
        )
        .unwrap();
        let error = read_ioc_csv(&path).unwrap_err().to_string();
        assert!(error.contains("IOC CSV line 4"), "{error}");
        assert!(error.contains(&format!("{value:?}")), "{error}");
    }
    fs::write(
        &path,
        "flag,tag,query,column,value\nsafe,a,one,,\nsuspicious,b,two,,\ncritical,c,three,,\n",
    )
    .unwrap();
    assert_eq!(
        read_ioc_csv(&path)
            .unwrap()
            .iter()
            .map(|entry| entry.flag.as_str())
            .collect::<Vec<_>>(),
        ["safe", "suspicious", "critical"]
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn csv_invalid_flag_locations_match_physical_lines_across_line_endings_and_blank_lines() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("invalid-ioc.csv");
    let mut failures = Vec::new();
    for newline in ["\n", "\r\n", "\r"] {
        for (prefix, expected_line) in [
            (String::new(), 2),
            (format!("safe,valid,notepad,,{newline}"), 3),
            (format!("safe,\"multi{newline}line\",notepad,,{newline}"), 4),
            (newline.into(), 3),
            (format!("safe,valid,notepad,,{newline}{newline}"), 4),
            (
                format!("{newline}safe,\"multi{newline}line\",notepad,,{newline}{newline}"),
                6,
            ),
        ] {
            fs::write(
                &path,
                format!("flag,tag,query,column,value{newline}{prefix}critcal,bad,powershell,,{newline}"),
            )
            .unwrap();
            let error = read_ioc_csv(&path).unwrap_err().to_string();
            assert!(error.contains("\"critcal\""), "{error}");
            if !error.contains(&format!("IOC CSV line {expected_line}:")) {
                failures.push(format!(
                    "newline={newline:?}, expected line {expected_line}: {error}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn crlf_csv_preserves_literal_cell_newlines_without_normalizing_source_values() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("literal-ioc.csv");
    let literal = "  first\r\nsecond\ronly\nlast, \"quoted\"  ";
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_path(&path)
        .unwrap();
    writer
        .write_record(["flag", "tag", "query", "column", "value"])
        .unwrap();
    writer
        .write_record(["critical", "review", "", "message", literal])
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    let entries = read_ioc_csv(&path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].flag, "critical");
    assert_eq!(entries[0].cell.as_ref().unwrap().column, "message");
    assert_eq!(entries[0].cell.as_ref().unwrap().value, literal);
}

#[test]
fn failed_import_and_save_preserve_the_complete_existing_rule_set() {
    let (root, store, id) = setup();
    let project_dir = store.project_dir(&id);
    save_ioc_entries(&project_dir, &[rule("critical")]).unwrap();
    let before = load_setting_bytes(&project_dir, "iocs").unwrap().unwrap();
    let source = root.join("invalid.csv");
    fs::write(
        &source,
        "flag,tag,query,column,value\nsafe,valid,notepad,,\ncritcal,bad,powershell,,\n",
    )
    .unwrap();
    let error = import_iocs(
        &store,
        ImportIocsPayload {
            project_id: id,
            path: source.to_string_lossy().into(),
        },
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("line 3") && error.contains("critcal"),
        "{error}"
    );
    assert_eq!(
        load_setting_bytes(&project_dir, "iocs").unwrap().unwrap(),
        before
    );
    let error = save_iocs(
        &store,
        SaveIocsPayload {
            project_id: id,
            entries: vec![rule("safe"), rule("critcal")],
        },
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("rule 2") && error.contains("critcal"),
        "{error}"
    );
    assert_eq!(
        load_setting_bytes(&project_dir, "iocs").unwrap().unwrap(),
        before
    );
    assert!(save_ioc_entries(&project_dir, &[rule("")]).is_err());
    assert_eq!(
        load_setting_bytes(&project_dir, "iocs").unwrap().unwrap(),
        before
    );
    assert_eq!(load_ioc_entries(&project_dir).unwrap()[0].flag, "critical");
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn corrupt_rules_remain_visible_without_blocking_the_case_or_becoming_safe() {
    let (root, store, id) = setup();
    let project_dir = store.project_dir(&id);
    let original = serde_json::to_vec(&vec![rule("critcal"), rule(" ◯ "), rule("")]).unwrap();
    save_setting_bytes(&project_dir, "iocs", &original).unwrap();
    let loaded = load_project(&store, ProjectRequest { project_id: id }).unwrap();
    assert_eq!(
        loaded
            .iocs
            .iter()
            .map(|entry| entry.flag.as_str())
            .collect::<Vec<_>>(),
        ["critcal", " ◯ ", ""]
    );
    assert!(common::rows(&store, id)
        .iter()
        .all(|row| row.flag.is_empty()));
    assert_eq!(loaded.project.meta.flagged_records, 0);
    assert!(save_iocs(
        &store,
        SaveIocsPayload {
            project_id: id,
            entries: loaded.iocs
        }
    )
    .is_err());
    assert_eq!(
        load_setting_bytes(&project_dir, "iocs").unwrap().unwrap(),
        original
    );

    // Export validation also occurs before replacing an existing destination.
    let destination = root.join("rules.csv");
    fs::write(&destination, "existing export").unwrap();
    let error = export_iocs(
        &store,
        ExportIocsPayload {
            allow_overwrite: false,
            project_id: id,
            destination: destination.to_string_lossy().into(),
            entries: None,
        },
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("critcal"), "{error}");
    assert_eq!(fs::read_to_string(destination).unwrap(), "existing export");
    assert_eq!(
        load_setting_bytes(&project_dir, "iocs").unwrap().unwrap(),
        original
    );
    fs::remove_dir_all(root).unwrap();
}

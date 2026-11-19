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
    ] {
        // The tag in the valid preceding record spans two physical lines.
        fs::write(
            &path,
            format!("flag,tag,query\nsafe,\"multi\nline\",notepad\n{value},bad,{query}\n"),
        )
        .unwrap();
        let error = read_ioc_csv(&path).unwrap_err().to_string();
        assert!(error.contains("IOC CSV line 4"), "{error}");
        assert!(error.contains(&format!("{value:?}")), "{error}");
    }
    fs::write(
        &path,
        "flag,tag,query\n SAFE ,a,one\n?,b,two\n✗,c,three\n◯,d,four\nSuspicious,e,five\n",
    )
    .unwrap();
    assert_eq!(
        read_ioc_csv(&path)
            .unwrap()
            .iter()
            .map(|entry| entry.flag.as_str())
            .collect::<Vec<_>>(),
        ["safe", "suspicious", "critical", "safe", "suspicious"]
    );
    fs::remove_dir_all(root).unwrap();
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
        "flag,tag,query\nsafe,valid,notepad\ncritcal,bad,powershell\n",
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
fn invalid_legacy_rules_remain_visible_without_blocking_the_case_or_becoming_safe() {
    let (root, store, id) = setup();
    let project_dir = store.project_dir(&id);
    let original = serde_json::to_vec(&vec![rule("◯"), rule("critcal"), rule("")]).unwrap();
    save_setting_bytes(&project_dir, "iocs", &original).unwrap();
    let loaded = load_project(&store, ProjectRequest { project_id: id }).unwrap();
    assert_eq!(
        loaded
            .iocs
            .iter()
            .map(|entry| entry.flag.as_str())
            .collect::<Vec<_>>(),
        ["safe", "critcal", ""]
    );
    assert!(loaded.initial_rows.iter().all(|row| row.flag.is_empty()));
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

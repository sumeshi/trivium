//! A nonempty expression without a predicate must never annotate every row.
use std::{fs, path::PathBuf};

use trivium_engine::{
    annotation::AnnotationPatch,
    ioc::{load_ioc_entries, save_ioc_entries},
    models::IocEntry,
    ops::{
        bulk::{bulk_preview, bulk_update, BulkAnnotatePayload},
        export::{export_project, ExportProjectPayload},
        flags::{update_flag, UpdateFlagPayload},
        iocs::{import_iocs, save_iocs, ImportIocsPayload, SaveIocsPayload},
        projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    search::validate_search_query,
    storage::{load_flags, load_setting_bytes, project_revision, save_setting_bytes},
    store::ProjectsStore,
};
use uuid::Uuid;

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
}

impl Fixture {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("trivium-invalid-ioc-query-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source.csv");
        fs::write(&source, "command\npowershell -enc\nnotepad\ncmd\n").unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let id = create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index: 1,
                flag: Some("safe".into()),
                memo: Some(Some("Sender: <alice@example.org>".into())),
                tags: Some(vec!["existing".into()]),
                bookmarked: Some(true),
            },
        )
        .unwrap();
        save_ioc_entries(&store.project_dir(&id), &[rule("notepad")]).unwrap();
        Self { root, store, id }
    }

    fn bulk(&self, query: &str) -> BulkAnnotatePayload {
        BulkAnnotatePayload {
            project_id: self.id,
            target: "filtered".into(),
            row_indices: None,
            spec: QuerySpec {
                search: Some(query.into()),
                ..Default::default()
            },
            patch: AnnotationPatch {
                flag: Some("critical".into()),
                add_tags: vec!["ioc".into()],
                ..Default::default()
            },
            preview_token: None,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn rule(query: &str) -> IocEntry {
    IocEntry {
        flag: "critical".into(),
        tag: "review".into(),
        query: query.into(),
        cell: None,
    }
}

#[test]
fn search_validation_distinguishes_blank_input_from_empty_or_incomplete_expressions() {
    for query in [
        "",
        " \t\n",
        "powershell",
        "command:powershell|notepad",
        "one|||two",
        "one | -two",
        "command:\"powershell -enc\"",
        "-\"powershell -enc\"",
        "command:notepad|-\"powershell -enc\"",
        "\"|\"",
        "\"||\"",
        "\" \"",
        "command:\" \"",
        "\"-\"",
        "\"host:\"",
        "-",
        "OR",
        "one \"\" two",
        "\"unterminated phrase",
    ] {
        assert!(validate_search_query(query).is_ok(), "{query:?}");
    }
    for query in [
        "|",
        "||",
        "\"\"",
        "| || \"\"",
        "command:",
        "-command:",
        "command:\"\"",
        "-\"\"",
        "one -\"\"",
        "command: -\"powershell -enc\"",
        "|powershell",
        "powershell|",
        "@tag:%GG",
    ] {
        assert!(validate_search_query(query).is_err(), "{query:?}");
    }
}

#[test]
fn invalid_ioc_queries_cannot_replace_saved_rules_or_mutate_annotations() {
    let f = Fixture::new();
    let dir = f.store.project_dir(&f.id);
    let saved = load_setting_bytes(&dir, "iocs").unwrap().unwrap();
    let flags = serde_json::to_value(load_flags(&f.store.flags_path(&f.id)).unwrap()).unwrap();
    let revision = project_revision(&dir).unwrap();
    for query in [
        "|",
        "||",
        "\"\"",
        "command:",
        "-command:",
        "powershell|",
        "|powershell",
    ] {
        let error = save_iocs(
            &f.store,
            SaveIocsPayload {
                project_id: f.id,
                entries: vec![rule("notepad"), rule(query)],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("IOC rule 2") && error.contains("Invalid search query"),
            "{error}"
        );

        let source = f.root.join("rules.csv");
        {
            let mut writer = csv::Writer::from_path(&source).unwrap();
            writer.write_record(["flag", "tag", "query"]).unwrap();
            writer
                .write_record(["safe", "multi\nline", "notepad"])
                .unwrap();
            writer.write_record(["critical", "invalid", query]).unwrap();
            writer.flush().unwrap();
        }
        let error = import_iocs(
            &f.store,
            ImportIocsPayload {
                project_id: f.id,
                path: source.to_string_lossy().into(),
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("IOC CSV line 4") && error.contains("Invalid search query"),
            "{error}"
        );
        assert_eq!(load_setting_bytes(&dir, "iocs").unwrap().unwrap(), saved);
        assert_eq!(load_ioc_entries(&dir).unwrap()[0].query, "notepad");

        let error = bulk_preview(&f.store, f.bulk(query))
            .unwrap_err()
            .to_string();
        assert!(error.contains("Invalid search query"), "{error}");
        // Even with a real earlier preview token, syntax must fail before a
        // transaction can resolve a new all-row target or write annotations.
        let preview = bulk_preview(&f.store, f.bulk("notepad")).unwrap();
        let mut apply = f.bulk(query);
        apply.preview_token = Some(preview.preview_token);
        let error = bulk_update(&f.store, apply).unwrap_err().to_string();
        assert!(error.contains("Invalid search query"), "{error}");
        assert_eq!(
            serde_json::to_value(load_flags(&f.store.flags_path(&f.id)).unwrap()).unwrap(),
            flags
        );
        assert_eq!(project_revision(&dir).unwrap(), revision);
    }
}

#[test]
fn invalid_legacy_queries_remain_editable_without_applying_or_blocking_case_load() {
    let f = Fixture::new();
    let dir = f.store.project_dir(&f.id);
    let rules: Vec<_> = ["|", "||", "\"\""].into_iter().map(rule).collect();
    let saved = serde_json::to_vec(&rules).unwrap();
    save_setting_bytes(&dir, "iocs", &saved).unwrap();
    let flags = serde_json::to_value(load_flags(&f.store.flags_path(&f.id)).unwrap()).unwrap();
    let loaded = load_project(&f.store, ProjectRequest { project_id: f.id }).unwrap();
    assert_eq!(
        loaded
            .iocs
            .iter()
            .map(|entry| entry.query.as_str())
            .collect::<Vec<_>>(),
        ["|", "||", "\"\""]
    );
    assert_eq!(loaded.project.meta.flagged_records, 1);
    assert_eq!(
        loaded
            .initial_rows
            .iter()
            .map(|row| row.flag.as_str())
            .collect::<Vec<_>>(),
        ["", "safe", ""]
    );
    assert!(save_iocs(
        &f.store,
        SaveIocsPayload {
            project_id: f.id,
            entries: loaded.iocs
        }
    )
    .is_err());
    assert_eq!(load_setting_bytes(&dir, "iocs").unwrap().unwrap(), saved);
    assert_eq!(
        serde_json::to_value(load_flags(&f.store.flags_path(&f.id)).unwrap()).unwrap(),
        flags
    );
}

#[test]
fn persisted_queries_and_exports_reject_nonempty_empty_filters_while_blank_search_keeps_all_rows() {
    let f = Fixture::new();
    for search in [None, Some(""), Some(" \t ")] {
        let response = query_project_rows(
            &f.store,
            QueryRowsPayload {
                project_id: f.id,
                spec: QuerySpec {
                    search: search.map(str::to_string),
                    ..Default::default()
                },
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        assert_eq!(response.total_filtered_rows, 3);
    }
    for (query, expected) in [
        ("\" \"", vec![0]),
        ("command:\" \"", vec![0]),
        ("\"|\"", vec![]),
        ("\"||\"", vec![]),
    ] {
        let response = query_project_rows(
            &f.store,
            QueryRowsPayload {
                project_id: f.id,
                spec: QuerySpec {
                    search: Some(query.into()),
                    ..Default::default()
                },
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        assert_eq!(
            response
                .rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>(),
            expected,
            "{query}"
        );
    }
    for query in ["|", "||", "\"\""] {
        let spec = QuerySpec {
            search: Some(query.into()),
            ..Default::default()
        };
        let error = query_project_rows(
            &f.store,
            QueryRowsPayload {
                project_id: f.id,
                spec: spec.clone(),
                offset: None,
                limit: None,
            },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("Invalid search query"), "{error}");
        let destination = f.root.join("export.csv");
        fs::write(&destination, "existing output").unwrap();
        assert!(export_project(
            &f.store,
            ExportProjectPayload {
                project_id: f.id,
                destination: destination.to_string_lossy().into(),
                scope: Some("filtered".into()),
                spec,
                columns: None,
                row_indices: None,
                include_annotations: Some(true),
                allow_overwrite: true,
            }
        )
        .is_err());
        assert_eq!(fs::read_to_string(destination).unwrap(), "existing output");
    }
}

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::{json, Value};
use trivium_engine::{
    error::AppError,
    ioc::{load_ioc_entries, read_ioc_csv},
    models::{IocCellMatch, IocEntry},
    ops::{
        flags::{update_flag, UpdateFlagPayload},
        iocs::{
            export_iocs, import_iocs, save_iocs, ExportIocsPayload, ImportIocsPayload,
            SaveIocsPayload,
        },
        projects::{create_project, CreateProjectPayload},
    },
    storage::{load_flags, load_setting_bytes, project_revision},
    store::ProjectsStore,
};
use uuid::Uuid;

fn query_rule(tag: &str) -> IocEntry {
    IocEntry {
        flag: "critical".into(),
        tag: tag.into(),
        query: "powershell".into(),
        cell: None,
    }
}

fn cell_rule(tag: &str, column: &str, value: &str) -> IocEntry {
    IocEntry {
        flag: "suspicious".into(),
        tag: tag.into(),
        query: String::new(),
        cell: Some(IocCellMatch {
            column: column.into(),
            value: value.into(),
        }),
    }
}

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    column: String,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-ioc-cell-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source.csv");
        let mut writer = csv::Writer::from_path(&source).unwrap();
        writer.write_record(["message"]).unwrap();
        for value in ["alpha|beta", "", "powershell"] {
            writer.write_record([value]).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);
        let store = ProjectsStore::new(root.join("workspace")).unwrap();
        let meta = create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta;
        let column = meta
            .columns
            .iter()
            .find(|column| column.source == "message")
            .unwrap()
            .id
            .clone();
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: meta.id,
                row_index: 2,
                flag: Some("safe".into()),
                memo: Some(Some("manual note".into())),
                tags: Some(vec!["human".into()]),
                bookmarked: Some(true),
            },
        )
        .unwrap();
        let fixture = Self {
            root,
            store,
            id: meta.id,
            column,
        };
        fixture.save(vec![query_rule("original")]).unwrap();
        fixture
    }

    fn save(&self, entries: Vec<IocEntry>) -> Result<(), AppError> {
        save_iocs(
            &self.store,
            SaveIocsPayload {
                project_id: self.id,
                entries,
            },
        )
    }

    fn import(&self, path: &Path) -> Result<Vec<IocEntry>, AppError> {
        import_iocs(
            &self.store,
            ImportIocsPayload {
                project_id: self.id,
                path: path.to_string_lossy().into(),
            },
        )
    }

    fn export(&self, path: &Path, entries: Option<Vec<IocEntry>>) -> Result<(), AppError> {
        export_iocs(
            &self.store,
            ExportIocsPayload {
                project_id: self.id,
                destination: path.to_string_lossy().into(),
                entries,
                allow_overwrite: true,
            },
        )
    }

    fn rules_bytes(&self) -> Vec<u8> {
        load_setting_bytes(&self.store.project_dir(&self.id), "iocs")
            .unwrap()
            .unwrap()
    }

    fn annotations(&self) -> (Value, u64) {
        (
            serde_json::to_value(load_flags(&self.store.flags_path(&self.id)).unwrap()).unwrap(),
            project_revision(&self.store.project_dir(&self.id)).unwrap(),
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn write_csv(path: &Path, headers: &[&str], rows: Vec<Vec<String>>) {
    let mut writer = csv::Writer::from_path(path).unwrap();
    writer.write_record(headers).unwrap();
    for row in rows {
        writer.write_record(row).unwrap();
    }
    writer.flush().unwrap();
}

fn cell_csv_row(entry: &IocEntry) -> Vec<String> {
    let cell = entry.cell.as_ref().unwrap();
    vec![
        entry.flag.clone(),
        entry.tag.clone(),
        entry.query.clone(),
        cell.column.clone(),
        cell.value.clone(),
    ]
}

#[test]
fn legacy_three_column_csv_and_json_definitions_remain_query_rules() {
    let f = Fixture::new();
    let before = f.annotations();
    let path = f.root.join("legacy.csv");
    // The original three headers remain positional, including a third header
    // named value. Optional cell headers are recognized only after these.
    write_csv(
        &path,
        &["severity", "label", "value"],
        vec![vec![
            "critical".into(),
            "legacy".into(),
            "powershell".into(),
        ]],
    );
    let imported = f.import(&path).unwrap();
    assert_eq!(imported.len(), 1);
    assert!(imported[0].cell.is_none());
    assert_eq!(imported[0].query, "powershell");
    let legacy: IocEntry =
        serde_json::from_value(json!({"flag":"critical", "tag":"legacy", "query":"powershell"}))
            .unwrap();
    assert!(legacy.cell.is_none());
    assert!(serde_json::to_value(&legacy).unwrap().get("cell").is_none());

    let output = f.root.join("legacy-export.csv");
    f.export(&output, None).unwrap();
    let mut csv = csv::Reader::from_path(&output).unwrap();
    assert_eq!(
        csv.headers().unwrap(),
        &csv::StringRecord::from(vec!["flag", "tag", "query"])
    );
    assert_eq!(
        serde_json::to_value(read_ioc_csv(&output).unwrap()).unwrap(),
        serde_json::to_value(imported).unwrap()
    );
    assert_eq!(f.annotations(), before);
}

#[test]
fn cell_literals_survive_saved_rules_and_mixed_csv_roundtrips_without_auto_annotation() {
    let f = Fixture::new();
    let before = f.annotations();
    let entries = vec![
        query_rule("00-query"),
        cell_rule("01-empty", &f.column, ""),
        cell_rule("02-spaces", &f.column, " \t "),
        cell_rule(
            "03-literal",
            &f.column,
            "alpha|beta @tag:* <alice@example.org>, C:\\Windows\\System32\\cmd.exe \"quoted\"\r\n日本語  ",
        ),
    ];
    f.save(entries.clone()).unwrap();
    let expected = serde_json::to_value(&entries).unwrap();
    assert_eq!(
        serde_json::to_value(load_ioc_entries(&f.store.project_dir(&f.id)).unwrap()).unwrap(),
        expected
    );
    let output = f.root.join("mixed.csv");
    f.export(&output, None).unwrap();
    let mut csv = csv::Reader::from_path(&output).unwrap();
    assert_eq!(
        csv.headers().unwrap(),
        &csv::StringRecord::from(vec!["flag", "tag", "query", "column", "value"])
    );
    let mut csv_entries = entries.clone();
    for entry in &mut csv_entries {
        if let Some(cell) = &mut entry.cell {
            cell.column = "message".into();
        }
    }
    assert_eq!(
        serde_json::to_value(read_ioc_csv(&output).unwrap()).unwrap(),
        serde_json::to_value(csv_entries).unwrap()
    );
    assert_eq!(
        serde_json::to_value(f.import(&output).unwrap()).unwrap(),
        expected
    );

    let reordered = f.root.join("optional-header-order.csv");
    let rows = entries
        .iter()
        .map(|entry| {
            let (value, column) = entry
                .cell
                .as_ref()
                .map(|cell| (cell.value.clone(), "message".to_string()))
                .unwrap_or_default();
            vec![
                entry.flag.clone(),
                entry.tag.clone(),
                entry.query.clone(),
                value,
                column,
            ]
        })
        .collect();
    write_csv(
        &reordered,
        &["flag", "tag", "query", " VALUE ", " CoLuMn "],
        rows,
    );
    assert_eq!(
        serde_json::to_value(f.import(&reordered).unwrap()).unwrap(),
        expected
    );
    assert_eq!(
        f.annotations(),
        before,
        "definitions must not apply flags, notes, tags, bookmarks, or annotation revisions"
    );
    let meta = f.store.try_find(&f.id).unwrap().unwrap();
    assert_eq!(meta.flagged_records, 1);
    assert_eq!(meta.ioc_applied_records, 0);
}

#[test]
fn invalid_cell_definitions_preserve_saved_rules_annotations_and_existing_output() {
    let f = Fixture::new();
    let saved = f.rules_bytes();
    let annotations = f.annotations();
    let output = f.root.join("existing.csv");
    let source = f.root.join("invalid.csv");
    fs::write(&output, b"existing export\n").unwrap();
    let mut ambiguous = cell_rule("both", &f.column, "literal");
    ambiguous.query = "powershell".into();
    for invalid in [
        cell_rule("missing", "not-a-project-column", "literal"),
        cell_rule("blank", " \t ", "literal"),
        ambiguous,
    ] {
        assert!(f.save(vec![query_rule("valid"), invalid.clone()]).is_err());
        assert_eq!(f.rules_bytes(), saved);
        assert!(f.export(&output, Some(vec![invalid.clone()])).is_err());
        assert_eq!(fs::read(&output).unwrap(), b"existing export\n");
        write_csv(
            &source,
            &["flag", "tag", "query", "column", "value"],
            vec![cell_csv_row(&invalid)],
        );
        assert!(f.import(&source).is_err());
        assert_eq!(f.rules_bytes(), saved);
        assert_eq!(f.annotations(), annotations);
    }
}

#[test]
fn incomplete_or_duplicate_optional_csv_headers_do_not_replace_saved_rules() {
    let f = Fixture::new();
    let saved = f.rules_bytes();
    let source = f.root.join("invalid-headers.csv");
    for headers in [
        vec!["flag", "tag", "query", "column"],
        vec!["flag", "tag", "query", "value"],
        vec!["flag", "tag", "query", "column", "COLUMN", "value"],
    ] {
        let mut row = vec!["critical".into(), "valid-query".into(), "powershell".into()];
        row.resize(headers.len(), String::new());
        write_csv(&source, &headers, vec![row]);
        assert!(f.import(&source).is_err(), "{headers:?}");
        assert_eq!(f.rules_bytes(), saved);
    }
    write_csv(
        &source,
        &["flag", "tag", "query", "column", "value"],
        vec![vec![
            "critical".into(),
            "blank-cell".into(),
            String::new(),
            String::new(),
            String::new(),
        ]],
    );
    assert!(
        f.import(&source).is_err(),
        "a blank five-column cell row must not silently clear saved rules"
    );
    assert_eq!(f.rules_bytes(), saved);
}

#[test]
fn unrecognized_optional_cell_headers_do_not_silently_erase_saved_rules() {
    let f = Fixture::new();
    let before = f.rules_bytes();
    let annotations = f.annotations();
    let source = f.root.join("misspelled-cell-headers.csv");
    // A blank query must not erase rules; a valid query must not silently
    // discard the unrecognized literal criteria and become broader.
    for query in ["", "powershell"] {
        write_csv(
            &source,
            &["flag", "tag", "query", "colum", "valeu"],
            vec![vec![
                "critical".into(),
                "review".into(),
                query.into(),
                "message".into(),
                "powershell".into(),
            ]],
        );
        let error = f.import(&source).unwrap_err().to_string();
        assert!(
            error.contains("column 4") && error.contains("colum"),
            "{error}"
        );
        assert_eq!(f.rules_bytes(), before);
        assert_eq!(f.annotations(), annotations);
    }
}

#[test]
fn missing_legacy_csv_columns_do_not_silently_erase_saved_rules() {
    let f = Fixture::new();
    let before = f.rules_bytes();
    let annotations = f.annotations();
    let source = f.root.join("incomplete-query-headers.csv");
    for headers in [vec!["flag"], vec!["flag", "tag"]] {
        let mut row = vec!["critical".into()];
        row.resize(headers.len(), "review".into());
        write_csv(&source, &headers, vec![row]);
        let error = f.import(&source).unwrap_err().to_string();
        assert!(error.contains("three columns"), "{error}");
        assert_eq!(f.rules_bytes(), before);
        assert_eq!(f.annotations(), annotations);
    }
}

#[test]
fn cell_value_budget_counts_utf8_bytes_and_preserves_the_last_valid_rules() {
    let f = Fixture::new();
    let allowed = format!("{}a", "界".repeat(21_845));
    assert_eq!(allowed.len(), 65_536);
    f.save(vec![cell_rule("boundary", &f.column, &allowed)])
        .unwrap();
    assert_eq!(
        load_ioc_entries(&f.store.project_dir(&f.id)).unwrap()[0]
            .cell
            .as_ref()
            .unwrap()
            .value,
        allowed
    );
    let saved = f.rules_bytes();
    let invalid = cell_rule("over-budget", &f.column, &"界".repeat(21_846));
    assert!(f.save(vec![invalid.clone()]).is_err());
    let source = f.root.join("oversized.csv");
    write_csv(
        &source,
        &["flag", "tag", "query", "column", "value"],
        vec![cell_csv_row(&invalid)],
    );
    assert!(f.import(&source).is_err());
    let output = f.root.join("existing.csv");
    fs::write(&output, b"existing export\n").unwrap();
    assert!(f.export(&output, Some(vec![invalid])).is_err());
    assert_eq!(fs::read(&output).unwrap(), b"existing export\n");
    assert_eq!(f.rules_bytes(), saved);
}

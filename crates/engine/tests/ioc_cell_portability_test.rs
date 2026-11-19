use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;
use trivium_engine::{
    error::AppError,
    ioc::read_ioc_csv,
    models::{IocCellMatch, IocEntry, ProjectMeta},
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

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-ioc-portable-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let store = ProjectsStore::new(root.join("workspace")).unwrap();
        Self { root, store }
    }
    fn case(&self, name: &str, body: &str) -> ProjectMeta {
        let source = self.root.join(name);
        fs::write(&source, body).unwrap();
        let meta = create_project(
            &self.store,
            CreateProjectPayload {
                path: source.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta;
        save_iocs(
            &self.store,
            SaveIocsPayload {
                project_id: meta.id,
                entries: vec![IocEntry {
                    flag: "safe".into(),
                    tag: "kept".into(),
                    query: "literal".into(),
                    cell: None,
                }],
            },
        )
        .unwrap();
        update_flag(
            &self.store,
            UpdateFlagPayload {
                project_id: meta.id,
                row_index: 0,
                flag: Some("safe".into()),
                memo: Some(Some("manual note".into())),
                tags: Some(vec!["manual".into()]),
                bookmarked: Some(true),
            },
        )
        .unwrap();
        meta
    }
    fn import(&self, id: Uuid, path: &Path) -> Result<Vec<IocEntry>, AppError> {
        import_iocs(
            &self.store,
            ImportIocsPayload {
                project_id: id,
                path: path.to_string_lossy().into(),
            },
        )
    }
    fn export(&self, id: Uuid, path: &Path, entries: Vec<IocEntry>) -> Result<(), AppError> {
        export_iocs(
            &self.store,
            ExportIocsPayload {
                project_id: id,
                destination: path.to_string_lossy().into(),
                entries: Some(entries),
                allow_overwrite: true,
            },
        )
    }
    fn snapshot(&self, id: Uuid) -> (Vec<u8>, Value, u64) {
        let path = self.store.project_dir(&id);
        (
            load_setting_bytes(&path, "iocs").unwrap().unwrap(),
            serde_json::to_value(load_flags(&self.store.flags_path(&id)).unwrap()).unwrap(),
            project_revision(&path).unwrap(),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn cell(column: &str, value: &str) -> IocEntry {
    IocEntry {
        flag: "critical".into(),
        tag: "portable".into(),
        query: String::new(),
        cell: Some(IocCellMatch {
            column: column.into(),
            value: value.into(),
        }),
    }
}
fn column<'a>(meta: &'a ProjectMeta, source: &str) -> &'a str {
    &meta
        .columns
        .iter()
        .find(|column| column.source == source)
        .unwrap()
        .id
}
fn write_cell_csv(path: &Path, source: &str, value: &str) {
    let mut writer = csv::Writer::from_path(path).unwrap();
    writer
        .write_record(["flag", "tag", "query", "column", "value"])
        .unwrap();
    writer
        .write_record(["critical", "portable", "", source, value])
        .unwrap();
    writer.flush().unwrap();
}

#[test]
fn csv_source_names_and_json_pointers_follow_fields_across_reordered_cases() {
    for (first_name, first_body, next_name, next_body, source) in [
        (
            "first.csv",
            "first,message\nunused,literal\n",
            "next.csv",
            "message,first\nliteral,unused\n",
            "message",
        ),
        (
            "first.json",
            r#"{"a/b":{"~":"literal"},"id":1}"#,
            "next.json",
            r#"{"id":1,"a/b":{"~":"literal"}}"#,
            "/a~1b/~0",
        ),
    ] {
        let fixture = Fixture::new();
        let first = fixture.case(first_name, first_body);
        let next = fixture.case(next_name, next_body);
        let old_id = column(&first, source);
        let new_id = column(&next, source);
        assert_ne!(
            old_id, new_id,
            "the fixture must actually reorder internal IDs"
        );
        let value = " \"literal\" C:\\Windows\\日本語\r\n ";
        let output = fixture.root.join("portable.csv");
        fixture
            .export(first.id, &output, vec![cell(old_id, value)])
            .unwrap();
        let exported = read_ioc_csv(&output).unwrap();
        assert_eq!(exported[0].cell.as_ref().unwrap().column, source);
        let before = fixture.snapshot(next.id);
        let imported = fixture.import(next.id, &output).unwrap();
        assert_eq!(imported, [cell(new_id, value)]);
        assert_eq!(fixture.snapshot(next.id).1, before.1);
        assert_eq!(fixture.snapshot(next.id).2, before.2);
        // Re-exporting the imported API rule still writes the stable source.
        let second_output = fixture.root.join("portable-again.csv");
        fixture.export(next.id, &second_output, imported).unwrap();
        assert_eq!(fs::read(output).unwrap(), fs::read(second_output).unwrap());
    }
}

#[test]
fn csv_import_treats_id_like_names_as_sources_and_never_falls_back_to_ids() {
    let fixture = Fixture::new();
    let meta = fixture.case("collision.csv", "ordinary,c000001\nfirst,literal\n");
    let actual_id = column(&meta, "c000001");
    assert_eq!(actual_id, "c000002");
    let csv = fixture.root.join("collision-rule.csv");
    write_cell_csv(&csv, "c000001", "literal");
    assert_eq!(
        fixture.import(meta.id, &csv).unwrap(),
        [cell(actual_id, "literal")]
    );
    let before = fixture.snapshot(meta.id);
    // c000002 is a valid ID, but not a source name in this case.
    write_cell_csv(&csv, "c000002", "literal");
    let error = fixture.import(meta.id, &csv).unwrap_err().to_string();
    assert!(
        error.contains("source column") && error.contains("not found"),
        "{error}"
    );
    assert_eq!(fixture.snapshot(meta.id), before);
    let output = fixture.root.join("collision-export.csv");
    fixture
        .export(meta.id, &output, vec![cell(actual_id, "literal")])
        .unwrap();
    assert_eq!(
        read_ioc_csv(&output).unwrap()[0]
            .cell
            .as_ref()
            .unwrap()
            .column,
        "c000001"
    );
}

#[test]
fn ambiguous_and_unnamed_sources_never_replace_rules_exports_or_annotations() {
    for (body, source, expected) in [
        ("message,message\nliteral,other\n", "message", "ambiguous"),
        (",first\nliteral,other\n", "", "unnamed"),
        ("  ,first\nliteral,other\n", "  ", "unnamed"),
    ] {
        let fixture = Fixture::new();
        let meta = fixture.case("ambiguous.csv", body);
        let before = fixture.snapshot(meta.id);
        let output = fixture.root.join("existing.csv");
        fs::write(&output, "existing output\n").unwrap();
        let error = fixture
            .export(meta.id, &output, vec![cell(&meta.columns[0].id, "literal")])
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
        assert_eq!(fs::read_to_string(output).unwrap(), "existing output\n");
        let input = fixture.root.join("invalid-source.csv");
        write_cell_csv(&input, source, "literal");
        assert!(fixture.import(meta.id, &input).is_err());
        assert_eq!(fixture.snapshot(meta.id), before);
    }
}

//! CSV import: configurable delimiters and delimiter option parsing.

use std::fs;
use std::path::{Path, PathBuf};

use polars::prelude::DataFrame;
use uuid::Uuid;

use trivium_engine::import::csv;
use trivium_engine::ops;
use trivium_engine::project_io::is_internal_column;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};
use trivium_engine::store::ProjectsStore;

fn fixture_text(separator: char) -> String {
    let header = ["ts", "EventID", "host", "user", "proc", "path", "msg"];
    let rows: [[&str; 7]; 3] = [
        [
            "2024-01-01T00:00:01",
            "4624",
            "WS01",
            "alice",
            "explorer",
            "C:/Windows/explorer.exe",
            "logon ok",
        ],
        [
            "2024-01-01T00:00:02",
            "4688",
            "WS02",
            "bob",
            "powershell",
            "C:/Windows/System32/ps.exe",
            "process created",
        ],
        [
            "2024-01-01T00:00:03",
            "4625",
            "WS03",
            "carol",
            "cmd",
            "C:/Windows/System32/cmd.exe",
            "logon failed",
        ],
    ];

    let separator = separator.to_string();
    let mut text = header.join(&separator);
    text.push('\n');
    for row in rows {
        text.push_str(&row.join(&separator));
        text.push('\n');
    }
    text
}

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-import-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn write_fixture(root: &Path, name: &str, separator: char) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, fixture_text(separator)).expect("failed to write fixture");
    path
}

fn utf16_bytes(text: &str, big_endian: bool) -> Vec<u8> {
    let mut out = if big_endian {
        vec![0xFE, 0xFF]
    } else {
        vec![0xFF, 0xFE]
    };
    for unit in text.encode_utf16() {
        let bytes = if big_endian {
            unit.to_be_bytes()
        } else {
            unit.to_le_bytes()
        };
        out.extend_from_slice(&bytes);
    }
    out
}

fn write_utf16_fixture(root: &Path, name: &str, big_endian: bool) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, utf16_bytes(&fixture_text(','), big_endian))
        .expect("failed to write fixture");
    path
}

fn text_at(df: &DataFrame, column: &str, row: usize) -> Option<String> {
    df.column(column)
        .ok()?
        .str()
        .ok()?
        .get(row)
        .map(|value| value.to_string())
}

fn assert_same_import(ascii: &DataFrame, decoded: &DataFrame) {
    assert_eq!(ascii.get_column_names(), decoded.get_column_names());
    assert_eq!(ascii.height(), decoded.height());
    for column in ascii.get_column_names() {
        if column == "__rowid" || column == "__original" {
            continue;
        }
        for row in 0..ascii.height() {
            assert_eq!(
                text_at(ascii, column, row),
                text_at(decoded, column, row),
                "column {} row {} differs between ascii and utf16 import",
                column,
                row
            );
        }
    }
}

macro_rules! column_count {
    ($df:expr) => {
        $df.get_column_names()
            .into_iter()
            .filter(|name| !is_internal_column(name))
            .count()
    };
}

#[test]
fn comma_fixture_loads_with_default_delimiter() {
    let root = temp_root();
    let path = write_fixture(&root, "comma.csv", ',');

    let imported = csv::load(&path, None, false).expect("comma load failed");
    let df = imported.frame;
    assert_eq!(column_count!(&df), 7);
    assert_eq!(df.height(), 3);
    assert_eq!(imported.columns.len(), 7);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn tab_fixture_loads_with_tab_delimiter() {
    let root = temp_root();
    let path = write_fixture(&root, "tab.tsv", '\t');

    let imported = csv::load(&path, Some(b'\t'), false).expect("tab load failed");
    let df = imported.frame;
    assert_eq!(column_count!(&df), 7);
    assert_eq!(df.height(), 3);
    assert_eq!(imported.columns.len(), 7);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn pipe_fixture_loads_with_pipe_delimiter() {
    let root = temp_root();
    let path = write_fixture(&root, "pipe.psv", '|');

    let imported = csv::load(&path, Some(b'|'), false).expect("pipe load failed");
    let df = imported.frame;
    assert_eq!(column_count!(&df), 7);
    assert_eq!(df.height(), 3);
    assert_eq!(imported.columns.len(), 7);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn wrong_delimiter_never_yields_seven_columns() {
    let root = temp_root();
    let path = write_fixture(&root, "pipe.psv", '|');

    match csv::load(&path, Some(b','), false) {
        Err(_) => {}
        Ok(imported) => assert_ne!(
            column_count!(&imported.frame),
            7,
            "comma-parsed pipe data must not look like a valid 7-column import"
        ),
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn build_delimiter_maps_options_to_bytes() {
    // The plan fixes no selection/empty to a comma, not auto-detection.
    assert_eq!(
        csv::build_delimiter(None).expect("none"),
        Some(b',')
    );
    assert_eq!(
        csv::build_delimiter(Some("")).expect("empty"),
        Some(b',')
    );
    assert_eq!(
        csv::build_delimiter(Some("\t")).expect("tab"),
        Some(b'\t')
    );
    assert_eq!(csv::build_delimiter(Some(",")).expect("comma"), Some(b','));
    assert_eq!(
        csv::build_delimiter(Some(";")).expect("semicolon"),
        Some(b';')
    );
    assert_eq!(csv::build_delimiter(Some("|")).expect("pipe"), Some(b'|'));
    assert_eq!(csv::build_delimiter(Some("^")).expect("caret"), Some(b'^'));
    assert_eq!(
        csv::build_delimiter(Some(":")).expect("colon"),
        Some(b':')
    );

    assert!(csv::build_delimiter(Some("ab")).is_err());
    assert!(csv::build_delimiter(Some("; ;")).is_err());
}

#[test]
fn ops_create_project_accepts_tab_delimiter() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_fixture(&root, "tab.tsv", '\t');

    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: Some("\t".to_string()),
            restore_annotations: None,
        },
    )
    .expect("failed to create project from tab fixture");
    assert_eq!(summary.meta.total_records, 3);

    let loaded = ops::projects::load_project(
        &store,
        ProjectRequest {
            project_id: summary.meta.id,
        },
    )
    .expect("failed to load created project");
    assert_eq!(loaded.columns.len(), 7);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn create_project_records_source_sha256() {
    use sha2::{Digest, Sha256};

    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let path = write_fixture(&root, "sha.csv", ',');

    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project from fixture");

    let bytes = fs::read(&path).expect("failed to read fixture");
    let expected = format!("{:x}", Sha256::digest(&bytes));
    assert_eq!(
        summary.meta.source_sha256.as_deref(),
        Some(expected.as_str()),
        "recorded sha256 must match the source file digest"
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn legacy_project_meta_json_loads_without_sha256() {
    use trivium_engine::models::ProjectMeta;

    let json = r#"{"id":"0f8f1f7e-6f10-4f6a-9f8a-0f8f1f7e6f10","name":"old","description":null,"created_at":"2024-01-01T00:00:00Z","total_records":3,"flagged_records":0,"ioc_applied_records":0,"hidden_columns":[]}"#;
    let meta: ProjectMeta = serde_json::from_str(json).expect("legacy meta failed to parse");
    assert_eq!(meta.source_sha256, None, "legacy meta must default to no digest");
}

#[test]
fn utf16_le_fixture_imports_same_as_ascii() {
    let root = temp_root();
    let ascii_path = write_fixture(&root, "ascii.csv", ',');
    let utf16_path = write_utf16_fixture(&root, "utf16le.csv", false);

    let ascii = csv::load(&ascii_path, None, false)
        .expect("ascii load failed")
        .frame;
    let decoded = csv::load(&utf16_path, None, false)
        .expect("utf16le load failed")
        .frame;
    assert_same_import(&ascii, &decoded);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn utf16_be_fixture_imports_same_as_ascii() {
    let root = temp_root();
    let ascii_path = write_fixture(&root, "ascii.csv", ',');
    let utf16_path = write_utf16_fixture(&root, "utf16be.csv", true);

    let ascii = csv::load(&ascii_path, None, false)
        .expect("ascii load failed")
        .frame;
    let decoded = csv::load(&utf16_path, None, false)
        .expect("utf16be load failed")
        .frame;
    assert_same_import(&ascii, &decoded);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn utf16_fixture_imports_with_explicit_delimiter() {
    let root = temp_root();
    let mut out = vec![0xFF, 0xFE];
    for unit in fixture_text('\t').encode_utf16() {
        out.extend_from_slice(&unit.to_le_bytes());
    }
    let path = root.join("utf16le.tsv");
    fs::write(&path, out).expect("failed to write fixture");

    let imported = csv::load(&path, Some(b'\t'), false).expect("utf16 tsv load failed");
    let msg_id = imported
        .columns
        .iter()
        .find(|column| column.source == "msg")
        .expect("msg column")
        .id
        .clone();
    let df = imported.frame;
    assert_eq!(column_count!(&df), 7);
    assert_eq!(df.height(), 3);
    assert_eq!(
        text_at(&df, &msg_id, 0),
        Some("logon ok".to_string()),
        "representative value must survive the utf16 decode"
    );

    let _ = fs::remove_dir_all(root);
}

//! CSV import: configurable delimiters and delimiter option parsing.

use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::import::csv;
use trivium_engine::ops;
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

macro_rules! column_count {
    ($df:expr) => {
        $df.get_column_names()
            .into_iter()
            .filter(|name| name != &"__rowid")
            .count()
    };
}

#[test]
fn comma_fixture_loads_with_automatic_delimiter() {
    let root = temp_root();
    let path = write_fixture(&root, "comma.csv", ',');

    let (df, _) = csv::load(&path, None).expect("comma load failed");
    assert_eq!(column_count!(&df), 7);
    assert_eq!(df.height(), 3);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn tab_fixture_loads_with_tab_delimiter() {
    let root = temp_root();
    let path = write_fixture(&root, "tab.tsv", '\t');

    let (df, _) = csv::load(&path, Some(b'\t')).expect("tab load failed");
    assert_eq!(column_count!(&df), 7);
    assert_eq!(df.height(), 3);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn pipe_fixture_loads_with_pipe_delimiter() {
    let root = temp_root();
    let path = write_fixture(&root, "pipe.psv", '|');

    let (df, _) = csv::load(&path, Some(b'|')).expect("pipe load failed");
    assert_eq!(column_count!(&df), 7);
    assert_eq!(df.height(), 3);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn wrong_delimiter_never_yields_seven_columns() {
    let root = temp_root();
    let path = write_fixture(&root, "pipe.psv", '|');

    match csv::load(&path, Some(b',')) {
        Err(_) => {}
        Ok((df, _)) => assert_ne!(
            column_count!(&df),
            7,
            "comma-parsed pipe data must not look like a valid 7-column import"
        ),
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn build_delimiter_maps_options_to_bytes() {
    assert_eq!(csv::build_delimiter(None).expect("none"), None);
    assert_eq!(csv::build_delimiter(Some("")).expect("empty"), None);
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

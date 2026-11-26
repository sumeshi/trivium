//! Restored annotation booleans must never guess an investigation result.
use std::{fs, path::Path};

use trivium_engine::{
    import::csv::DEFAULT_BATCH_ROWS,
    ops::projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
    store::ProjectsStore,
};

const HEADERS: [&str; 5] = [
    "value",
    "trivium-safe",
    "trivium-suspicious",
    "trivium-critical",
    "trivium-bookmarked",
];

fn payload(path: &Path, restore: bool) -> CreateProjectPayload {
    CreateProjectPayload {
        import_options: None,
        path: path.to_string_lossy().into(),
        description: None,
        delimiter: None,
        restore_annotations: Some(restore),
    }
}

fn write_csv(path: &Path, rows: &[[&str; 5]]) {
    let mut writer = csv::Writer::from_path(path).unwrap();
    writer.write_record(HEADERS).unwrap();
    for row in rows {
        writer.write_record(row).unwrap();
    }
    writer.flush().unwrap();
}

fn assert_no_case(store: &ProjectsStore) {
    assert!(store.try_all().unwrap().is_empty());
    assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
}

#[test]
fn boolean_spellings_restore_consistently_without_altering_source_cells() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let path = root.path().join("booleans.csv");
    let rows = [
        ["safe", "1", "false", "0", "true"],
        ["suspicious", " FALSE ", "TrUe", "", "0"],
        ["critical", "0", "0", " true ", " FALSE "],
        ["unflagged", "", " 0 ", "false", "1"],
        ["unflagged", "false", "FALSE", " 0 ", " 0 "],
        ["unflagged", "", "", "", ""],
    ];
    write_csv(&path, &rows);
    let bytes = fs::read(&path).unwrap();
    let project = create_project(&store, payload(&path, true)).unwrap();
    assert_eq!(project.meta.flagged_records, 3);
    let loaded = load_project(
        &store,
        ProjectRequest {
            project_id: project.meta.id,
        },
    )
    .unwrap();

    let loaded_rows = common::rows(&store, loaded.project.meta.id);
    assert_eq!(loaded_rows.len(), rows.len());
    for (index, ((row, expected), bookmark)) in loaded_rows
        .iter()
        .zip(rows.iter())
        .zip([true, false, false, true, false, false])
        .enumerate()
    {
        assert_eq!(row.row_index, index);
        assert_eq!(row.flag, if index < 3 { expected[0] } else { "" });
        assert_eq!(row.bookmarked, bookmark);
        for (column, value) in loaded.columns_meta.iter().zip(expected) {
            assert_eq!(row.cells[&column.id].text.as_deref(), Some(*value));
        }
    }
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn unknown_annotation_booleans_fail_atomically_but_ordinary_import_keeps_raw_data() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("invalid.csv");
    for column in 1..HEADERS.len() {
        for value in ["banana", "-1", "2", "0.0", "null"] {
            let store =
                ProjectsStore::new(root.path().join(format!("case-{column}-{value}"))).unwrap();
            let mut invalid = ["invalid", "0", "0", "0", "0"];
            invalid[column] = value;
            write_csv(&path, &[["valid", "0", "0", "0", "0"], invalid]);
            let bytes = fs::read(&path).unwrap();
            let error = create_project(&store, payload(&path, true))
                .unwrap_err()
                .to_string();
            assert!(error.contains("CSV record 2 (line 3)"), "{error}");
            assert!(
                error.contains(HEADERS[column]) && error.contains(value),
                "{error}"
            );
            assert!(error.contains(&format!("column {}", column + 1)), "{error}");
            assert_no_case(&store);
            let project = create_project(&store, payload(&path, false)).unwrap();
            assert_eq!(project.meta.flagged_records, 0);
            let loaded = load_project(
                &store,
                ProjectRequest {
                    project_id: project.meta.id,
                },
            )
            .unwrap();

            let loaded_rows = common::rows(&store, loaded.project.meta.id);
            let row = &loaded_rows[1];
            assert_eq!(row.flag, "");
            assert!(!row.bookmarked);
            assert_eq!(
                row.cells[&loaded.columns_meta[column].id].text.as_deref(),
                Some(value)
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }
}

#[test]
fn contradictory_flags_are_rejected_instead_of_picking_a_precedence() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let path = root.path().join("conflicting.csv");
    for enabled in [[1, 2], [1, 3], [2, 3]] {
        let mut invalid = ["invalid", "0", "0", "0", "0"];
        invalid[enabled[0]] = "1";
        invalid[enabled[1]] = "TRUE";
        write_csv(&path, &[["valid", "0", "0", "0", "0"], invalid]);
        let bytes = fs::read(&path).unwrap();
        let error = create_project(&store, payload(&path, true))
            .unwrap_err()
            .to_string();
        assert!(error.contains("CSV record 2 (line 3)"), "{error}");
        assert!(error.contains("Conflicting flag annotations"), "{error}");
        assert!(
            error.contains(HEADERS[enabled[0]]) && error.contains(HEADERS[enabled[1]]),
            "{error}"
        );
        assert!(
            error.contains("\"1\"") && error.contains("\"TRUE\""),
            "{error}"
        );
        assert_no_case(&store);
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn invalid_annotations_after_a_written_batch_leave_no_partial_case() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let path = root.path().join("late-error.csv");
    let mut writer = csv::Writer::from_path(&path).unwrap();
    writer.write_record(HEADERS).unwrap();
    for _ in 0..DEFAULT_BATCH_ROWS {
        writer.write_record(["valid", "0", "0", "0", "0"]).unwrap();
    }
    writer
        .write_record(["invalid", "not-a-boolean", "0", "0", "0"])
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    let bytes = fs::read(&path).unwrap();
    let error = create_project(&store, payload(&path, true))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(&format!(
            "CSV record {} (line {})",
            DEFAULT_BATCH_ROWS + 1,
            DEFAULT_BATCH_ROWS + 2
        )),
        "{error}"
    );
    assert_no_case(&store);
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

mod common;

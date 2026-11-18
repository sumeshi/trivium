//! Reject unsupported CSV settings/encodings without publishing partial cases.
use std::{fs, path::Path};

use trivium_engine::{
    import::csv::{build_delimiter, BatchLimits, CsvImporter},
    jobs::JobContext,
    ops::export::{export_project, ExportProjectPayload},
    ops::projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
    ops::rows::QuerySpec,
    store::ProjectsStore,
};

fn payload(path: &Path, delimiter: Option<String>) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().into(),
        description: None,
        delimiter,
        restore_annotations: None,
    }
}

fn assert_no_partial_case(store: &ProjectsStore) {
    assert!(store.try_all().unwrap().is_empty());
    assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
}

#[test]
fn reserved_delimiters_are_rejected_by_both_option_and_direct_reader_apis() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source.csv");
    let bytes = b"id,message\n00123,unaltered\n";
    fs::write(&path, bytes).unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    for delimiter in *b"\r\n\"" {
        let option = char::from(delimiter).to_string();
        let error = build_delimiter(Some(&option)).unwrap_err();
        assert!(error.to_string().contains("Invalid delimiter"));
        assert!(CsvImporter::open(&path, Some(delimiter), BatchLimits::default()).is_err());
        let error = create_project(&store, payload(&path, Some(option))).unwrap_err();
        assert!(error.to_string().contains("Invalid delimiter"));
        assert_no_partial_case(&store);
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    for delimiter in *b"\t ,;|^:\\~" {
        let option = char::from(delimiter).to_string();
        assert_eq!(build_delimiter(Some(&option)).unwrap(), Some(delimiter));
    }
    let imported = create_project(&store, payload(&path, None)).unwrap();
    assert_eq!(imported.meta.total_records, 1);
}

#[test]
fn ascii_control_delimiters_preserve_values_through_import_export_and_reimport() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    for delimiter in [0, 0x1F, 0x7F] {
        let path = root.path().join(format!("control-{delimiter}.csv"));
        let note = format!("before{}after, \"quoted\"\n調査😀", char::from(delimiter));
        let rows = [["00123", note.as_str()], ["00000", "last"]];
        let mut writer = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .from_path(&path)
            .unwrap();
        writer.write_record(["id", "message"]).unwrap();
        for row in &rows {
            writer.write_record(row).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);
        let imported = create_project(
            &store,
            payload(&path, Some(char::from(delimiter).to_string())),
        )
        .unwrap();
        let output = root.path().join(format!("export-{delimiter}.csv"));
        export_project(
            &store,
            ExportProjectPayload {
                project_id: imported.meta.id,
                destination: output.to_string_lossy().into(),
                allow_overwrite: false,
                scope: Some("all".into()),
                spec: QuerySpec::default(),
                columns: None,
                row_indices: None,
                include_annotations: Some(false),
            },
        )
        .unwrap();
        let restored = create_project(&store, payload(&output, None)).unwrap();
        for project_id in [imported.meta.id, restored.meta.id] {
            let loaded = load_project(&store, ProjectRequest { project_id }).unwrap();
            assert_eq!(loaded.project.meta.total_records, rows.len());
            for (index, expected) in rows.iter().enumerate() {
                let actual = &loaded.initial_rows[index];
                assert_eq!(actual.row_index, index);
                assert_eq!(actual.cells["c000001"].text.as_deref(), Some(expected[0]));
                assert_eq!(actual.cells["c000002"].text.as_deref(), Some(expected[1]));
            }
        }
    }
}

fn utf32_bytes(text: &str, big_endian: bool) -> Vec<u8> {
    let mut bytes = if big_endian {
        vec![0, 0, 0xFE, 0xFF]
    } else {
        vec![0xFF, 0xFE, 0, 0]
    };
    for character in text.chars() {
        let unit = character as u32;
        bytes.extend(if big_endian {
            unit.to_be_bytes()
        } else {
            unit.to_le_bytes()
        });
    }
    bytes
}

#[test]
fn utf32_boms_reject_without_garbled_rows_or_published_cases_and_valid_import_recovers() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    for big_endian in [false, true] {
        let path = root.path().join("utf32.csv");
        let bytes = utf32_bytes("value\n00123\n調査😀\n", big_endian);
        fs::write(&path, &bytes).unwrap();
        let error = create_project(&store, payload(&path, None)).unwrap_err();
        assert!(
            error.to_string().contains("UTF-32 CSV is not supported"),
            "{error}"
        );
        assert_no_partial_case(&store);
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    let path = root.path().join("valid.csv");
    fs::write(&path, "\u{feff}value\n00123\n調査😀\n").unwrap();
    let imported = create_project(&store, payload(&path, None)).unwrap();
    let loaded = load_project(
        &store,
        ProjectRequest {
            project_id: imported.meta.id,
        },
    )
    .unwrap();
    assert_eq!(loaded.project.meta.total_records, 2);
    assert_eq!(loaded.columns_meta[0].source, "value");
    assert_eq!(loaded.initial_rows[0].row_index, 0);
    assert_eq!(
        loaded.initial_rows[0].cells["c000001"].text.as_deref(),
        Some("00123")
    );
    assert_eq!(loaded.initial_rows[1].row_index, 1);
    assert_eq!(
        loaded.initial_rows[1].cells["c000001"].text.as_deref(),
        Some("調査😀")
    );
}

#[test]
fn short_utf8_bom_and_utf16_inputs_still_decode_through_tiny_buffers() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("short.csv");
    for bytes in [
        vec![0xEF, 0xBB, 0xBF, b'x'],
        vec![0xFF, 0xFE, b'x', 0],
        vec![0xFE, 0xFF, 0, b'x'],
    ] {
        fs::write(&path, bytes).unwrap();
        let mut importer = CsvImporter::open(
            &path,
            None,
            BatchLimits {
                max_rows: 1,
                max_bytes: 1,
                read_buffer: 1,
            },
        )
        .unwrap();
        assert_eq!(importer.columns()[0].source, "x");
        assert!(importer.next_batch(&JobContext::none()).unwrap().is_none());
    }
}

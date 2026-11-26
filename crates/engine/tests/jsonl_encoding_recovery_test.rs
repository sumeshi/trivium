//! Per-line decoding must preserve readable neighbors, source positions, hashes,
//! and the strict-mode failure contract without manufacturing replacement text.
use std::{fs, path::Path};

use sha2::{Digest, Sha256};
use trivium_engine::{
    import::options::{ImportIssueAction, ImportOptions, TextEncoding},
    ops::{
        projects::{create_project, CreateProjectPayload},
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    project_io::{read_project_dataframe, INTERNAL_ORIGINAL, INTERNAL_SOURCE_RECORD},
    store::ProjectsStore,
};

fn encode(text: &str, encoding: TextEncoding) -> Vec<u8> {
    match encoding {
        TextEncoding::Utf16Le => text.encode_utf16().flat_map(u16::to_le_bytes).collect(),
        TextEncoding::Utf16Be => text.encode_utf16().flat_map(u16::to_be_bytes).collect(),
        TextEncoding::Utf32Le => text
            .chars()
            .flat_map(|c| (c as u32).to_le_bytes())
            .collect(),
        TextEncoding::Utf32Be => text
            .chars()
            .flat_map(|c| (c as u32).to_be_bytes())
            .collect(),
        _ => text.as_bytes().to_vec(),
    }
}

fn marker(encoding: TextEncoding) -> Vec<u8> {
    match encoding {
        TextEncoding::Utf16Le => vec![0xff, 0xfe],
        TextEncoding::Utf16Be => vec![0xfe, 0xff],
        TextEncoding::Utf32Le => vec![0xff, 0xfe, 0, 0],
        TextEncoding::Utf32Be => vec![0, 0, 0xfe, 0xff],
        _ => vec![],
    }
}

fn invalid_unit(encoding: TextEncoding) -> Vec<u8> {
    match encoding {
        TextEncoding::Utf16Le => 0xd800u16.to_le_bytes().to_vec(),
        TextEncoding::Utf16Be => 0xd800u16.to_be_bytes().to_vec(),
        TextEncoding::Utf32Le => 0x11_0000u32.to_le_bytes().to_vec(),
        TextEncoding::Utf32Be => 0x11_0000u32.to_be_bytes().to_vec(),
        TextEncoding::ShiftJis => vec![0x81],
        _ => vec![0xff],
    }
}

fn payload(path: &Path, encoding: TextEncoding, recover: bool) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().into_owned(),
        description: None,
        delimiter: None,
        restore_annotations: None,
        import_options: Some(ImportOptions {
            encoding,
            recover,
            ..Default::default()
        }),
    }
}

#[test]
fn unicode_jsonl_recovers_an_invalid_middle_line_without_shifting_source_positions() {
    for encoding in [
        TextEncoding::Utf16Le,
        TextEncoding::Utf16Be,
        TextEncoding::Utf32Le,
        TextEncoding::Utf32Be,
    ] {
        let root = tempfile::tempdir().unwrap();
        let store = ProjectsStore::new(root.path().join("cases")).unwrap();
        let source = root.path().join("unicode.ndjson");
        let mut bytes = marker(encoding);
        bytes.extend(encode(
            "{\"message\":\"first 日本語 😀\",\"id\":\"00123\"}\r\n\n{\"bad\":\"",
            encoding,
        ));
        bytes.extend(invalid_unit(encoding));
        bytes.extend(encode(
            "\"}\n{\"message\":\"last\",\"id\":\"00001\"}\n",
            encoding,
        ));
        fs::write(&source, &bytes).unwrap();
        assert!(create_project(&store, payload(&source, TextEncoding::Auto, false)).is_err());
        assert!(store.all().is_empty());
        let project = create_project(&store, payload(&source, TextEncoding::Auto, true)).unwrap();
        assert_eq!(project.meta.total_records, 2);
        assert_eq!(
            project.meta.source_sha256,
            Some(format!("{:x}", Sha256::digest(&bytes)))
        );
        let report = project.meta.import_report.as_ref().unwrap();
        assert_eq!(
            (
                report.source_records,
                report.imported_records,
                report.skipped_records
            ),
            (3, 2, 1)
        );
        assert_eq!(report.issues[0].source_record, Some(3));
        assert_eq!(report.issues[0].action, ImportIssueAction::Skipped);
        assert!(!report.unreadable_tail);
        let rows = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: project.meta.id,
                spec: QuerySpec::default(),
                offset: None,
                limit: None,
            },
        )
        .unwrap()
        .rows;
        let message = &project.meta.columns[0].id;
        let id = &project.meta.columns[1].id;
        assert_eq!(
            rows.iter().map(|row| row.row_index).collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(
            rows[0].cells[message].text.as_deref(),
            Some("first 日本語 😀")
        );
        assert_eq!(rows[1].cells[message].text.as_deref(), Some("last"));
        assert_eq!(rows[0].cells[id].text.as_deref(), Some("00123"));
        let df = read_project_dataframe(&store.project_dir(&project.meta.id).join("data.parquet"))
            .unwrap();
        assert_eq!(
            df.column(INTERNAL_SOURCE_RECORD)
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            [1, 4]
        );
        assert!(df
            .column(INTERNAL_ORIGINAL)
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .all(|text| !text.contains('\u{fffd}')));
        assert_eq!(fs::read(&source).unwrap(), bytes);
    }
}

#[test]
fn bomless_selected_shift_jis_uses_independent_strict_line_decoders() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let source = root.path().join("legacy.jsonl");
    let mut bytes = b"{\"message\":\"".to_vec();
    bytes.extend_from_slice(&[0x93, 0xfa, 0x96, 0x7b]); // 日本
    bytes.extend_from_slice(b"\"}\n{\"message\":\"");
    bytes.extend_from_slice(&[0x81, b'\n']); // Invalid lead byte cannot consume the next line.
    bytes.extend_from_slice(b"{\"message\":\"last\"}\n");
    fs::write(&source, &bytes).unwrap();
    assert!(create_project(&store, payload(&source, TextEncoding::ShiftJis, false)).is_err());
    let project = create_project(&store, payload(&source, TextEncoding::ShiftJis, true)).unwrap();
    let report = project.meta.import_report.unwrap();
    assert_eq!(report.encoding, "shift-jis");
    assert!(!report.bom);
    assert_eq!(report.skipped_records, 1);
    assert_eq!(report.issues[0].source_record, Some(2));
    let rows = query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: project.meta.id,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 2);
    let column = &project.meta.columns[0].id;
    assert_eq!(rows[0].cells[column].text.as_deref(), Some("日本"));
    assert_eq!(rows[1].cells[column].text.as_deref(), Some("last"));
    assert_eq!(fs::read(source).unwrap(), bytes);
}

#[test]
fn truncated_unicode_unit_is_an_unknown_tail_not_a_guessed_next_record() {
    for encoding in [
        TextEncoding::Utf16Le,
        TextEncoding::Utf16Be,
        TextEncoding::Utf32Le,
        TextEncoding::Utf32Be,
    ] {
        let root = tempfile::tempdir().unwrap();
        let store = ProjectsStore::new(root.path().join("cases")).unwrap();
        let source = root.path().join("truncated.jsonl");
        let mut bytes = marker(encoding);
        bytes.extend(encode(
            "{\"message\":\"first\"}\n{\"message\":\"unfinished\"}",
            encoding,
        ));
        bytes.push(b'x');
        fs::write(&source, &bytes).unwrap();
        let project = create_project(&store, payload(&source, TextEncoding::Auto, true)).unwrap();
        assert_eq!(project.meta.total_records, 1);
        let report = project.meta.import_report.unwrap();
        assert!(report.unreadable_tail);
        assert_eq!(
            report.skipped_records, 0,
            "unknown remainder is not a counted skipped record"
        );
        assert_eq!(report.issues[0].source_record, Some(2));
        assert_eq!(report.issues[0].action, ImportIssueAction::UnreadableTail);
    }
}

#[test]
fn complete_unit_decode_failures_do_not_turn_into_successful_empty_cases() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let source = root.path().join("invalid.jsonl");
    let mut bytes = marker(TextEncoding::Utf16Le);
    bytes.extend(encode("{\"bad\":\"", TextEncoding::Utf16Le));
    bytes.extend(invalid_unit(TextEncoding::Utf16Le));
    bytes.extend(encode("\"}\n", TextEncoding::Utf16Le));
    fs::write(&source, bytes).unwrap();
    let error = create_project(&store, payload(&source, TextEncoding::Auto, true)).unwrap_err();
    assert!(
        error.to_string().contains("No readable JSON records"),
        "{error}"
    );
    assert!(store.all().is_empty());
    assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
}

#[test]
fn recovery_does_not_hide_a_literal_second_bom_or_override_an_explicit_conflict() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let source = root.path().join("second-bom.jsonl");
    let mut bytes = marker(TextEncoding::Utf16Le);
    bytes.extend(encode(
        "\u{feff}{\"message\":\"invalid-prefix\"}\n{\"message\":\"last\"}\n",
        TextEncoding::Utf16Le,
    ));
    fs::write(&source, &bytes).unwrap();
    assert!(create_project(&store, payload(&source, TextEncoding::Utf16Be, true)).is_err());
    let project = create_project(&store, payload(&source, TextEncoding::Auto, true)).unwrap();
    assert_eq!(project.meta.total_records, 1);
    assert_eq!(project.meta.import_report.unwrap().skipped_records, 1);
    assert_eq!(fs::read(source).unwrap(), bytes);
}

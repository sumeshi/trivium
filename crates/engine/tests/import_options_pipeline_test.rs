//! Exercise decoding/recovery through the same staged import and saved-case API as the GUI.
use std::{fs, path::Path};

use sha2::{Digest, Sha256};
use trivium_engine::{
    import::options::{ImportOptions, TextEncoding},
    models::ProjectSummary,
    ops::{
        projects::{create_project, list_projects, CreateProjectPayload},
        rows::{query_project_rows, QueryRowsPayload, QuerySpec},
    },
    store::ProjectsStore,
};

fn payload(path: &Path, options: Option<ImportOptions>) -> CreateProjectPayload {
    CreateProjectPayload {
        path: path.to_string_lossy().into_owned(),
        description: None,
        delimiter: None,
        restore_annotations: None,
        import_options: options,
    }
}

fn text(
    store: &ProjectsStore,
    project: &ProjectSummary,
    column: usize,
    row: usize,
) -> Option<String> {
    let rows = query_project_rows(
        store,
        QueryRowsPayload {
            project_id: project.meta.id,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .unwrap()
    .rows;
    rows[row].cells[&project.meta.columns[column].id]
        .text
        .clone()
}

#[test]
fn omitted_and_partial_options_stay_strict_with_headers_and_auto_encoding() {
    let omitted: CreateProjectPayload = serde_json::from_str(r#"{"path":"input.csv"}"#).unwrap();
    assert!(omitted.import_options.is_none());
    let partial: CreateProjectPayload =
        serde_json::from_str(r#"{"path":"input.csv","importOptions":{"recover":true}}"#).unwrap();
    assert_eq!(
        partial.import_options.unwrap(),
        ImportOptions {
            recover: true,
            ..Default::default()
        }
    );
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let path = root.path().join("ragged.csv");
    fs::write(&path, "A,B\n1,2\n3\n").unwrap();
    assert!(create_project(&store, payload(&path, None)).is_err());
    assert!(store.all().is_empty());
}

#[test]
fn unicode_bom_decodes_for_all_formats_without_changing_source_identity() {
    for (name, source, encoding, prefix) in [
        (
            "events.json",
            r#"[{"message":"日本語 😀"}]"#,
            TextEncoding::Utf16Le,
            vec![0xff, 0xfe],
        ),
        (
            "events.ndjson",
            "{\"message\":\"日本語 😀\"}\n",
            TextEncoding::Utf16Be,
            vec![0xfe, 0xff],
        ),
        (
            "events.csv",
            "message\r\n日本語 😀\r\n",
            TextEncoding::Utf32Le,
            vec![0xff, 0xfe, 0, 0],
        ),
        (
            "wide.json",
            r#"{"message":"日本語 😀"}"#,
            TextEncoding::Utf32Be,
            vec![0, 0, 0xfe, 0xff],
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let store = ProjectsStore::new(root.path().join("cases")).unwrap();
        let path = root.path().join(name);
        let mut bytes = prefix;
        match encoding {
            TextEncoding::Utf16Le => bytes.extend(source.encode_utf16().flat_map(u16::to_le_bytes)),
            TextEncoding::Utf16Be => bytes.extend(source.encode_utf16().flat_map(u16::to_be_bytes)),
            TextEncoding::Utf32Le => {
                bytes.extend(source.chars().flat_map(|c| (c as u32).to_le_bytes()))
            }
            TextEncoding::Utf32Be => {
                bytes.extend(source.chars().flat_map(|c| (c as u32).to_be_bytes()))
            }
            _ => unreachable!(),
        }
        fs::write(&path, &bytes).unwrap();
        let project = create_project(&store, payload(&path, None)).unwrap();
        assert_eq!(
            text(&store, &project, 0, 0).as_deref(),
            Some("日本語 😀"),
            "{name}"
        );
        assert_eq!(
            project.meta.source_sha256,
            Some(format!("{:x}", Sha256::digest(&bytes)))
        );
        assert_eq!(project.meta.source_size, Some(bytes.len() as u64));
        assert_eq!(fs::read(path).unwrap(), bytes);
        let report = project.meta.import_report.as_ref().unwrap();
        assert!(report.bom);
        assert_ne!(report.encoding, "auto");
        assert_eq!(report.imported_records, 1);
        assert_eq!(report.skipped_records, 0);
        assert!(!store
            .project_dir(&project.meta.id)
            .join("decoded-source")
            .exists());
        assert_eq!(
            list_projects(&store).unwrap()[0].meta.import_report,
            project.meta.import_report
        );
    }
}

#[test]
fn explicit_legacy_encoding_is_required_and_never_rewrites_input() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let path = root.path().join("legacy.csv");
    let bytes = b"message\nPrice \x80, caf\xe9\n";
    // Use a separator absent from the source value.
    fs::write(&path, bytes).unwrap();
    let mut args = payload(&path, None);
    args.delimiter = Some(";".into());
    assert!(create_project(&store, args).is_err());
    let mut args = payload(
        &path,
        Some(ImportOptions {
            encoding: TextEncoding::Windows1252,
            ..Default::default()
        }),
    );
    args.delimiter = Some(";".into());
    let project = create_project(&store, args).unwrap();
    assert_eq!(
        text(&store, &project, 0, 0).as_deref(),
        Some("Price €, café")
    );
    assert_eq!(
        project.meta.import_report.as_ref().unwrap().encoding,
        "windows-1252"
    );
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn recovered_report_survives_reopen_and_ids_remain_dense() {
    let root = tempfile::tempdir().unwrap();
    let cases = root.path().join("cases");
    let store = ProjectsStore::new(cases.clone()).unwrap();
    let path = root.path().join("partial.jsonl");
    fs::write(
        &path,
        "{\"message\":\"first\"}\ninvalid\n{\"message\":\"second\"}\n",
    )
    .unwrap();
    let project = create_project(
        &store,
        payload(
            &path,
            Some(ImportOptions {
                recover: true,
                ..Default::default()
            }),
        ),
    )
    .unwrap();
    let report = project.meta.import_report.clone().unwrap();
    assert_eq!(
        (
            report.source_records,
            report.imported_records,
            report.skipped_records
        ),
        (3, 2, 1)
    );
    assert_eq!(report.issues[0].source_record, Some(2));
    drop(store);
    let store = ProjectsStore::new(cases).unwrap();
    let saved = list_projects(&store).unwrap().remove(0);
    assert_eq!(saved.meta.import_report, Some(report));
    let rows = query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: saved.meta.id,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(
        rows.iter().map(|row| row.row_index).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(text(&store, &saved, 0, 1).as_deref(), Some("second"));
}

#[test]
fn malformed_encoding_and_incompatible_headerless_restore_publish_nothing() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let path = root.path().join("broken.json");
    fs::write(&path, [0xff, 0xfe, b'{', 0, 0x00, 0xd8]).unwrap();
    assert!(create_project(&store, payload(&path, None)).is_err());
    let path = root.path().join("no-header.csv");
    fs::write(&path, "1,2\n").unwrap();
    let mut args = payload(
        &path,
        Some(ImportOptions {
            has_headers: false,
            ..Default::default()
        }),
    );
    args.restore_annotations = Some(true);
    assert!(create_project(&store, args)
        .unwrap_err()
        .to_string()
        .contains("header"));
    assert!(store.all().is_empty());
    assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
}

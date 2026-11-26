//! Preview samples use the actual parsers/encodings without creating a case.
use std::{fs, path::Path};

use tempfile::TempDir;
use trivium_engine::{
    import::options::{ImportOptions, TextEncoding},
    jobs::{request_cancel, try_begin_job, JobContext, JobKind},
    ops::import_preview::{
        preview_import, preview_import_with_job, ImportPreview, PreviewImportPayload,
        MAX_PREVIEW_BYTES,
    },
};

fn payload(path: &Path, options: ImportOptions) -> PreviewImportPayload {
    PreviewImportPayload {
        path: path.to_string_lossy().into_owned(),
        delimiter: None,
        import_options: Some(options),
    }
}

fn sample(name: &str, bytes: impl AsRef<[u8]>, options: ImportOptions) -> (TempDir, ImportPreview) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(name);
    let source = bytes.as_ref();
    fs::write(&path, source).unwrap();
    let result = preview_import(payload(&path, options)).unwrap();
    assert_eq!(
        fs::read(path).unwrap(),
        source,
        "preview never rewrites source bytes"
    );
    assert_eq!(
        fs::read_dir(root.path()).unwrap().count(),
        1,
        "preview creates no case or intermediate files"
    );
    (root, result)
}

#[test]
fn tab_candidate_does_not_change_the_selected_comma_parser() {
    let (_root, result) = sample(
        "events.csv",
        "name\tvalue\nalpha\tone\nbeta\ttwo\n",
        ImportOptions::default(),
    );
    assert_eq!(result.suggested_delimiter.as_deref(), Some("\t"));
    assert_eq!(result.columns, ["name\tvalue"]);
    assert_eq!(result.rows[0], [Some("alpha\tone".into())]);
    assert!(!result.truncated);
}

#[test]
fn quoted_delimiters_and_multiline_fields_are_not_used_as_candidate_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("quoted.csv");
    fs::write(
        &path,
        "name;value\n\"alpha;inside\";\"one\ntwo\"\nbeta;\"three;four\"\n",
    )
    .unwrap();
    let mut args = payload(&path, ImportOptions::default());
    args.delimiter = Some(";".into());
    let result = preview_import(args).unwrap();
    assert_eq!(result.suggested_delimiter, None);
    assert_eq!(result.columns, ["name", "value"]);
    assert_eq!(
        result.rows[0],
        [Some("alpha;inside".into()), Some("one\ntwo".into())]
    );
    assert!(!result.truncated);
}

#[test]
fn headerless_first_row_is_data_and_empty_cells_remain_distinct_from_missing() {
    let (_root, result) = sample(
        "headerless.csv",
        "00123,\n00456\n",
        ImportOptions {
            has_headers: false,
            ..Default::default()
        },
    );
    assert_eq!(result.columns, ["column 1", "column 2"]);
    assert_eq!(
        result.rows,
        [
            vec![Some("00123".into()), Some("".into())],
            vec![Some("00456".into()), None]
        ]
    );
    assert!(
        result
            .warnings
            .iter()
            .any(|warning| warning.contains("field counts")),
        "headerless unequal records must also warn that strict import rejects them"
    );
    let (_root, result) = sample(
        "ragged.csv",
        "name,value\nfirst,\nsecond\n",
        ImportOptions::default(),
    );
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning.contains("field counts")));
    assert_eq!(result.rows[1][1], None);
}

#[test]
fn an_unclosed_csv_tail_is_not_fabricated_as_a_complete_record() {
    let (_root, result) = sample(
        "unclosed.csv",
        "name,value\nfirst,complete\nbroken,\"incomplete\nstill part of this record\n",
        ImportOptions::default(),
    );
    assert_eq!(
        result.rows,
        [vec![Some("first".into()), Some("complete".into())]]
    );
    assert!(result.truncated);
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning.contains("incomplete CSV record")));
}

#[test]
fn source_prefix_and_display_limits_do_not_scan_or_return_the_remaining_data() {
    let mut source = b"name,value\nfirst,complete\nsecond,\"".to_vec();
    source.extend(vec![b'x'; MAX_PREVIEW_BYTES * 2]);
    source.extend_from_slice(b"\"\nlast,unread\n");
    let (_root, result) = sample("large.csv", &source, ImportOptions::default());
    assert_eq!(
        result.rows,
        [vec![Some("first".into()), Some("complete".into())]]
    );
    assert!(result.truncated);
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning.contains("bounded source sample")));

    let mut source = String::new();
    for index in 0..25 {
        source.push_str(&format!("h{index}{}", if index == 24 { '\n' } else { ',' }));
    }
    for _ in 0..25 {
        source.push_str(&(vec!["長".repeat(300); 25].join(",") + "\n"));
    }
    let (_root, result) = sample("wide.csv", source, ImportOptions::default());
    assert!(result.columns.len() <= 20);
    assert!(result.rows.len() <= 20);
    assert!(result
        .rows
        .iter()
        .flatten()
        .flatten()
        .all(|value| value.chars().count() <= 200));
    assert!(
        result
            .rows
            .iter()
            .flatten()
            .flatten()
            .map(|value| value.chars().count())
            .sum::<usize>()
            <= 20_000
    );
    assert!(result.truncated);
}

#[test]
fn bom_unicode_and_explicit_legacy_encoding_preserve_values_without_replacement() {
    let mut bytes = vec![0xff, 0xfe];
    bytes.extend(
        "name,value\n日本語,😀\n"
            .encode_utf16()
            .flat_map(u16::to_le_bytes),
    );
    let (_root, result) = sample("utf16.csv", bytes, ImportOptions::default());
    assert_eq!(result.encoding, "utf-16le");
    assert!(result.bom);
    assert_eq!(result.rows[0], [Some("日本語".into()), Some("😀".into())]);

    let bytes = b"value\nPrice \x80\n";
    let (_root, automatic) = sample("legacy.csv", bytes, ImportOptions::default());
    assert!(automatic
        .warnings
        .iter()
        .any(|warning| warning.contains("Invalid UTF-8")));
    assert!(automatic.rows.is_empty());
    let (_root, explicit) = sample(
        "legacy.csv",
        bytes,
        ImportOptions {
            encoding: TextEncoding::Windows1252,
            ..Default::default()
        },
    );
    assert_eq!(explicit.rows[0], [Some("Price €".into())]);
    assert_eq!(explicit.encoding, "windows-1252");
    assert!(!explicit.truncated);
}

#[test]
fn a_literal_bom_after_the_encoding_marker_is_not_stripped_again() {
    let (_root, result) = sample(
        "bom.csv",
        "\u{feff}\u{feff}name\nvalue\n",
        ImportOptions::default(),
    );
    assert_eq!(result.columns, ["\u{feff}name"]);
    assert_eq!(result.rows[0], [Some("value".into())]);
}

#[test]
fn json_preview_uses_source_pointers_and_exact_number_text() {
    let (_root, result) = sample(
        "mixed.json",
        r#"[{"payload":"legacy","n":9007199254740993},{"payload":{"url":"https://example.test"},"n":1e+400}]"#,
        ImportOptions::default(),
    );
    assert_eq!(result.columns, ["payload", "n", "payload/url"]);
    assert_eq!(
        result.rows[0],
        [Some("legacy".into()), Some("9007199254740993".into()), None]
    );
    assert_eq!(
        result.rows[1][0].as_deref(),
        Some(r#"{"url":"https://example.test"}"#)
    );
    assert_eq!(result.rows[1][1].as_deref(), Some("1e+400"));
}

#[test]
fn invalid_json_records_are_visible_as_warnings_and_recovery_is_explicit() {
    let bytes = "{\"name\":\"first\"}\n{\"name\":\"discarded\",\"name\":\"duplicate\"}\n{\"name\":\"last\"}\n";
    let (_root, strict) = sample("data.jsonl", bytes, ImportOptions::default());
    assert_eq!(strict.rows.len(), 1);
    assert!(strict
        .warnings
        .iter()
        .any(|warning| warning.contains("Duplicate JSON key")));
    let (_root, recovered) = sample(
        "data.jsonl",
        bytes,
        ImportOptions {
            recover: true,
            ..Default::default()
        },
    );
    assert_eq!(
        recovered.rows,
        [vec![Some("first".into())], vec![Some("last".into())]]
    );
    assert!(recovered
        .warnings
        .iter()
        .any(|warning| warning.contains("JSONL line 2")));

    let (_root, prefix) = sample(
        "incomplete.json",
        "[{\"name\":\"first\"},{\"name\":",
        ImportOptions::default(),
    );
    assert_eq!(prefix.rows, [vec![Some("first".into())]]);
    assert!(prefix.truncated);
    assert!(!prefix.warnings.is_empty());
}

#[test]
fn preview_honors_cancel_and_rejects_directories_without_creating_files() {
    let root = tempfile::tempdir().unwrap();
    assert!(preview_import(payload(root.path(), ImportOptions::default())).is_err());
    let guard = try_begin_job(JobKind::Load, None).unwrap();
    request_cancel(guard.id());
    let job = JobContext::with_job(guard, None);
    let error = preview_import_with_job(
        payload(
            &root.path().join("not-opened.csv"),
            ImportOptions::default(),
        ),
        &job,
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "Cancelled.");
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

use std::{fs, path::Path, sync::Arc};

use tempfile::tempdir;
use trivium_engine::{
    import::{options::TextEncoding, text_encoding::prepare_source},
    jobs::{request_cancel, try_begin_job, JobContext, JobKind, ProgressFn},
};

fn decoded_text(path: &Path) -> String {
    let bytes = fs::read(path).unwrap();
    assert!(
        bytes.starts_with(b"\xef\xbb\xbf"),
        "decoded copy has one parser BOM"
    );
    String::from_utf8(bytes[3..].to_vec()).unwrap()
}

fn encode_unicode(text: &str, encoding: TextEncoding, bom: bool) -> Vec<u8> {
    let mut bytes = match (encoding, bom) {
        (TextEncoding::Utf16Le, true) => vec![0xff, 0xfe],
        (TextEncoding::Utf16Be, true) => vec![0xfe, 0xff],
        (TextEncoding::Utf32Le, true) => vec![0xff, 0xfe, 0, 0],
        (TextEncoding::Utf32Be, true) => vec![0, 0, 0xfe, 0xff],
        (_, false) => vec![],
        _ => unreachable!(),
    };
    match encoding {
        TextEncoding::Utf16Le | TextEncoding::Utf16Be => {
            for unit in text.encode_utf16() {
                bytes.extend_from_slice(&if encoding == TextEncoding::Utf16Be {
                    unit.to_be_bytes()
                } else {
                    unit.to_le_bytes()
                });
            }
        }
        TextEncoding::Utf32Le | TextEncoding::Utf32Be => {
            for ch in text.chars() {
                bytes.extend_from_slice(&if encoding == TextEncoding::Utf32Be {
                    (ch as u32).to_be_bytes()
                } else {
                    (ch as u32).to_le_bytes()
                });
            }
        }
        _ => unreachable!(),
    }
    bytes
}

#[test]
fn unicode_boms_and_explicit_bomless_encodings_preserve_all_characters() {
    for encoding in [
        TextEncoding::Utf16Le,
        TextEncoding::Utf16Be,
        TextEncoding::Utf32Le,
        TextEncoding::Utf32Be,
    ] {
        for bom in [false, true] {
            // A leading U+FEFF itself encodes to a BOM. Test literal U+FEFF
            // after an actual marker, and ordinary text for BOM-less input.
            let original = if bom {
                "\u{feff}field,value\r\n名前,\"🕵️ café\nsecond line\"\r\n"
            } else {
                "field,value\r\n名前,\"🕵️ café\nsecond line\"\r\n"
            };
            let root = tempdir().unwrap();
            let source = root.path().join("source");
            let destination = root.path().join("decoded");
            let bytes = encode_unicode(original, encoding, bom);
            fs::write(&source, &bytes).unwrap();
            let requested = if bom { TextEncoding::Auto } else { encoding };
            let prepared =
                prepare_source(&source, &destination, requested, &JobContext::none()).unwrap();
            assert!(prepared.decoded);
            assert_eq!(prepared.path, destination);
            assert_eq!(prepared.bom, bom);
            assert_eq!(decoded_text(&prepared.path), original);
            assert_eq!(
                fs::read(source).unwrap(),
                bytes,
                "source must stay byte-identical"
            );
        }
    }
}

#[test]
fn utf8_is_passed_through_including_bom_and_invalid_record_bytes() {
    for bytes in [
        b"a,b\n1,2\n".as_slice(),
        b"\xef\xbb\xbfa,b\n",
        b"a,b\n1,\xff\n",
    ] {
        let root = tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("decoded");
        fs::write(&source, bytes).unwrap();
        let prepared = prepare_source(
            &source,
            &destination,
            TextEncoding::Auto,
            &JobContext::none(),
        )
        .unwrap();
        assert_eq!(prepared.path, source);
        assert_eq!(prepared.encoding, "utf-8");
        assert_eq!(prepared.bom, bytes.starts_with(b"\xef\xbb\xbf"));
        assert!(!prepared.decoded);
        assert!(!destination.exists());
        assert_eq!(fs::read(source).unwrap(), bytes);
    }
}

#[test]
fn explicit_legacy_encodings_decode_without_guessing() {
    for (encoding, bytes, expected) in [
        (
            TextEncoding::Windows1252,
            b"value\r\ncaf\xe9 \x80\r\n".as_slice(),
            "value\r\ncafé €\r\n",
        ),
        (
            TextEncoding::ShiftJis,
            b"value\n\x93\xfa\x96\x7b\n".as_slice(),
            "value\n日本\n",
        ),
    ] {
        let root = tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("decoded");
        fs::write(&source, bytes).unwrap();
        let auto = prepare_source(
            &source,
            &destination,
            TextEncoding::Auto,
            &JobContext::none(),
        )
        .unwrap();
        assert_eq!(auto.path, source, "Auto must not guess a legacy encoding");
        assert!(!destination.exists());
        let prepared =
            prepare_source(&source, &destination, encoding, &JobContext::none()).unwrap();
        assert_eq!(decoded_text(&prepared.path), expected);
        assert_eq!(fs::read(source).unwrap(), bytes);
    }
}

#[test]
fn multibyte_characters_can_cross_a_decode_buffer_boundary() {
    for encoding in [
        TextEncoding::Utf16Le,
        TextEncoding::Utf16Be,
        TextEncoding::ShiftJis,
    ] {
        let root = tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("decoded");
        let (bytes, expected) = if encoding == TextEncoding::ShiftJis {
            let mut bytes = vec![b'a'; 65_535];
            bytes.extend_from_slice(b"\x93\xfa\x96\x7b\r\n");
            (bytes, format!("{}日本\r\n", "a".repeat(65_535)))
        } else {
            let text = format!("{}🕵\r\n", "a".repeat(32_767));
            (encode_unicode(&text, encoding, false), text)
        };
        fs::write(&source, bytes).unwrap();
        let prepared =
            prepare_source(&source, &destination, encoding, &JobContext::none()).unwrap();
        assert_eq!(decoded_text(&prepared.path), expected);
    }
}

#[test]
fn invalid_unicode_and_shift_jis_fail_without_leaving_a_partial_copy() {
    for (encoding, invalid) in [
        (TextEncoding::Utf16Le, vec![b'a', 0, 0]), // odd trailing byte
        (TextEncoding::Utf16Be, vec![0, b'a', 0xd8, 0]), // lone high surrogate
        (TextEncoding::Utf16Le, vec![0, 0xdc]),    // lone low surrogate
        (TextEncoding::Utf32Le, vec![0, 0xd8, 0, 0]), // surrogate scalar
        (TextEncoding::Utf32Be, vec![0, 0x11, 0, 0]), // beyond Unicode
        (TextEncoding::Utf32Le, vec![b'a', 0, 0]), // truncated scalar
        (TextEncoding::ShiftJis, vec![b'a', 0x82]), // dangling lead byte
        (TextEncoding::ShiftJis, vec![0x82, 0x20]), // invalid trail byte
    ] {
        let root = tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("decoded");
        fs::write(&source, &invalid).unwrap();
        let error = prepare_source(&source, &destination, encoding, &JobContext::none())
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("No replacement characters"),
            "{error}"
        );
        assert!(
            !destination.exists(),
            "failed decoding must clean up its partial file"
        );
        assert_eq!(fs::read(source).unwrap(), invalid);
    }
}

#[test]
fn explicit_encoding_conflicts_do_not_override_a_unicode_bom() {
    let root = tempdir().unwrap();
    let source = root.path().join("source");
    let destination = root.path().join("decoded");
    fs::write(&source, encode_unicode("日本", TextEncoding::Utf16Be, true)).unwrap();
    for encoding in [
        TextEncoding::Utf8,
        TextEncoding::Utf16Le,
        TextEncoding::ShiftJis,
    ] {
        let error = prepare_source(&source, &destination, encoding, &JobContext::none())
            .err()
            .unwrap();
        assert!(error.to_string().contains("conflicts"), "{error}");
        assert!(!destination.exists());
    }
    let prepared = prepare_source(
        &source,
        &destination,
        TextEncoding::Utf16Be,
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(decoded_text(&prepared.path), "日本");
}

#[test]
fn existing_destination_is_never_overwritten_or_removed() {
    let root = tempdir().unwrap();
    let source = root.path().join("source");
    let destination = root.path().join("decoded");
    fs::write(&source, [b'a', 0]).unwrap();
    fs::write(&destination, "existing data").unwrap();
    assert!(prepare_source(
        &source,
        &destination,
        TextEncoding::Utf16Le,
        &JobContext::none()
    )
    .is_err());
    assert_eq!(fs::read_to_string(destination).unwrap(), "existing data");
}

#[test]
fn cancellation_during_decoding_removes_its_partial_copy() {
    let root = tempdir().unwrap();
    let source = root.path().join("source");
    let destination = root.path().join("decoded");
    let bytes = encode_unicode(&"a".repeat(100_000), TextEncoding::Utf16Le, true);
    fs::write(&source, &bytes).unwrap();
    let guard = try_begin_job(JobKind::Import, None).unwrap();
    let id = guard.id();
    let emit: ProgressFn = Arc::new(move |progress| {
        if progress.message.as_deref() == Some("Decoding source") {
            request_cancel(id);
        }
    });
    let job = JobContext::with_job(guard, Some(emit));
    let error = prepare_source(&source, &destination, TextEncoding::Auto, &job)
        .err()
        .unwrap();
    assert_eq!(error.to_string(), "Cancelled.");
    assert!(!destination.exists());
    assert_eq!(fs::read(source).unwrap(), bytes);
}

#[test]
fn csv_pipeline_preserves_a_literal_bom_in_headers_and_headerless_first_cells() {
    use sha2::{Digest, Sha256};
    use trivium_engine::{
        import::options::ImportOptions,
        ops::projects::{create_project, load_project, CreateProjectPayload, ProjectRequest},
        store::ProjectsStore,
    };

    for encoding in [
        TextEncoding::Utf8,
        TextEncoding::Utf16Le,
        TextEncoding::Utf16Be,
        TextEncoding::Utf32Le,
        TextEncoding::Utf32Be,
    ] {
        for has_headers in [true, false] {
            let root = tempdir().unwrap();
            let source = root.path().join("source.csv");
            let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
            let text = if has_headers {
                "\u{feff}name,value\n\u{feff}entry,00123\n"
            } else {
                "\u{feff}entry,00123\n"
            };
            let bytes = if encoding == TextEncoding::Utf8 {
                let mut bytes = b"\xef\xbb\xbf".to_vec();
                bytes.extend_from_slice(text.as_bytes());
                bytes
            } else {
                encode_unicode(text, encoding, true)
            };
            fs::write(&source, &bytes).unwrap();
            let summary = create_project(
                &store,
                CreateProjectPayload {
                    import_options: Some(ImportOptions {
                        has_headers,
                        ..ImportOptions::default()
                    }),
                    path: source.to_string_lossy().to_string(),
                    description: None,
                    delimiter: None,
                    restore_annotations: None,
                },
            )
            .unwrap();
            assert_eq!(summary.meta.total_records, 1);
            if has_headers {
                assert_eq!(summary.meta.columns[0].source, "\u{feff}name");
                assert_eq!(summary.meta.columns[0].label, "\u{feff}name");
            }
            let loaded = load_project(
                &store,
                ProjectRequest {
                    project_id: summary.meta.id,
                },
            )
            .unwrap();
            let row = &loaded.initial_rows[0];
            assert_eq!(
                row.cells[&summary.meta.columns[0].id].text.as_deref(),
                Some("\u{feff}entry")
            );
            assert_eq!(
                row.cells[&summary.meta.columns[1].id].text.as_deref(),
                Some("00123")
            );
            assert_eq!(
                summary.meta.source_sha256.as_deref(),
                Some(format!("{:x}", Sha256::digest(&bytes)).as_str())
            );
            assert_eq!(fs::read(source).unwrap(), bytes);
        }
    }
}

#[test]
fn json_pipeline_does_not_hide_a_second_bom_in_the_actual_payload() {
    use trivium_engine::{
        ops::projects::{create_project, CreateProjectPayload},
        store::ProjectsStore,
    };
    let root = tempdir().unwrap();
    let source = root.path().join("source.json");
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let bytes = encode_unicode("\u{feff}{\"value\":1}", TextEncoding::Utf16Le, true);
    fs::write(&source, &bytes).unwrap();
    let result = create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: source.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    );
    assert!(
        result.is_err(),
        "the second U+FEFF is data, not another marker to discard"
    );
    assert!(store.try_all().unwrap().is_empty());
    assert_eq!(fs::read(source).unwrap(), bytes);
}

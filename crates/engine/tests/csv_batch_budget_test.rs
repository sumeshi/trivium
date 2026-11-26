//! CSV bounds include retained cell slots and preserve its annotation side channel.
use std::{fs, path::Path};

use trivium_engine::{
    import::csv::{BatchLimits, CsvImporter, MAX_CSV_COLUMNS},
    jobs::JobContext,
    ops::{
        export::{export_project, ExportProjectPayload},
        flags::{update_flag, UpdateFlagPayload},
        projects::{create_project, CreateProjectPayload},
        rows::QuerySpec,
    },
    project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD},
    storage::load_flags,
    store::ProjectsStore,
};

fn payload(path: &Path, restore: bool) -> CreateProjectPayload {
    CreateProjectPayload {
        import_options: None,
        path: path.to_string_lossy().into(),
        description: None,
        delimiter: None,
        restore_annotations: Some(restore),
    }
}

fn csv_bytes(headers: &[String], rows: &[Vec<String>]) -> Vec<u8> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(headers).unwrap();
    for row in rows {
        writer.write_record(row).unwrap();
    }
    writer.into_inner().unwrap()
}

fn utf16(text: &str, big_endian: bool) -> Vec<u8> {
    let mut bytes = if big_endian {
        vec![0xFE, 0xFF]
    } else {
        vec![0xFF, 0xFE]
    };
    for unit in text.encode_utf16() {
        bytes.extend(if big_endian {
            unit.to_be_bytes()
        } else {
            unit.to_le_bytes()
        });
    }
    bytes
}

#[test]
fn excessive_csv_headers_fail_before_publication_and_leave_evidence_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("too-wide.csv");
    let headers = (0..=MAX_CSV_COLUMNS)
        .map(|index| format!("c{index}"))
        .collect::<Vec<_>>();
    let body = csv_bytes(&headers, &[]);
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    for bytes in [
        body.clone(),
        utf16(std::str::from_utf8(&body).unwrap(), false),
        utf16(std::str::from_utf8(&body).unwrap(), true),
    ] {
        fs::write(&path, &bytes).unwrap();
        let error = create_project(&store, payload(&path, false))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("4097 data columns") && error.contains("4096"),
            "{error}"
        );
        assert!(error.contains("No columns were discarded"), "{error}");
        assert!(store.try_all().unwrap().is_empty());
        assert!(fs::read_dir(store.staging_root()).unwrap().next().is_none());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    fs::write(&path, "value\n00123\n").unwrap();
    assert_eq!(
        create_project(&store, payload(&path, false))
            .unwrap()
            .meta
            .total_records,
        1
    );
}

#[test]
fn exact_column_boundary_preserves_blank_duplicate_and_utf16_headers() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("boundary.csv");
    let mut headers = (0..MAX_CSV_COLUMNS)
        .map(|index| format!("c{index}"))
        .collect::<Vec<_>>();
    headers[0].clear();
    headers[1] = "duplicate".into();
    headers[2] = "duplicate".into();
    headers[MAX_CSV_COLUMNS - 1] = "調査😀".into();
    let mut values = vec![String::new(); MAX_CSV_COLUMNS];
    values[0] = "00123".into();
    values[MAX_CSV_COLUMNS - 1] = "last, \"quoted\"\n値".into();
    let body = csv_bytes(&headers, &[values.clone()]);
    for bytes in [
        body.clone(),
        utf16(std::str::from_utf8(&body).unwrap(), false),
        utf16(std::str::from_utf8(&body).unwrap(), true),
    ] {
        fs::write(&path, bytes).unwrap();
        let mut reader = CsvImporter::open(&path, None, BatchLimits::default()).unwrap();
        let columns = reader.columns().to_vec();
        assert_eq!(columns.len(), MAX_CSV_COLUMNS);
        assert_eq!(columns[0].source, "");
        assert_eq!(columns[0].label, "column 1");
        assert_eq!(columns[1].source, columns[2].source);
        assert_ne!(columns[1].id, columns[2].id);
        assert_eq!(columns.last().unwrap().source, "調査😀");
        let frame = reader.next_batch(&JobContext::none()).unwrap().unwrap();
        for index in [0, MAX_CSV_COLUMNS - 1] {
            assert_eq!(
                frame
                    .column(&columns[index].id)
                    .unwrap()
                    .str()
                    .unwrap()
                    .get(0),
                Some(values[index].as_str())
            );
        }
        assert!(reader.next_batch(&JobContext::none()).unwrap().is_none());
    }
}

#[test]
fn maximum_width_annotated_exports_open_normally_and_restore_annotations() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source.csv");
    let mut headers = (0..MAX_CSV_COLUMNS)
        .map(|index| format!("c{index}"))
        .collect::<Vec<_>>();
    headers[0] = "trivium-safe".into();
    let mut values = vec![String::new(); MAX_CSV_COLUMNS];
    values[0] = "00123".into();
    values[MAX_CSV_COLUMNS - 1] = "last source value".into();
    fs::write(&path, csv_bytes(&headers, &[values])).unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let id = create_project(&store, payload(&path, false))
        .unwrap()
        .meta
        .id;
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: Some("critical".into()),
            memo: Some(Some("<alice@example.test>\nexact note".into())),
            tags: Some(vec!["review".into()]),
            bookmarked: Some(true),
        },
    )
    .unwrap();
    let output = root.path().join("export.csv");
    export_project(
        &store,
        ExportProjectPayload {
            project_id: id,
            destination: output.to_string_lossy().into(),
            allow_overwrite: false,
            scope: Some("all".into()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: Some(true),
        },
    )
    .unwrap();
    let mut reader = CsvImporter::open(&output, None, BatchLimits::default()).unwrap();
    assert_eq!(reader.columns().len(), MAX_CSV_COLUMNS + 6);
    assert_eq!(reader.columns()[0].source, "trivium-safe_2");
    let first_source = reader.columns()[6].id.clone();
    let last = reader.columns().last().unwrap().id.clone();
    let frame = reader.next_batch(&JobContext::none()).unwrap().unwrap();
    assert_eq!(
        frame.column(&last).unwrap().str().unwrap().get(0),
        Some("last source value")
    );
    assert_eq!(
        frame.column(&first_source).unwrap().str().unwrap().get(0),
        Some("00123")
    );
    for restore in [false, true] {
        let restored = create_project(&store, payload(&output, restore)).unwrap();
        assert_eq!(restored.meta.total_records, 1);
        let flags = load_flags(&store.flags_path(&restored.meta.id)).unwrap();
        if restore {
            assert_eq!(flags[&0].flag, "critical");
            assert_eq!(
                flags[&0].memo.as_deref(),
                Some("<alice@example.test>\nexact note")
            );
            assert_eq!(flags[&0].tags, ["review"]);
            assert!(flags[&0].bookmarked);
        } else {
            assert!(flags.is_empty());
        }
    }
    // Merely resembling part of the side channel cannot bypass the limit.
    let mut headers = csv::Reader::from_path(&output)
        .unwrap()
        .headers()
        .unwrap()
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    headers[0] = "ordinary data".into();
    fs::write(&path, csv_bytes(&headers, &[])).unwrap();
    assert!(CsvImporter::open(&path, None, BatchLimits::default()).is_err());
    headers[0] = "trivium-safe_2".into();
    headers.push("overflow".into());
    fs::write(&path, csv_bytes(&headers, &[])).unwrap();
    let error = CsvImporter::open(&path, None, BatchLimits::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("4097 data columns"), "{error}");
}

#[test]
fn wide_empty_batches_count_cell_slots_and_preserve_originals_and_row_positions() {
    const COLUMNS: usize = 1_000;
    const ROWS: usize = 80;
    const BUDGET: usize = 128 * 1024;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("empty-wide.csv");
    let headers = (0..COLUMNS)
        .map(|index| format!("c{index}"))
        .collect::<Vec<_>>();
    let rows = (0..ROWS)
        .map(|index| {
            let mut row = vec![String::new(); COLUMNS];
            row[0] = format!("{index:05}");
            row[COLUMNS - 1] = format!("last {index}");
            row
        })
        .collect::<Vec<_>>();
    fs::write(&path, csv_bytes(&headers, &rows)).unwrap();
    let mut reader = CsvImporter::open(
        &path,
        None,
        BatchLimits {
            max_rows: 50_000,
            max_bytes: BUDGET,
            read_buffer: 17,
        },
    )
    .unwrap();
    let columns = reader.columns().to_vec();
    let mut position = 0;
    let mut batches = 0;
    while let Some(frame) = reader.next_batch(&JobContext::none()).unwrap() {
        batches += 1;
        assert!(frame.height() * COLUMNS * std::mem::size_of::<String>() <= BUDGET);
        for local in 0..frame.height() {
            let row = position + local;
            assert_eq!(
                frame
                    .column(INTERNAL_ROW_ID)
                    .unwrap()
                    .i64()
                    .unwrap()
                    .get(local),
                Some(row as i64)
            );
            assert_eq!(
                frame
                    .column(INTERNAL_SOURCE_RECORD)
                    .unwrap()
                    .i64()
                    .unwrap()
                    .get(local),
                Some(row as i64 + 2)
            );
            let original = frame
                .column(INTERNAL_ORIGINAL)
                .unwrap()
                .str()
                .unwrap()
                .get(local)
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Vec<String>>(original).unwrap(),
                rows[row]
            );
            for index in [0, COLUMNS - 1] {
                assert_eq!(
                    frame
                        .column(&columns[index].id)
                        .unwrap()
                        .str()
                        .unwrap()
                        .get(local),
                    Some(rows[row][index].as_str())
                );
            }
        }
        position += frame.height();
    }
    assert_eq!(position, ROWS);
    assert!(
        batches > 20,
        "cell slots must bound the batch before payload alone would"
    );
    assert_eq!(reader.into_summary().rows, ROWS);
}

#[test]
fn preflush_keeps_multiline_annotation_records_and_the_oversized_row_contract() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("annotations.csv");
    let headers = [
        "trivium-safe",
        "trivium-suspicious",
        "trivium-critical",
        "trivium-memo",
        "trivium-tags-json",
        "trivium-bookmarked",
        "value",
    ]
    .map(str::to_owned);
    let rows = vec![
        vec!["0", "0", "1", "first\nsecond", "[\"review\"]", "1", "00123"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        vec!["1", "0", "0", "kept", "[]", "0", "last"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
    ];
    fs::write(&path, csv_bytes(&headers, &rows)).unwrap();
    let mut reader = CsvImporter::open(
        &path,
        None,
        BatchLimits {
            max_rows: 50_000,
            max_bytes: 1,
            read_buffer: 3,
        },
    )
    .unwrap()
    .with_annotations(true);
    for (index, physical_line) in [2, 4].into_iter().enumerate() {
        let frame = reader.next_batch(&JobContext::none()).unwrap().unwrap();
        assert_eq!(frame.height(), 1, "one oversized row is still readable");
        assert_eq!(
            frame.column(INTERNAL_ROW_ID).unwrap().i64().unwrap().get(0),
            Some(index as i64)
        );
        assert_eq!(
            frame
                .column(INTERNAL_SOURCE_RECORD)
                .unwrap()
                .i64()
                .unwrap()
                .get(0),
            Some(physical_line)
        );
    }
    assert!(reader.next_batch(&JobContext::none()).unwrap().is_none());
    let summary = reader.into_summary();
    assert_eq!(summary.rows, 2);
    assert_eq!(summary.flags[&0].flag, "critical");
    assert_eq!(summary.flags[&0].memo.as_deref(), Some("first\nsecond"));
    assert_eq!(summary.flags[&0].tags, ["review"]);
    assert_eq!(summary.flags[&1].flag, "safe");
}

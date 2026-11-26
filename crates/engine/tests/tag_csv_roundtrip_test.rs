//! Annotation CSV uses an explicit JSON-array column for exact tag roundtrips.

use std::fs;
use std::path::{Path, PathBuf};

use trivium_engine::ops::{
    export::{export_project, ExportProjectPayload},
    flags::{update_flag, UpdateFlagPayload},
    projects::{create_project, CreateProjectPayload},
    rows::{query_project_rows, QueryRowsPayload, QuerySpec},
};
use trivium_engine::storage::load_flags;
use trivium_engine::store::ProjectsStore;
use uuid::Uuid;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("trivium-tag-csv-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn import(store: &ProjectsStore, path: &Path, restore: bool) -> Result<Uuid, String> {
    create_project(
        store,
        CreateProjectPayload {
            import_options: None,
            path: path.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: Some(restore),
        },
    )
    .map(|project| project.meta.id)
    .map_err(|error| error.to_string())
}

fn write_csv(path: &Path, headers: &[&str], records: &[Vec<&str>]) {
    let mut writer = csv::Writer::from_path(path).unwrap();
    writer.write_record(headers).unwrap();
    for record in records {
        writer.write_record(record).unwrap();
    }
    writer.flush().unwrap();
}

#[test]
fn special_tag_names_survive_export_restore_and_exact_tag_search() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.0.join("store")).unwrap();
    let path = fixture.0.join("source.csv");
    fs::write(&path, "id,message\n0,first\n1,last\n").unwrap();
    let id = import(&store, &path, false).unwrap();
    let tags = vec![
        "host;admin",
        r#"["review"]"#,
        " padded ",
        "quote\" and\nnewline",
        "調査対象",
        "",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: id,
            row_index: 0,
            flag: Some("critical".into()),
            memo: Some(Some("original note".into())),
            tags: Some(tags.clone()),
            bookmarked: Some(true),
        },
    )
    .unwrap();
    let destination = fixture.0.join("export.csv");
    export_project(
        &store,
        ExportProjectPayload {
            allow_overwrite: false,
            project_id: id,
            destination: destination.to_string_lossy().into(),
            scope: Some("all".into()),
            spec: QuerySpec::default(),
            columns: None,
            row_indices: None,
            include_annotations: Some(true),
        },
    )
    .unwrap();
    let mut csv = csv::Reader::from_path(&destination).unwrap();
    let headers = csv.headers().unwrap().clone();
    assert!(!headers.iter().any(|header| header == "trivium-tags"));
    let index = headers
        .iter()
        .position(|header| header == "trivium-tags-json")
        .unwrap();
    let exported = csv.records().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        serde_json::from_str::<Vec<String>>(&exported[0][index]).unwrap(),
        tags
    );
    assert_eq!(&exported[1][index], "[]");

    let restored = import(&store, &destination, true).unwrap();
    let annotations = load_flags(&store.database_path(&restored)).unwrap();
    let entry = annotations.get(&0).unwrap();
    assert_eq!(entry.tags, tags);
    assert_eq!(entry.flag, "critical");
    assert_eq!(entry.memo.as_deref(), Some("original note"));
    assert!(entry.bookmarked);
    assert!(!annotations.contains_key(&1));

    for (search, expected) in [(r#"@tag:"host;admin""#, 1), ("@tag:host", 0)] {
        let results = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: restored,
                spec: QuerySpec {
                    search: Some(search.into()),
                    ..Default::default()
                },
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        assert_eq!(results.total_filtered_rows, expected, "{search}");
    }
}

#[test]
fn only_json_tag_columns_restore_annotations_and_other_source_columns_remain_data() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.0.join("store")).unwrap();
    let legacy = fixture.0.join("legacy.csv");
    write_csv(
        &legacy,
        &["id", "trivium-tags"],
        &[vec!["0", "old;other"], vec!["1", r#"["one exact tag"]"#]],
    );
    let id = import(&store, &legacy, true).unwrap();
    let entries = load_flags(&store.database_path(&id)).unwrap();
    assert!(entries.is_empty());
    let rows = query_project_rows(
        &store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .unwrap();
    let tag_column = rows.rows[0]
        .cells
        .iter()
        .find(|(_, cell)| cell.text.as_deref() == Some("old;other"))
        .map(|(column, _)| column)
        .unwrap();
    assert_eq!(
        rows.rows[1].cells[tag_column].text.as_deref(),
        Some(r#"["one exact tag"]"#)
    );

    let mixed = fixture.0.join("mixed.csv");
    write_csv(
        &mixed,
        &["id", "trivium-tags", "trivium-tags-json"],
        &[
            vec!["0", "old;ignored", r#"["new;exact"," padded "]"#],
            vec!["1", "old;ignored", "[]"],
        ],
    );
    let id = import(&store, &mixed, true).unwrap();
    let entries = load_flags(&store.database_path(&id)).unwrap();
    assert_eq!(entries[&0].tags, ["new;exact", " padded "]);
    assert!(
        !entries.contains_key(&1),
        "[] restores no tags"
    );
}

#[test]
fn invalid_explicit_tag_json_rejects_restore_with_record_number_and_no_partial_case() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.0.join("store")).unwrap();
    for text in ["", "not json", "[", "[1]", "null", "{}", r#""one tag""#] {
        let source = fixture.0.join("invalid.csv");
        write_csv(
            &source,
            &["id", "trivium-tags-json"],
            &[vec!["0", r#"["valid"]"#], vec!["1", text]],
        );
        let before = fs::read(&source).unwrap();
        let error = import(&store, &source, true).unwrap_err();
        assert!(error.contains("CSV record 2 (line 3)"), "{text:?}: {error}");
        assert!(error.contains("trivium-tags-json"), "{text:?}: {error}");
        assert!(store.try_all().unwrap().is_empty());
        assert_eq!(fs::read_dir(store.staging_root()).unwrap().count(), 0);
        assert_eq!(fs::read(&source).unwrap(), before);
    }
    let source = fixture.0.join("invalid.csv");
    assert!(
        import(&store, &source, false).is_ok(),
        "ordinary data import must not interpret annotation columns"
    );
}

#[test]
fn repeated_exports_restore_latest_annotations_despite_canonical_and_suffixed_source_columns() {
    let canonical = [
        "trivium-safe",
        "trivium-suspicious",
        "trivium-critical",
        "trivium-memo",
        "trivium-tags-json",
        "trivium-bookmarked",
    ];
    for existing_suffix in [false, true] {
        let fixture = Fixture::new();
        let store = ProjectsStore::new(fixture.0.join("store")).unwrap();
        let source = fixture.0.join("source.csv");
        let mut headers = vec!["id".to_owned()];
        headers.extend(canonical.map(str::to_owned));
        let source_annotations = ["0", "0", "1", "source note", r#"["source tag"]"#, "1"];
        let mut first_row = vec!["0".to_owned()];
        first_row.extend(source_annotations.map(str::to_owned));
        if existing_suffix {
            headers.extend(canonical.map(|name| format!("{name}_2")));
            first_row.extend(source_annotations.map(str::to_owned));
        }
        let mut second_row = first_row.clone();
        second_row[0] = "1".to_owned();
        write_csv(
            &source,
            &headers.iter().map(String::as_str).collect::<Vec<_>>(),
            &[
                first_row.iter().map(String::as_str).collect(),
                second_row.iter().map(String::as_str).collect(),
            ],
        );
        let initial = import(&store, &source, false).unwrap();
        for row in [0, 1] {
            update_flag(
                &store,
                UpdateFlagPayload {
                    project_id: initial,
                    row_index: row,
                    flag: Some("safe".into()),
                    memo: Some(Some("first investigation note".into())),
                    tags: Some(vec!["first investigation tag".into()]),
                    bookmarked: Some(true),
                },
            )
            .unwrap();
        }
        let mut current = initial;
        for round in [1, 2] {
            let destination = fixture.0.join(format!("round-{round}.csv"));
            export_project(
                &store,
                ExportProjectPayload {
                    project_id: current,
                    destination: destination.to_string_lossy().into(),
                    allow_overwrite: false,
                    scope: Some("all".into()),
                    spec: QuerySpec::default(),
                    columns: None,
                    row_indices: None,
                    include_annotations: Some(true),
                },
            )
            .unwrap();
            let mut csv = csv::Reader::from_path(&destination).unwrap();
            let exported_headers = csv.headers().unwrap().clone();
            let suffix = round + if existing_suffix { 2 } else { 1 };
            assert_eq!(&exported_headers[4], format!("trivium-tags-json_{suffix}"));
            let source_note = exported_headers
                .iter()
                .position(|name| name == "trivium-memo")
                .unwrap();
            let exported = csv.records().collect::<Result<Vec<_>, _>>().unwrap();
            assert!(
                exported
                    .iter()
                    .all(|row| &row[source_note] == "source note"),
                "source data remains unchanged"
            );

            current = import(&store, &destination, true).unwrap();
            let restored = load_flags(&store.database_path(&current)).unwrap();
            if round == 1 {
                assert_eq!(restored[&0].flag, "safe");
                assert_eq!(
                    restored[&0].memo.as_deref(),
                    Some("first investigation note")
                );
                assert_eq!(restored[&0].tags, ["first investigation tag"]);
                assert!(restored[&0].bookmarked);
                update_flag(
                    &store,
                    UpdateFlagPayload {
                        project_id: current,
                        row_index: 0,
                        flag: Some("critical".into()),
                        memo: Some(Some("latest <note>\n<alice@example.org>".into())),
                        tags: Some(vec![
                            "latest;exact".into(),
                            r#"["literal"]"#.into(),
                            " padded ".into(),
                        ]),
                        bookmarked: Some(false),
                    },
                )
                .unwrap();
                update_flag(
                    &store,
                    UpdateFlagPayload {
                        project_id: current,
                        row_index: 1,
                        flag: Some(String::new()),
                        memo: Some(None),
                        tags: Some(Vec::new()),
                        bookmarked: Some(false),
                    },
                )
                .unwrap();
            } else {
                assert_eq!(restored[&0].flag, "critical");
                assert_eq!(
                    restored[&0].memo.as_deref(),
                    Some("latest <note>\n<alice@example.org>")
                );
                assert_eq!(
                    restored[&0].tags,
                    ["latest;exact", r#"["literal"]"#, " padded "]
                );
                assert!(!restored[&0].bookmarked);
                assert!(
                    !restored.contains_key(&1),
                    "cleared annotations must not be resurrected from older data columns"
                );
            }
        }
    }
}

#[test]
fn partial_annotation_blocks_do_not_guess_suffixed_source_columns() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.0.join("store")).unwrap();
    for malformed in [
        "missing-last-column",
        "invalid-plus-suffix",
        "invalid-leading-zero",
        "nonleading-block",
    ] {
        let path = fixture.0.join(format!("{malformed}.csv"));
        let mut headers = vec![
            "trivium-safe_2",
            "trivium-suspicious_2",
            "trivium-critical_2",
            "trivium-memo_2",
            "trivium-tags-json_2",
            "trivium-bookmarked_2",
        ];
        let mut record = vec![
            "1",
            "0",
            "0",
            "unselected alias note",
            r#"["unselected alias tag"]"#,
            "1",
        ];
        match malformed {
            "missing-last-column" => {
                headers.pop();
                record.pop();
            }
            "invalid-plus-suffix" => headers[0] = "trivium-safe_+2",
            "invalid-leading-zero" => headers[0] = "trivium-safe_02",
            "nonleading-block" => {
                headers.insert(0, "id");
                record.insert(0, "0");
            }
            _ => unreachable!(),
        }
        headers.extend(["trivium-safe", "trivium-memo", "trivium-tags-json"]);
        record.extend(["1", "canonical note", r#"["canonical tag"]"#]);
        write_csv(&path, &headers, &[record]);
        let id = import(&store, &path, true).unwrap();
        let entries = load_flags(&store.database_path(&id)).unwrap();
        assert_eq!(entries[&0].flag, "safe");
        assert_eq!(
            entries[&0].memo.as_deref(),
            Some("canonical note"),
            "{malformed}"
        );
        assert_eq!(entries[&0].tags, ["canonical tag"], "{malformed}");
        assert!(!entries[&0].bookmarked, "{malformed}");
    }
}

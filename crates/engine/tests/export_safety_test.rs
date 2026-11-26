//! Output transactions protect evidence and all active case storage, including aliases.
use std::{fs, path::PathBuf};
use trivium_engine::{
    ops::{self, export::ExportProjectPayload, projects::CreateProjectPayload, rows::QuerySpec},
    store::ProjectsStore,
};
use uuid::Uuid;
struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn spreadsheet_formula_candidates_keep_source_values_and_notes_on_csv_roundtrip() {
    let root =
        Fixture(std::env::temp_dir().join(format!("trivium-csv-formulas-{}", Uuid::new_v4())));
    fs::create_dir_all(&root.0).unwrap();
    let source = root.0.join("source.csv");
    let values = [
        "=1+1",
        "+SUM(A1:A2)",
        "-42",
        "@SUM(1,2)",
        "\t=1+1",
        "\r=1+1",
    ];
    {
        let mut writer = csv::Writer::from_path(&source).unwrap();
        writer.write_record(["=source header"]).unwrap();
        for value in values {
            writer.write_record([value]).unwrap();
        }
        writer.flush().unwrap();
    }
    let source_bytes = fs::read(&source).unwrap();
    let store = ProjectsStore::new(root.0.clone()).unwrap();
    let project = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    for (row_index, value) in values.iter().enumerate() {
        ops::flags::update_flag(
            &store,
            ops::flags::UpdateFlagPayload {
                project_id: project.meta.id,
                row_index,
                flag: None,
                memo: Some(Some((*value).into())),
                tags: None,
                bookmarked: None,
            },
        )
        .unwrap();
    }
    for include_annotations in [false, true] {
        let destination = root.0.join(format!("export-{include_annotations}.csv"));
        ops::export::export_project(
            &store,
            ExportProjectPayload {
                allow_overwrite: false,
                project_id: project.meta.id,
                destination: destination.to_string_lossy().into(),
                scope: None,
                spec: QuerySpec::default(),
                columns: None,
                row_indices: None,
                include_annotations: Some(include_annotations),
            },
        )
        .unwrap();
        let mut reader = csv::Reader::from_path(&destination).unwrap();
        let header = reader.headers().unwrap().clone();
        let data = header
            .iter()
            .position(|value| value == "=source header")
            .unwrap();
        let memo = header.iter().position(|value| value == "trivium-memo");
        assert_eq!(memo.is_some(), include_annotations);
        let records: Vec<_> = reader.records().map(Result::unwrap).collect();
        assert_eq!(records.len(), values.len());
        for (record, expected) in records.iter().zip(values) {
            assert_eq!(&record[data], expected);
            if let Some(memo) = memo {
                assert_eq!(&record[memo], expected);
            }
        }
        let restored = ops::projects::create_project(
            &store,
            CreateProjectPayload {
                import_options: None,
                path: destination.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: Some(include_annotations),
            },
        )
        .unwrap();
        let source_column = &restored
            .meta
            .columns
            .iter()
            .find(|column| column.source == "=source header")
            .unwrap()
            .id;
        let page = ops::rows::query_project_rows(
            &store,
            ops::rows::QueryRowsPayload {
                project_id: restored.meta.id,
                spec: QuerySpec::default(),
                offset: None,
                limit: None,
            },
        )
        .unwrap();
        assert_eq!(page.rows.len(), values.len());
        for (row, expected) in page.rows.iter().zip(values) {
            assert_eq!(row.cells[source_column].text.as_deref(), Some(expected));
            if include_annotations {
                assert_eq!(row.memo.as_deref(), Some(expected));
            }
        }
    }
    assert_eq!(fs::read(&source).unwrap(), source_bytes);
}

#[test]
fn export_preserves_managed_files_and_aliases_and_supports_annotation_only_output() {
    let root =
        Fixture(std::env::temp_dir().join(format!("trivium-output-safety-{}", Uuid::new_v4())));
    fs::create_dir_all(&root.0).unwrap();
    let source = root.0.join("source.csv");
    fs::write(&source, b"seq,msg\n0,first\n1,last\n").unwrap();
    let store = ProjectsStore::new(root.0.clone()).unwrap();
    let create = || {
        ops::projects::create_project(
            &store,
            CreateProjectPayload {
                import_options: None,
                path: source.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id
    };
    let first = create();
    let other = create();
    let payload = |destination: PathBuf, columns: Option<Vec<String>>| ExportProjectPayload {
        allow_overwrite: false,
        project_id: first,
        destination: destination.to_string_lossy().into(),
        scope: None,
        spec: QuerySpec::default(),
        columns,
        row_indices: None,
        include_annotations: Some(true),
    };
    let other_data = store.project_dir(&other).join("data.parquet");
    let data_before = fs::read(&other_data).unwrap();
    for destination in [
        store.project_dir(&first).join("data.parquet"),
        other_data.clone(),
        store.project_dir(&first).join("project.sqlite"),
        store.project_dir(&first).join("project.sqlite-wal"),
        store.staging_root().join("future").join("new.csv"),
        store
            .project_dir(&first)
            .join("new")
            .join("..")
            .join("future.csv"),
    ] {
        let error = ops::export::export_project(&store, payload(destination, None)).unwrap_err();
        assert!(
            error.to_string().contains("managed case storage"),
            "{error}"
        );
    }
    assert_eq!(fs::read(&other_data).unwrap(), data_before);
    let hardlink = root.0.join("data-alias.csv");
    fs::hard_link(&other_data, &hardlink).unwrap();
    assert!(
        ops::export::export_project(&store, payload(hardlink.clone(), None))
            .unwrap_err()
            .to_string()
            .contains("managed case storage")
    );
    assert_eq!(fs::read(&hardlink).unwrap(), data_before);
    let evidence_alias = root.0.join("evidence-alias.csv");
    fs::hard_link(&source, &evidence_alias).unwrap();
    assert!(
        ops::export::export_project(&store, payload(evidence_alias, None))
            .unwrap_err()
            .to_string()
            .contains("Overwrite confirmation required:")
    );
    #[cfg(unix)]
    {
        let alias = root.0.join("case-alias");
        std::os::unix::fs::symlink(store.project_dir(&other), &alias).unwrap();
        assert!(ops::export::export_project(
            &store,
            payload(alias.join("new").join("out.csv"), None)
        )
        .unwrap_err()
        .to_string()
        .contains("managed case storage"));
    }
    let output = root.0.join("annotations.csv");
    fs::write(&output, b"previous destination").unwrap();
    let mut overwrite = payload(output.clone(), Some(Vec::new()));
    overwrite.allow_overwrite = true;
    let result = ops::export::export_project(&store, overwrite).unwrap();
    assert_eq!(result.rows, 2);
    assert_eq!(result.columns.len(), 6);
    assert_eq!(
        csv::Reader::from_path(&output).unwrap().records().count(),
        2
    );
    assert!(fs::read_dir(&root.0).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".trivium-export-")));
}

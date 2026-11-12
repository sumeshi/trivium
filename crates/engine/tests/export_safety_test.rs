//! Output transactions protect evidence and all active case storage, including aliases.
use polars::prelude::*;
use std::{
    fs::{self, File},
    path::PathBuf,
};
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
fn export_preserves_managed_files_and_aliases_and_supports_real_legacy_annotation_only() {
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
        root.0.join("projects.json"),
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
            .contains("source evidence")
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
    // A genuine legacy typed file has no synthetic __rowid column.
    let columns =
        ops::projects::load_project(&store, ops::projects::ProjectRequest { project_id: first })
            .unwrap()
            .columns_meta;
    let mut typed = DataFrame::new(vec![
        Series::new(&columns[0].id, [0i64, 1]),
        Series::new(&columns[1].id, ["first", "last"]),
    ])
    .unwrap();
    ParquetWriter::new(File::create(store.project_dir(&first).join("data.parquet")).unwrap())
        .finish(&mut typed)
        .unwrap();
    let output = root.0.join("annotations.csv");
    fs::write(&output, b"previous destination").unwrap();
    let result =
        ops::export::export_project(&store, payload(output.clone(), Some(Vec::new()))).unwrap();
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

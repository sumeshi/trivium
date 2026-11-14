//! Every output requires explicit overwrite consent; case storage is protected.

use std::{
    fs,
    path::{Path, PathBuf},
};
use trivium_engine::{
    error::AppError,
    ops::{
        diff::{diff_projects, DiffProjectsPayload},
        export::{export_project, ExportProjectPayload},
        iocs::{export_iocs, ExportIocsPayload},
        preset::{save_preset, SavePresetPayload},
        projects::{create_project, CreateProjectPayload},
        report::{build_report, BuildReportPayload},
        rows::QuerySpec,
    },
    store::ProjectsStore,
};
use uuid::Uuid;

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn create(store: &ProjectsStore, path: &Path) -> Uuid {
    fs::write(path, "id,message\n0,original source\n").unwrap();
    create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap()
    .meta
    .id
}

fn output(
    kind: &str,
    store: &ProjectsStore,
    first: Uuid,
    second: Uuid,
    destination: &Path,
    allow_overwrite: bool,
) -> Result<(), AppError> {
    let destination = destination.to_string_lossy().into_owned();
    match kind {
        "CSV" => export_project(
            store,
            ExportProjectPayload {
                project_id: first,
                destination,
                allow_overwrite,
                scope: None,
                spec: QuerySpec::default(),
                columns: None,
                row_indices: None,
                include_annotations: None,
            },
        )
        .map(|_| ()),
        "IOC" => export_iocs(
            store,
            ExportIocsPayload {
                project_id: first,
                entries: Some(Vec::new()),
                destination,
                allow_overwrite,
            },
        ),
        "Preset" => save_preset(
            store,
            SavePresetPayload {
                project_id: first,
                name: None,
                condition: None,
                hidden_columns: Vec::new(),
                sort_key: None,
                sort_direction: None,
                column_order: None,
                column_widths: None,
                destination,
                allow_overwrite,
            },
        ),
        "Report" => build_report(
            store,
            BuildReportPayload {
                project_id: first,
                search: None,
                columns: None,
                search_columns: None,
                flag_filter: None,
                bookmarked_only: None,
                sort_key: None,
                sort_direction: None,
                condition: None,
                destination,
                allow_overwrite,
            },
        )
        .map(|_| ()),
        "Diff" => diff_projects(
            store,
            DiffProjectsPayload {
                project_id_a: first,
                project_id_b: second,
                key_columns: vec!["c000001".into()],
                destination: Some(destination),
                allow_overwrite,
            },
        )
        .map(|_| ()),
        _ => unreachable!(),
    }
}

#[test]
fn every_output_confirms_existing_files_and_sources_then_honors_explicit_consent() {
    let root = Fixture(std::env::temp_dir().join(format!("trivium-overwrite-{}", Uuid::new_v4())));
    fs::create_dir(&root.0).unwrap();
    let store = ProjectsStore::new(root.0.join("store")).unwrap();
    let first_source = root.0.join("a.csv");
    let second_source = root.0.join("b.csv");
    let third_source = root.0.join("c.csv");
    let first = create(&store, &first_source);
    let second = create(&store, &second_source);
    create(&store, &third_source);
    for kind in ["CSV", "IOC", "Preset", "Report", "Diff"] {
        for destination in [
            root.0.join("existing.out"),
            first_source.clone(),
            third_source.clone(),
        ] {
            fs::write(&destination, b"previous output or source").unwrap();
            let error = output(kind, &store, first, second, &destination, false).unwrap_err();
            assert!(
                error
                    .to_string()
                    .starts_with("Overwrite confirmation required:"),
                "{kind}: {error}"
            );
            assert!(error
                .to_string()
                .contains(destination.to_string_lossy().as_ref()));
            assert_eq!(
                fs::read(&destination).unwrap(),
                b"previous output or source"
            );
            output(kind, &store, first, second, &destination, true).unwrap();
            assert_ne!(
                fs::read(&destination).unwrap(),
                b"previous output or source",
                "{kind}"
            );
        }
        let fresh = root.0.join(format!("new-{kind}.out"));
        output(kind, &store, first, second, &fresh, false).unwrap();
        assert!(fresh.is_file(), "new output needs no overwrite consent");

        let managed = store.project_dir(&first).join("data.parquet");
        let before = fs::read(&managed).unwrap();
        let error = output(kind, &store, first, second, &managed, true).unwrap_err();
        assert!(
            error.to_string().contains("managed case storage"),
            "{kind}: {error}"
        );
        assert_eq!(fs::read(&managed).unwrap(), before);
    }
}

#[test]
fn existing_aliases_require_consent_and_managed_aliases_remain_blocked() {
    let root =
        Fixture(std::env::temp_dir().join(format!("trivium-overwrite-alias-{}", Uuid::new_v4())));
    fs::create_dir(&root.0).unwrap();
    let store = ProjectsStore::new(root.0.join("store")).unwrap();
    let source = root.0.join("source.csv");
    let id = create(&store, &source);
    let before = fs::read(&source).unwrap();
    let alias = root.0.join("source-alias.csv");
    fs::hard_link(&source, &alias).unwrap();
    assert!(output("CSV", &store, id, id, &alias, false)
        .unwrap_err()
        .to_string()
        .starts_with("Overwrite confirmation required:"));
    assert_eq!(fs::read(&source).unwrap(), before);
    output("CSV", &store, id, id, &alias, true).unwrap();
    assert_eq!(
        fs::read(&source).unwrap(),
        before,
        "atomic replacement changes only the requested hardlink"
    );

    let managed = store.project_dir(&id).join("data.parquet");
    let managed_alias = root.0.join("managed-alias.csv");
    fs::hard_link(&managed, &managed_alias).unwrap();
    assert!(output("CSV", &store, id, id, &managed_alias, true)
        .unwrap_err()
        .to_string()
        .contains("managed case storage"));
    #[cfg(unix)]
    {
        let symlink = root.0.join("source-symlink.csv");
        std::os::unix::fs::symlink(&source, &symlink).unwrap();
        assert!(output("CSV", &store, id, id, &symlink, false)
            .unwrap_err()
            .to_string()
            .starts_with("Overwrite confirmation required:"));
        assert_eq!(fs::read(&source).unwrap(), before);
    }
}

#[test]
fn missing_overwrite_option_defaults_to_false_for_every_payload() {
    let id = Uuid::new_v4();
    let value = serde_json::json!({"projectId":id,"destination":"existing.csv"});
    assert!(
        !serde_json::from_value::<ExportProjectPayload>(value.clone())
            .unwrap()
            .allow_overwrite
    );
    assert!(
        !serde_json::from_value::<ExportIocsPayload>(value.clone())
            .unwrap()
            .allow_overwrite
    );
    assert!(
        !serde_json::from_value::<SavePresetPayload>(value.clone())
            .unwrap()
            .allow_overwrite
    );
    assert!(
        !serde_json::from_value::<BuildReportPayload>(value)
            .unwrap()
            .allow_overwrite
    );
    assert!(
        !serde_json::from_value::<DiffProjectsPayload>(serde_json::json!({
            "projectIdA":id,"projectIdB":id,"keyColumns":["c000001"],"destination":"existing.csv",
        }))
        .unwrap()
        .allow_overwrite
    );
}

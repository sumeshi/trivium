use super::*;

#[test]
fn sqlite_cases_persist_annotations_settings_and_metadata() {
    let fixture = Fixture::new();
    let id = Uuid::new_v4();
    let store = ProjectsStore::new(fixture.path().into()).unwrap();
    store.insert(meta(id)).unwrap();
    patch_flag_atomic(&store.database_path(&id), 5, &patch("critical")).unwrap();
    save_setting(
        &store.project_dir(&id),
        "fixture",
        &serde_json::json!({"value": 42}),
    )
    .unwrap();
    let reopened = ProjectsStore::new(fixture.path().into()).unwrap();
    assert_eq!(reopened.try_all().unwrap().len(), 1);
    assert_eq!(reopened.try_find(&id).unwrap().unwrap().flagged_records, 1);
    assert_eq!(
        load_flag(&reopened.database_path(&id), 5)
            .unwrap()
            .unwrap()
            .flag,
        "critical"
    );
    assert_eq!(
        load_setting(&reopened.project_dir(&id), "fixture")
            .unwrap()
            .unwrap()["value"],
        42
    );
}

#[test]
fn sidecar_only_storage_is_rejected_without_creating_an_empty_database() {
    for name in ["flags.json", "flags.db", "view_state.json"] {
        let fixture = Fixture::new();
        let path = fixture.path().join(name);
        if name == "flags.db" {
            fs::create_dir(&path).unwrap();
            fs::write(path.join("db"), b"unsupported annotation storage").unwrap();
        } else {
            fs::write(&path, b"unsupported project storage").unwrap();
        }
        let error = load_flags(&fixture.flags()).unwrap_err();
        assert!(
            error.to_string().contains("Unsupported project storage"),
            "{error:#}"
        );
        assert!(load_setting_bytes(fixture.path(), "view_state").is_err());
        assert!(!project_database_path(fixture.path()).exists());
        assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 1);
        let retained = if name == "flags.db" {
            path.join("db")
        } else {
            path
        };
        assert!(fs::read(retained).unwrap().starts_with(b"unsupported"));
    }
}

#[test]
fn annotation_operations_require_the_current_database_path() {
    let fixture = Fixture::new();
    let error = load_flags(&fixture.path().join("flags.json")).unwrap_err();
    assert!(error.to_string().contains("project.sqlite is required"));
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}

#[test]
fn unsupported_saved_cases_fail_checked_listing_without_changing_files() {
    let fixture = Fixture::new();
    let store = ProjectsStore::new(fixture.path().into()).unwrap();
    let id = Uuid::new_v4();
    let directory = store.project_dir(&id);
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("data.parquet"), b"preserved evidence").unwrap();
    assert!(store
        .try_all()
        .unwrap_err()
        .to_string()
        .contains("project.sqlite is required"));
    assert!(store
        .try_find(&id)
        .unwrap_err()
        .to_string()
        .contains("project.sqlite is required"));
    assert_eq!(
        fs::read(directory.join("data.parquet")).unwrap(),
        b"preserved evidence"
    );
    assert_eq!(fs::read_dir(directory).unwrap().count(), 1);
}

#[test]
fn unsupported_project_versions_are_rejected_before_writes_or_metadata_use() {
    let fixture = Fixture::new();
    let mut metadata = meta(Uuid::new_v4());
    metadata.storage_version = 0;
    assert!(insert_project_meta(fixture.path(), &metadata).is_err());
    assert!(!project_database_path(fixture.path()).exists());
    metadata.storage_version = 2;
    insert_project_meta(fixture.path(), &metadata).unwrap();
    let db = sqlite::connect(fixture.path()).unwrap();
    db.execute(
        "UPDATE project_state SET metadata=json_set(metadata,'$.storage_version',0)",
        [],
    )
    .unwrap();
    drop(db);
    let bytes = fs::read(project_database_path(fixture.path())).unwrap();
    assert!(read_project_meta(fixture.path()).is_err());
    assert!(update_project_meta(fixture.path(), |meta| meta.name = "changed".into()).is_err());
    assert_eq!(
        fs::read(project_database_path(fixture.path())).unwrap(),
        bytes
    );
}

#[test]
fn registration_uses_the_transactional_annotation_count() {
    let fixture = Fixture::new();
    save_flags(&fixture.flags(), &HashMap::from([(5, entry("critical"))])).unwrap();
    let metadata = meta(Uuid::new_v4());
    insert_project_meta(fixture.path(), &metadata).unwrap();
    assert_eq!(
        read_project_meta(fixture.path())
            .unwrap()
            .unwrap()
            .flagged_records,
        1
    );
}

#[test]
fn corrupt_sqlite_is_reported_without_replacing_it() {
    let fixture = Fixture::new();
    let corrupt = b"this is not SQLite";
    fs::write(project_database_path(fixture.path()), corrupt).unwrap();
    assert!(load_flags(&fixture.flags()).is_err());
    assert_eq!(
        fs::read(project_database_path(fixture.path())).unwrap(),
        corrupt
    );
}

#[test]
fn an_unrecognized_sqlite_schema_is_never_initialized_as_an_empty_case() {
    let fixture = Fixture::new();
    let path = project_database_path(fixture.path());
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "CREATE TABLE preserved(payload TEXT); INSERT INTO preserved VALUES('evidence');",
    )
    .unwrap();
    drop(db);
    let before = fs::read(&path).unwrap();
    let error = load_flags(&fixture.flags()).unwrap_err();
    assert!(error.to_string().contains("unrecognized schema"));
    assert_eq!(fs::read(path).unwrap(), before);
}

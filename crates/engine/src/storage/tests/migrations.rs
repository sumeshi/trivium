use super::*;

#[test]
fn json_case_migrates_idempotently_and_retains_legacy_bytes() {
    let fixture = Fixture::new();
    let id = Uuid::new_v4();
    let case = fixture.path().join("projects").join(id.to_string());
    fs::create_dir_all(&case).unwrap();
    let registry = serde_json::to_vec(&vec![meta(id)]).unwrap();
    fs::write(fixture.path().join("projects.json"), &registry).unwrap();
    let legacy = serde_json::to_vec(&HashMap::from([(5, entry("critical"))])).unwrap();
    fs::write(case.join("flags.json"), &legacy).unwrap();
    let store = ProjectsStore::new(fixture.path().into()).unwrap();
    let (flags, revision) = load_flag_snapshot(&case.join("flags.json")).unwrap();
    assert_eq!(flags.get(&5), Some(&entry("critical")));
    assert_eq!(revision, 0);
    assert_eq!(store.try_find(&id).unwrap().unwrap().flagged_records, 1);
    patch_flag_atomic(&case.join("flags.json"), 5, &patch("safe")).unwrap();
    let reopened = ProjectsStore::new(fixture.path().into()).unwrap();
    assert_eq!(
        load_flag(&reopened.flags_path(&id), 5)
            .unwrap()
            .unwrap()
            .flag,
        "safe"
    );
    assert_eq!(fs::read(case.join("flags.json")).unwrap(), legacy);
    assert_eq!(
        fs::read(fixture.path().join("projects.json")).unwrap(),
        registry
    );
}

#[test]
fn sled_migration_refuses_active_database_and_never_modifies_source() {
    let fixture = Fixture::new();
    let legacy_path = fixture.path().join("flags.db");
    let db = sled::open(&legacy_path).unwrap();
    db.insert(
        7u64.to_be_bytes(),
        serde_json::to_vec(&entry("suspicious")).unwrap(),
    )
    .unwrap();
    db.flush().unwrap();
    let error = load_flags(&fixture.flags()).unwrap_err();
    assert!(format!("{error:#}").contains("in use"), "{error:#}");
    drop(db);
    fn files(dir: &Path) -> HashMap<PathBuf, Vec<u8>> {
        let mut result = HashMap::new();
        fn visit(root: &Path, dir: &Path, out: &mut HashMap<PathBuf, Vec<u8>>) {
            for item in fs::read_dir(dir).unwrap() {
                let path = item.unwrap().path();
                if path.is_dir() {
                    visit(root, &path, out);
                } else {
                    out.insert(
                        path.strip_prefix(root).unwrap().into(),
                        fs::read(path).unwrap(),
                    );
                }
            }
        }
        visit(dir, dir, &mut result);
        result
    }
    #[cfg(unix)]
    let original_permissions = {
        use std::os::unix::fs::PermissionsExt;
        let path = legacy_path.join("db");
        let original = fs::metadata(&path).unwrap().permissions();
        fs::set_permissions(path, fs::Permissions::from_mode(original.mode() & !0o222)).unwrap();
        original
    };
    let before = files(&legacy_path);
    assert_eq!(
        load_flag(&fixture.flags(), 7).unwrap(),
        Some(entry("suspicious"))
    );
    assert_eq!(files(&legacy_path), before);
    #[cfg(unix)]
    fs::set_permissions(legacy_path.join("db"), original_permissions).unwrap();
    assert_eq!(
        load_flag(&fixture.flags(), 7).unwrap(),
        Some(entry("suspicious"))
    );
}

#[test]
fn failed_legacy_migration_rolls_back_and_can_retry() {
    let fixture = Fixture::new();
    let db = sled::open(fixture.path().join("flags.db")).unwrap();
    db.insert(
        0u64.to_be_bytes(),
        serde_json::to_vec(&entry("safe")).unwrap(),
    )
    .unwrap();
    db.insert(1u64.to_be_bytes(), b"invalid json").unwrap();
    db.flush().unwrap();
    drop(db);
    assert!(load_flags(&fixture.flags()).is_err());
    let connection = sqlite::connect(fixture.path()).unwrap();
    assert!(!sqlite::migrated(&connection, "annotations").unwrap());
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM annotations", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    drop(connection);
    let db = sled::open(fixture.path().join("flags.db")).unwrap();
    db.remove(1u64.to_be_bytes()).unwrap();
    db.flush().unwrap();
    drop(db);
    assert_eq!(load_flags(&fixture.flags()).unwrap().len(), 1);
}

#[test]
fn corrupt_sqlite_and_registry_are_reported_without_replacing_them() {
    let fixture = Fixture::new();
    let corrupt = b"this is not SQLite";
    fs::write(project_database_path(fixture.path()), corrupt).unwrap();
    assert!(load_flags(&fixture.flags()).is_err());
    assert_eq!(
        fs::read(project_database_path(fixture.path())).unwrap(),
        corrupt
    );
    let workspace = Fixture::new();
    fs::write(workspace.path().join("projects.json"), b"broken registry").unwrap();
    assert!(ProjectsStore::new(workspace.path().into()).is_err());
    assert_eq!(
        fs::read(workspace.path().join("projects.json")).unwrap(),
        b"broken registry"
    );
    assert!(fs::read_dir(workspace.path()).unwrap().any(|item| item
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with("projects.json.corrupt.")));
}

#[test]
fn interrupted_registry_migration_resumes_without_overwriting_canonical_cases() {
    let fixture = Fixture::new();
    let first = meta(Uuid::new_v4());
    let second = meta(Uuid::new_v4());
    let first_dir = fixture.path().join("projects").join(first.id.to_string());
    let second_dir = fixture.path().join("projects").join(second.id.to_string());
    fs::create_dir_all(&first_dir).unwrap();
    fs::create_dir_all(&second_dir).unwrap();
    let bytes = serde_json::to_vec(&[first.clone(), second.clone()]).unwrap();
    fs::write(fixture.path().join("projects.json"), &bytes).unwrap();
    fs::write(project_database_path(&second_dir), b"broken fixture").unwrap();
    assert!(ProjectsStore::new(fixture.path().into()).is_err());
    assert_eq!(read_project_meta(&first_dir).unwrap().unwrap().id, first.id);
    update_project_meta(&first_dir, |meta| meta.name = "canonical edit".into()).unwrap();
    fs::rename(
        project_database_path(&second_dir),
        second_dir.join("broken-backup"),
    )
    .unwrap();
    let store = ProjectsStore::new(fixture.path().into()).unwrap();
    assert_eq!(store.try_all().unwrap().len(), 2);
    assert_eq!(
        store.try_find(&first.id).unwrap().unwrap().name,
        "canonical edit"
    );
    assert_eq!(
        fs::read(fixture.path().join("projects.json")).unwrap(),
        bytes
    );
}

#[test]
fn corrupt_registry_never_hides_unmigrated_orphan_cases() {
    let fixture = Fixture::new();
    let id = Uuid::new_v4();
    let store = ProjectsStore::new(fixture.path().into()).unwrap();
    store.insert(meta(id)).unwrap();
    let orphan = fixture
        .path()
        .join("projects")
        .join(Uuid::new_v4().to_string());
    fs::create_dir(&orphan).unwrap();
    fs::write(orphan.join("data.parquet"), b"fixture evidence").unwrap();
    fs::write(fixture.path().join("projects.json"), b"corrupt registry").unwrap();
    assert!(ProjectsStore::new(fixture.path().into()).is_err());
    assert_eq!(
        fs::read(orphan.join("data.parquet")).unwrap(),
        b"fixture evidence"
    );
    assert!(store.try_find(&id).unwrap().is_some());
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

use super::*;

#[test]
fn count_metadata_and_revision_fail_or_commit_with_the_annotation() {
    let fixture = Fixture::new();
    insert_project_meta(fixture.path(), &meta(Uuid::new_v4())).unwrap();
    load_flags(&fixture.flags()).unwrap();
    let connection = sqlite::connect(fixture.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_count BEFORE UPDATE OF revision ON project_state BEGIN SELECT RAISE(ABORT, 'fixture failure'); END;").unwrap();
    assert!(patch_flag_atomic(&fixture.flags(), 0, &patch("critical")).is_err());
    assert_eq!(
        load_flag_snapshot(&fixture.flags()).unwrap(),
        (HashMap::new(), 0)
    );
    assert_eq!(
        read_project_meta(fixture.path())
            .unwrap()
            .unwrap()
            .flagged_records,
        0
    );
    connection.execute_batch("DROP TRIGGER fail_count").unwrap();
    let result = patch_flag_atomic(&fixture.flags(), 0, &patch("critical")).unwrap();
    assert_eq!((result.flagged_records, result.revision), (1, 1));
    assert_eq!(
        read_project_meta(fixture.path())
            .unwrap()
            .unwrap()
            .flagged_records,
        1
    );
}

#[test]
fn concurrent_partial_patches_preserve_unmentioned_fields() {
    let fixture = Fixture::new();
    load_flags(&fixture.flags()).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = vec![];
    for patch in [
        patch("critical"),
        AnnotationPatch {
            memo: Some(Some("investigation".into())),
            ..Default::default()
        },
    ] {
        let barrier = barrier.clone();
        let path = fixture.flags();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            patch_flag_atomic(&path, 1, &patch).unwrap();
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }
    let result = load_flag(&fixture.flags(), 1).unwrap().unwrap();
    assert_eq!(result.flag, "critical");
    assert_eq!(result.memo.as_deref(), Some("investigation"));
    assert_eq!(count_flagged(&fixture.flags()).unwrap(), 1);
}

#[test]
fn durable_undo_survives_reopen_and_preserves_unrelated_edits() {
    if let Some(path) = std::env::var_os("TRIVIUM_TEST_UNDO_CASE") {
        assert_eq!(
            undo_bulk_patch(&PathBuf::from(path).join("flags.json"), &JobContext::none()).unwrap(),
            2
        );
        return;
    }
    let fixture = Fixture::new();
    let version = load_flag_snapshot(&fixture.flags()).unwrap().1;
    apply_bulk_patch(
        &fixture.flags(),
        &[1, 2],
        &patch("critical"),
        version,
        &JobContext::none(),
    )
    .unwrap();
    patch_flag_atomic(&fixture.flags(), 9, &patch("safe")).unwrap();
    // A separate process has no access to the writer's memory or connections.
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::tests::mutations::durable_undo_survives_reopen_and_preserves_unrelated_edits",
        ])
        .env("TRIVIUM_TEST_UNDO_CASE", fixture.path())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(load_flag(&fixture.flags(), 1).unwrap().is_none());
    assert_eq!(
        load_flag(&fixture.flags(), 9).unwrap().unwrap().flag,
        "safe"
    );
    assert_eq!(count_flagged(&fixture.flags()).unwrap(), 1);
    assert!(undo_bulk_patch(&fixture.flags(), &JobContext::none()).is_err());
}

#[test]
fn undo_conflicts_and_budget_errors_keep_the_previous_undo() {
    let fixture = Fixture::new();
    let version = load_flag_snapshot(&fixture.flags()).unwrap().1;
    apply_bulk_patch(
        &fixture.flags(),
        &[1],
        &patch("safe"),
        version,
        &JobContext::none(),
    )
    .unwrap();
    let version = project_revision(fixture.path()).unwrap();
    let too_large = AnnotationPatch {
        memo: Some(Some("x".repeat(512))),
        ..Default::default()
    };
    assert!(undo::apply_bulk_patch_with_limit(
        &fixture.flags(),
        &[2],
        &too_large,
        version,
        &JobContext::none(),
        100
    )
    .is_err());
    assert_eq!(project_revision(fixture.path()).unwrap(), version);
    assert!(load_flag(&fixture.flags(), 2).unwrap().is_none());
    patch_flag_atomic(&fixture.flags(), 1, &patch("critical")).unwrap();
    assert!(undo_bulk_patch(&fixture.flags(), &JobContext::none())
        .unwrap_err()
        .to_string()
        .contains("changed"));
    patch_flag_atomic(&fixture.flags(), 1, &patch("safe")).unwrap();
    assert_eq!(
        undo_bulk_patch(&fixture.flags(), &JobContext::none()).unwrap(),
        1
    );
}

#[test]
fn cancelling_after_a_batch_rolls_back_annotations_revision_and_undo() {
    let fixture = Fixture::new();
    let version = load_flag_snapshot(&fixture.flags()).unwrap().1;
    apply_bulk_patch(
        &fixture.flags(),
        &[900],
        &patch("safe"),
        version,
        &JobContext::none(),
    )
    .unwrap();
    let guard = jobs::try_begin_job(JobKind::Mutation, None).unwrap();
    let context = JobContext::with_job(
        guard,
        Some(Arc::new(|progress| {
            assert!(progress.done >= 256);
            jobs::request_cancel(progress.id);
        })),
    );
    let version = project_revision(fixture.path()).unwrap();
    assert!(apply_bulk_patch(
        &fixture.flags(),
        &(0..800).collect::<Vec<_>>(),
        &patch("critical"),
        version,
        &context
    )
    .is_err());
    assert_eq!(project_revision(fixture.path()).unwrap(), version);
    assert_eq!(count_flagged(&fixture.flags()).unwrap(), 1);
    assert!(load_flag(&fixture.flags(), 0).unwrap().is_none());
    assert_eq!(
        undo_bulk_patch(&fixture.flags(), &JobContext::none()).unwrap(),
        1
    );
}

#[test]
fn settings_migrate_once_and_guarded_saves_preserve_unreadable_bytes() {
    let fixture = Fixture::new();
    let legacy = b"not json";
    fs::write(fixture.path().join("view_state.json"), legacy).unwrap();
    assert_eq!(
        load_setting_bytes(fixture.path(), "view_state")
            .unwrap()
            .unwrap(),
        legacy
    );
    assert!(
        save_setting_bytes_checked(fixture.path(), "view_state", b"{}", |old| {
            serde_json::from_slice::<serde_json::Value>(old.unwrap())?;
            Ok(())
        })
        .is_err()
    );
    assert_eq!(
        load_setting_bytes(fixture.path(), "view_state")
            .unwrap()
            .unwrap(),
        legacy
    );
    assert_eq!(
        fs::read(fixture.path().join("view_state.json")).unwrap(),
        legacy
    );
    save_setting(
        fixture.path(),
        "column_profile",
        &serde_json::json!({"width":12}),
    )
    .unwrap();
    assert_eq!(
        load_setting(fixture.path(), "column_profile")
            .unwrap()
            .unwrap()["width"],
        12
    );
    let db = sqlite::connect(fixture.path()).unwrap();
    assert_eq!(
        db.pragma_query_value(None, "cache_size", |row| row.get::<_, i64>(0))
            .unwrap(),
        -2048
    );
}

#[test]
fn inactive_cases_retain_no_database_handles() {
    let fixture = Fixture::new();
    for index in 0..32 {
        let case = fixture.path().join(index.to_string());
        save_setting(&case, "profile", &serde_json::json!({"index": index})).unwrap();
        patch_flag_atomic(&case.join("flags.json"), 1, &patch("safe")).unwrap();
        let moved = fixture.path().join(format!("moved-{index}"));
        fs::rename(&case, &moved).unwrap();
        assert_eq!(
            load_flag(&moved.join("flags.json"), 1)
                .unwrap()
                .unwrap()
                .flag,
            "safe"
        );
        fs::remove_dir_all(moved).unwrap();
    }
    #[cfg(target_os = "linux")]
    {
        let handles = fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|item| fs::read_link(item.ok()?.path()).ok())
            .filter(|path| path.starts_with(fixture.path()))
            .count();
        assert_eq!(handles, 0, "no inactive case connection should remain open");
    }
}

#[test]
fn unknown_flags_are_unflagged_without_losing_raw_annotations() {
    let fixture = Fixture::new();
    let id = Uuid::new_v4();
    let store = ProjectsStore::new(fixture.path().into()).unwrap();
    store.insert(meta(id)).unwrap();
    let update = |flag: &str| {
        crate::ops::flags::update_flag(
            &store,
            crate::ops::flags::UpdateFlagPayload {
                project_id: id,
                row_index: 0,
                flag: Some(flag.into()),
                memo: Some(Some("keep note".into())),
                tags: None,
                bookmarked: None,
            },
        )
        .unwrap()
    };
    assert_eq!(update("✗").flagged_records, 1);
    let changed = update("unrecognized legacy label");
    assert_eq!(changed.flagged_records, 0);
    let reopened = ProjectsStore::new(fixture.path().into()).unwrap();
    assert_eq!(reopened.try_find(&id).unwrap().unwrap().flagged_records, 0);
    let (flags, count, version) = load_page_flag_snapshot(&reopened.flags_path(&id), &[0]).unwrap();
    assert_eq!(count, 0);
    assert_eq!(version, changed.revision);
    let annotation = flags.get(&0).unwrap();
    assert_eq!(annotation.flag, "unrecognized legacy label");
    assert_eq!(annotation.memo.as_deref(), Some("keep note"));
}

//! T02: single and bulk annotation updates are partial updates.
//!
//! Clearing a flag or a memo must not delete tags and bookmarks, and saving a
//! memo must not rewrite the flag, tags, or bookmark of other fields.

use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use trivium_engine::annotation::AnnotationPatch;
use trivium_engine::models::{FlagEntry, UpdateFlagResponse};
use trivium_engine::ops;
use trivium_engine::ops::bulk::{bulk_preview, bulk_update, BulkAnnotatePayload};
use trivium_engine::ops::export::{export_project, ExportProjectPayload};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::projects::{load_project, CreateProjectPayload, ProjectRequest};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::storage::load_flags;
use trivium_engine::store::ProjectsStore;

fn write_fixture(root: &std::path::Path) -> PathBuf {
    let path = root.join("annotation.csv");
    let body = "ts,user,proc\n\
2024-01-01T00:00:01,alice,powershell\n\
2024-01-01T00:00:02,bob,cmd\n\
2024-01-01T00:00:03,carol,explorer\n";
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn setup() -> (PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-annotation-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_fixture(&root);
    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project");
    (root, store, summary.meta.id)
}

fn update(
    store: &ProjectsStore,
    id: Uuid,
    row_index: usize,
    flag: Option<&str>,
    memo: Option<Option<&str>>,
    tags: Option<Vec<&str>>,
    bookmarked: Option<bool>,
) -> UpdateFlagResponse {
    update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index,
            flag: flag.map(str::to_string),
            memo: memo.map(|value| value.map(str::to_string)),
            tags: tags.map(|values| values.into_iter().map(str::to_string).collect()),
            bookmarked,
        },
    )
    .expect("update_flag failed")
}

fn bulk(
    store: &ProjectsStore,
    id: Uuid,
    flag: Option<&str>,
    memo: Option<Option<&str>>,
    tags: Option<Vec<&str>>,
    bookmarked: Option<bool>,
) -> usize {
    let payload = BulkAnnotatePayload {
        project_id: id,
        target: "all".to_string(),
        row_indices: None,
        preview_token: None,
        spec: QuerySpec {
            search: None,
            search_columns: None,
            flag_filter: None,
            condition: None,
            ..Default::default()
        },
        patch: AnnotationPatch {
            flag: flag.map(str::to_string),
            memo: memo.map(|value| value.map(str::to_string)),
            tags: tags.map(|values| values.into_iter().map(str::to_string).collect()),
            bookmarked,
            ..Default::default()
        },
    };
    let preview = bulk_preview(store, payload.clone()).expect("preview failed");
    let mut payload = payload;
    payload.preview_token = Some(preview.preview_token);
    bulk_update(store, payload)
        .expect("bulk update failed")
        .updated
}

fn stored(store: &ProjectsStore, id: Uuid, row_index: usize) -> Option<FlagEntry> {
    load_flags(&store.project_dir(&id).join("flags.json"))
        .expect("failed to load flags")
        .get(&row_index)
        .cloned()
}

fn annotate_all_fields(store: &ProjectsStore, id: Uuid, row_index: usize) {
    update(
        store,
        id,
        row_index,
        Some("suspicious"),
        Some(Some("note")),
        Some(vec!["pc", "triage"]),
        Some(true),
    );
}

#[test]
fn payloads_keep_the_omitted_and_explicit_clear_contract() {
    let project_id = Uuid::new_v4();
    let omitted: UpdateFlagPayload = serde_json::from_value(serde_json::json!({
        "projectId": project_id,
        "row_index": 0,
    }))
    .expect("omitted annotation fields");
    assert_eq!(omitted.flag, None);
    assert_eq!(omitted.memo, None);
    assert_eq!(omitted.tags, None);
    assert_eq!(omitted.bookmarked, None);

    let explicit: UpdateFlagPayload = serde_json::from_value(serde_json::json!({
        "projectId": project_id,
        "row_index": 0,
        "flag": "",
        "memo": null,
        "tags": [],
        "bookmarked": false,
    }))
    .expect("explicit annotation fields");
    assert_eq!(explicit.flag, Some(String::new()));
    assert_eq!(explicit.memo, Some(None));
    assert_eq!(explicit.tags, Some(Vec::new()));
    assert_eq!(explicit.bookmarked, Some(false));

    let bulk: BulkAnnotatePayload = serde_json::from_value(serde_json::json!({
        "projectId": project_id,
        "target": "all",
        "memo": "note",
    }))
    .expect("bulk annotation fields");
    assert_eq!(bulk.patch.flag, None);
    assert_eq!(bulk.patch.memo, Some(Some("note".to_string())));
    assert_eq!(bulk.patch.tags, None);
    assert_eq!(bulk.patch.bookmarked, None);
}

#[test]
fn clearing_a_flag_keeps_memo_tags_and_bookmark() {
    let (root, store, id) = setup();
    annotate_all_fields(&store, id, 0);

    let row = update(&store, id, 0, Some(""), None, None, None)
        .entry
        .expect("entry must remain");
    assert_eq!(row.flag, "");
    assert_eq!(row.memo.as_deref(), Some("note"));
    assert_eq!(row.tags, vec!["pc".to_string(), "triage".to_string()]);
    assert!(row.bookmarked);

    let entry = stored(&store, id, 0).expect("entry must remain after clearing the flag");
    assert_eq!(entry.flag, "");
    assert_eq!(entry.memo.as_deref(), Some("note"));
    assert_eq!(entry.tags, vec!["pc".to_string(), "triage".to_string()]);
    assert!(entry.bookmarked);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn saving_a_memo_keeps_flag_tags_bookmark_and_other_rows() {
    let (root, store, id) = setup();
    annotate_all_fields(&store, id, 0);
    update(
        &store,
        id,
        1,
        Some("safe"),
        Some(Some("second")),
        Some(vec!["other"]),
        Some(false),
    );

    let row = update(&store, id, 0, None, Some(Some("changed")), None, None)
        .entry
        .expect("entry must remain");
    assert_eq!(row.flag, "suspicious");
    assert_eq!(row.memo.as_deref(), Some("changed"));
    assert_eq!(row.tags, vec!["pc".to_string(), "triage".to_string()]);
    assert!(row.bookmarked);

    let other = stored(&store, id, 1).expect("row 1 must be untouched");
    assert_eq!(other.flag, "safe");
    assert_eq!(other.memo.as_deref(), Some("second"));
    assert_eq!(other.tags, vec!["other".to_string()]);
    assert!(!other.bookmarked);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn omitted_fields_keep_values_and_explicit_empties_clear_them() {
    let (root, store, id) = setup();
    annotate_all_fields(&store, id, 0);
    let original = stored(&store, id, 0).expect("entry missing");

    // Every field omitted: nothing changes.
    update(&store, id, 0, None, None, None, None);
    assert_eq!(stored(&store, id, 0), Some(original.clone()));

    // Explicit empty flag, empty memo, empty tags, and false bookmark clear
    // those fields. The entry survives while one field still has content.
    update(
        &store,
        id,
        0,
        Some(""),
        Some(Some("")),
        None,
        Some(false),
    );
    let entry = stored(&store, id, 0).expect("entry must survive while tags remain");
    assert_eq!(entry.flag, "");
    assert_eq!(entry.memo, None);
    assert_eq!(entry.tags, vec!["pc".to_string(), "triage".to_string()]);
    assert!(!entry.bookmarked);

    // Clearing the remaining tags removes the entry.
    update(
        &store,
        id,
        0,
        Some(""),
        Some(None),
        Some(vec![]),
        Some(false),
    );
    assert_eq!(stored(&store, id, 0), None);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn bulk_flag_clear_keeps_tags_bookmarks_and_memos() {
    let (root, store, id) = setup();
    annotate_all_fields(&store, id, 0);
    update(
        &store,
        id,
        1,
        None,
        None,
        Some(vec!["tag-only"]),
        None,
    );

    assert_eq!(
        bulk(&store, id, Some(""), None, None, None),
        1,
        "only row 0 had a flag to clear"
    );

    let row0 = stored(&store, id, 0).expect("row 0 entry must remain");
    assert_eq!(row0.flag, "");
    assert_eq!(row0.memo.as_deref(), Some("note"));
    assert_eq!(row0.tags, vec!["pc".to_string(), "triage".to_string()]);
    assert!(row0.bookmarked);

    let row1 = stored(&store, id, 1).expect("tag-only entry must remain");
    assert_eq!(row1.flag, "");
    assert_eq!(row1.tags, vec!["tag-only".to_string()]);

    // Clearing the memo is separate from clearing tags and bookmarks.
    assert_eq!(
        bulk(&store, id, None, Some(None), None, None),
        1,
        "only row 0 had a note to clear"
    );
    let row0 = stored(&store, id, 0).expect("row 0 entry must remain");
    assert_eq!(row0.memo, None);
    assert_eq!(row0.tags, vec!["pc".to_string(), "triage".to_string()]);
    assert!(row0.bookmarked);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn note_text_survives_update_reopen_query_export_and_restore() {
    let (root, store, id) = setup();
    let notes = [
        "Sender: <alice@example.org>",
        "<alice@example.org>",
        "<CommandLine>whoami & \"hostname\"</CommandLine>",
        "<img src=x onerror=alert(1)><script>alert(1)</script>",
        "  first line\r\n\tsecond line, \"quoted\"\n \t",
        " \t\n ",
        "\u{0000}\u{0008}\u{000b}\u{001f}",
    ];
    for (case, note) in notes.iter().enumerate() {
        let saved = update(&store, id, 0, None, Some(Some(note)), None, None)
            .entry
            .expect("nonempty plain text must remain an annotation");
        assert_eq!(saved.memo.as_deref(), Some(*note));

        // A fresh store reads the durable project rather than an in-memory draft.
        let reopened = ProjectsStore::new(root.clone()).expect("reopen project store");
        let loaded = load_project(&reopened, ProjectRequest { project_id: id })
            .expect("load reopened project");
        assert_eq!(loaded.initial_rows[0].memo.as_deref(), Some(*note));
        let rows = query_project_rows(
            &reopened,
            QueryRowsPayload {
                project_id: id,
                spec: QuerySpec::default(),
                offset: None,
                limit: None,
            },
        )
        .expect("query reopened note");
        assert_eq!(rows.rows[0].memo.as_deref(), Some(*note));
        let updated = update(
            &reopened,
            id,
            0,
            None,
            Some(rows.rows[0].memo.as_deref()),
            None,
            None,
        );
        assert_eq!(updated.entry.unwrap().memo.as_deref(), Some(*note));

        let destination = root.join(format!("note-{case}.csv"));
        export_project(
            &reopened,
            ExportProjectPayload {
                allow_overwrite: false,
                project_id: id,
                destination: destination.to_string_lossy().to_string(),
                scope: Some("all".into()),
                spec: QuerySpec::default(),
                columns: None,
                row_indices: None,
                include_annotations: Some(true),
            },
        )
        .expect("export note");
        let mut reader = csv::Reader::from_path(&destination).expect("read exported CSV");
        let memo_column = reader.headers().unwrap().iter()
            .position(|header| header == "trivium-memo").expect("memo header");
        let record = reader.records().next().unwrap().expect("exported row");
        assert_eq!(record.get(memo_column), Some(*note));

        let restored = ops::projects::create_project(
            &reopened,
            CreateProjectPayload {
                import_options: None,
                path: destination.to_string_lossy().to_string(),
                description: None,
                delimiter: None,
                restore_annotations: Some(true),
            },
        )
        .expect("restore exported annotations");
        assert_eq!(stored(&reopened, restored.meta.id, 0).unwrap().memo.as_deref(), Some(*note));
    }
    // Empty text still clears a memo without turning a whitespace-only note into deletion.
    update(&store, id, 0, None, Some(Some("")), None, None);
    assert_eq!(stored(&store, id, 0), None);
    let _ = fs::remove_dir_all(root);
}

//! T22: tag add/remove patches apply in one direction and to stable row ids.
//!
//! Bulk selection targets are original row ids, so page growth or page-cache
//! eviction in the UI cannot move the update to a different row.

use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use trivium_engine::annotation::{apply_annotation_patch, AnnotationPatch};
use trivium_engine::models::FlagEntry;
use trivium_engine::ops;
use trivium_engine::ops::bulk::{bulk_preview, bulk_update, BulkAnnotatePayload};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::projects::CreateProjectPayload;
use trivium_engine::ops::rows::QuerySpec;
use trivium_engine::storage::load_flags;
use trivium_engine::store::ProjectsStore;

fn write_fixture(root: &std::path::Path) -> PathBuf {
    let path = root.join("annotation-tags.csv");
    let mut body = String::from("ts,user,proc\n");
    for index in 0..6 {
        body.push_str(&format!(
            "2024-01-01T00:00:0{},user{},proc{}\n",
            index, index, index
        ));
    }
    fs::write(&path, body).expect("failed to write fixture");
    path
}

fn setup() -> (PathBuf, ProjectsStore, Uuid) {
    let root = std::env::temp_dir().join(format!("trivium-annotation-tags-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    let store = ProjectsStore::new(root.clone()).expect("failed to create store");
    let csv = write_fixture(&root);
    let summary = ops::projects::create_project(
        &store,
        CreateProjectPayload {
            path: csv.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project");
    (root, store, summary.meta.id)
}

fn entry(tags: &[&str]) -> FlagEntry {
    FlagEntry {
        flag: String::new(),
        memo: None,
        tags: tags.iter().map(|tag| tag.to_string()).collect(),
        bookmarked: false,
    }
}

fn set_tags(store: &ProjectsStore, id: Uuid, row_index: usize, tags: &[&str]) {
    let result = update_flag(
        store,
        UpdateFlagPayload {
            project_id: id,
            row_index,
            flag: None,
            memo: None,
            tags: Some(tags.iter().map(|tag| tag.to_string()).collect()),
            bookmarked: None,
        },
    )
    .expect("failed to update tags");
    assert!(result.entry.is_some(), "tag-only entry must be stored");
}

fn stored(store: &ProjectsStore, id: Uuid, row_index: usize) -> Option<FlagEntry> {
    load_flags(&store.project_dir(&id).join("flags.json"))
        .expect("failed to load flags")
        .get(&row_index)
        .cloned()
}

fn bulk_patch(
    store: &ProjectsStore,
    id: Uuid,
    row_indices: Vec<usize>,
    patch: AnnotationPatch,
) -> usize {
    let payload = BulkAnnotatePayload {
        project_id: id,
        target: "selection".to_string(),
        row_indices: Some(row_indices),
        preview_token: None,
        spec: QuerySpec::default(),
        patch,
    };
    let preview = bulk_preview(store, payload.clone()).expect("preview failed");
    let mut payload = payload;
    payload.preview_token = Some(preview.preview_token);
    bulk_update(store, payload)
        .expect("bulk update failed")
        .updated
}

#[test]
fn add_tags_keeps_existing_tags_and_does_not_duplicate() {
    let patch = AnnotationPatch {
        add_tags: vec!["new".to_string(), "pc".to_string()],
        ..AnnotationPatch::default()
    };
    let existing = entry(&["pc", "triage"]);
    let next = apply_annotation_patch(Some(&existing), &patch).expect("entry must remain");
    assert_eq!(
        next.tags,
        vec!["pc".to_string(), "triage".to_string(), "new".to_string()]
    );
}

#[test]
fn remove_tags_removes_only_the_named_tags() {
    let patch = AnnotationPatch {
        remove_tags: vec!["triage".to_string()],
        ..AnnotationPatch::default()
    };
    let existing = entry(&["pc", "triage", "keep"]);
    let next = apply_annotation_patch(Some(&existing), &patch).expect("entry must remain");
    assert_eq!(next.tags, vec!["pc".to_string(), "keep".to_string()]);
}

#[test]
fn replacement_then_add_then_remove_is_applied_in_order() {
    let patch = AnnotationPatch {
        tags: Some(vec!["b".to_string(), "c".to_string()]),
        add_tags: vec!["c".to_string(), "d".to_string()],
        remove_tags: vec!["a".to_string(), "b".to_string()],
        ..AnnotationPatch::default()
    };
    let existing = entry(&["a", "b"]);
    let next = apply_annotation_patch(Some(&existing), &patch).expect("entry must remain");
    // Replace -> ["b", "c"]; add -> ["b", "c", "d"]; remove -> ["c", "d"].
    assert_eq!(next.tags, vec!["c".to_string(), "d".to_string()]);
}

#[test]
fn tag_patch_deserializes_from_its_ipc_shape() {
    let patch: AnnotationPatch =
        serde_json::from_str(r#"{"add_tags":["from-ui"],"remove_tags":["old"],"tags":null}"#)
            .expect("tag patch");
    assert_eq!(patch.tags, None);
    assert_eq!(patch.add_tags, vec!["from-ui".to_string()]);
    assert_eq!(patch.remove_tags, vec!["old".to_string()]);
}

#[test]
fn bulk_selection_updates_the_same_ids_across_pages() {
    let (root, store, id) = setup();
    // Rows 0, 2, and 5 stand in for a selection that spans page boundaries.
    set_tags(&store, id, 0, &["alpha"]);
    set_tags(&store, id, 1, &["untouched"]);
    set_tags(&store, id, 2, &["beta"]);
    set_tags(&store, id, 5, &["alpha", "beta"]);

    let added = bulk_patch(
        &store,
        id,
        vec![0, 2, 5],
        AnnotationPatch {
            add_tags: vec!["added".to_string()],
            ..AnnotationPatch::default()
        },
    );
    assert_eq!(added, 3);

    let row0 = stored(&store, id, 0).expect("row 0");
    assert_eq!(row0.tags, vec!["alpha".to_string(), "added".to_string()]);
    let row1 = stored(&store, id, 1).expect("row 1");
    assert_eq!(row1.tags, vec!["untouched".to_string()]);
    let row2 = stored(&store, id, 2).expect("row 2");
    assert_eq!(row2.tags, vec!["beta".to_string(), "added".to_string()]);
    let row5 = stored(&store, id, 5).expect("row 5");
    assert_eq!(
        row5.tags,
        vec!["alpha".to_string(), "beta".to_string(), "added".to_string()]
    );

    let removed = bulk_patch(
        &store,
        id,
        vec![0, 2, 5],
        AnnotationPatch {
            remove_tags: vec!["alpha".to_string()],
            ..AnnotationPatch::default()
        },
    );
    assert_eq!(removed, 3);

    assert_eq!(
        stored(&store, id, 0).expect("row 0").tags,
        vec!["added".to_string()]
    );
    assert_eq!(
        stored(&store, id, 2).expect("row 2").tags,
        vec!["beta".to_string(), "added".to_string()]
    );
    assert_eq!(
        stored(&store, id, 5).expect("row 5").tags,
        vec!["beta".to_string(), "added".to_string()]
    );
    assert_eq!(
        stored(&store, id, 1).expect("row 1").tags,
        vec!["untouched".to_string()]
    );

    let _ = fs::remove_dir_all(root);
}

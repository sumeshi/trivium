use std::{collections::HashMap, fs, path::PathBuf};

use serde_json::{json, Value};
use trivium_engine::{
    models::ProjectRow,
    ops::{
        dupes::{duplicate_rows, DupeMode, DupesPayload},
        projects::{create_project, CreateProjectPayload},
    },
    store::ProjectsStore,
};
use uuid::Uuid;

mod common;

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn fixture(records: &[Value]) -> (Fixture, ProjectsStore, Uuid, HashMap<String, String>) {
    let root = Fixture(std::env::temp_dir().join(format!("trivium-dupes-null-{}", Uuid::new_v4())));
    fs::create_dir_all(&root.0).unwrap();
    let source = root.0.join("source.jsonl");
    fs::write(
        &source,
        records
            .iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let store = ProjectsStore::new(root.0.join("workspace")).unwrap();
    let id = create_project(
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
    .id;
    let columns = common::column_ids(&store, id);
    (root, store, id, columns)
}

fn rows(store: &ProjectsStore, id: Uuid, columns: &[String], mode: DupeMode) -> Vec<ProjectRow> {
    duplicate_rows(
        store,
        DupesPayload {
            project_id: id,
            columns: columns.to_vec(),
            mode,
        },
    )
    .unwrap()
    .rows
}

fn ids(rows: &[ProjectRow]) -> Vec<usize> {
    rows.iter().map(|row| row.row_index).collect()
}

#[test]
fn persisted_json_null_is_not_an_empty_string_duplicate_and_non_null_text_semantics_stay_exact() {
    let (_root, store, id, columns) = fixture(&[
        json!({"key": null}),
        json!({"key": ""}),
        json!({"key": "same"}),
        json!({"key": "same"}),
        json!({"key": " same"}),
        json!({"key": 1}),
        json!({"key": "1"}),
        json!({}),
    ]);
    let key = common::id(&columns, "/key");
    let duplicates = rows(&store, id, std::slice::from_ref(&key), DupeMode::Duplicates);
    assert_eq!(ids(&duplicates), [0, 2, 3, 5, 6, 7]);
    let unique = rows(&store, id, std::slice::from_ref(&key), DupeMode::Unique);
    assert_eq!(ids(&unique), [1, 4]);
    assert_eq!(common::cell_text(&unique[0], &key), Some(""));
    assert_eq!(common::cell_text(&unique[1], &key), Some(" same"));
    assert_eq!(common::cell_text(&duplicates[0], &key), None);
}

#[test]
fn null_and_empty_positions_remain_distinct_in_persisted_compound_keys() {
    let (_root, store, id, columns) = fixture(&[
        json!({"left": null, "right": ""}),
        json!({"left": "", "right": null}),
        json!({"left": null, "right": ""}),
        json!({"left": "", "right": ""}),
    ]);
    let columns = [
        common::id(&columns, "/left"),
        common::id(&columns, "/right"),
    ];
    assert_eq!(
        ids(&rows(&store, id, &columns, DupeMode::Duplicates)),
        [0, 2]
    );
    assert_eq!(ids(&rows(&store, id, &columns, DupeMode::Unique)), [1, 3]);
}

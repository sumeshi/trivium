//! T10: the IPC cell DTO carries exact text and a column-level kind.
//!
//! The response is inspected as the same JSON the frontend receives, not just
//! through the Rust struct, so a long integer that would be rounded by a
//! JavaScript `Number` is visible here as an exact decimal string.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::json;
use uuid::Uuid;

use trivium_engine::ops::projects::{
    create_project, load_project, CreateProjectPayload, ProjectRequest,
};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-ipc-cells-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("failed to create temp dir");
    root
}

fn create(store: &ProjectsStore, path: &Path) -> Uuid {
    create_project(
        store,
        CreateProjectPayload {
            path: path.to_string_lossy().to_string(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .expect("failed to create project")
    .meta
    .id
}

fn column_ids(store: &ProjectsStore, id: Uuid) -> std::collections::HashMap<String, String> {
    load_project(store, ProjectRequest { project_id: id })
        .expect("load project")
        .columns_meta
        .into_iter()
        .map(|column| (column.source, column.id))
        .collect()
}

fn query_json(store: &ProjectsStore, id: Uuid) -> serde_json::Value {
    let response = query_project_rows(
        store,
        QueryRowsPayload {
            project_id: id,
            spec: QuerySpec::default(),
            offset: None,
            limit: None,
        },
    )
    .expect("query failed");
    serde_json::to_value(&response).expect("serialize response")
}

#[test]
fn ipc_cells_keep_exact_long_integers_and_distinguish_null_missing_and_empty() {
    let root = temp_root();
    let store = ProjectsStore::new(root.clone()).expect("store");
    let jsonl = root.join("cells.jsonl");
    fs::write(
        &jsonl,
        concat!(
            "{\"big\": 9007199254740993, \"nullv\": null, \"empty\": \"\", \"flag\": true, \"arr\": [1,2,3]}\n",
            "{\"big\": 18446744073709551615, \"nullv\": null, \"empty\": \"\", \"flag\": false, \"arr\": [4], \"missing\": \"present\"}\n",
            "{\"big\": -9223372036854775808, \"nullv\": null, \"empty\": \"\", \"flag\": true, \"arr\": []}\n",
        ),
    )
    .expect("write jsonl");
    let id = create(&store, &jsonl);
    let ids = column_ids(&store, id);
    let response = query_json(&store, id);

    let cell = |row: usize, source: &str| {
        let column = ids.get(source).unwrap_or_else(|| panic!("missing column {}", source));
        response["rows"][row]["cells"][column].clone()
    };

    assert_eq!(
        cell(0, "/big"),
        json!({ "text": "9007199254740993", "kind": "int" })
    );
    assert_eq!(
        cell(1, "/big"),
        json!({ "text": "18446744073709551615", "kind": "int" })
    );
    assert_eq!(
        cell(2, "/big"),
        json!({ "text": "-9223372036854775808", "kind": "int" })
    );

    // No exponent spelling and no rounded neighbor.
    for row in 0..3 {
        let text = cell(row, "/big")["text"].as_str().expect("text").to_string();
        assert!(!text.contains(['e', 'E']), "exponent spelling: {}", text);
        assert!(!text.contains('.'), "non-decimal spelling: {}", text);
    }

    assert_eq!(cell(0, "/nullv"), json!({ "text": null, "kind": "null" }));
    assert_eq!(
        cell(0, "/empty"),
        json!({ "text": "", "kind": "string" })
    );
    assert_eq!(
        cell(0, "/flag"),
        json!({ "text": "true", "kind": "string" })
    );
    assert_eq!(
        cell(1, "/flag"),
        json!({ "text": "false", "kind": "string" })
    );
    assert_eq!(
        cell(0, "/arr"),
        json!({ "text": "[1,2,3]", "kind": "string" })
    );

    // An absent JSON key is "missing", not null and not an empty string.
    assert_eq!(
        cell(0, "/missing"),
        json!({ "text": null, "kind": "missing" })
    );
    assert_eq!(
        cell(1, "/missing"),
        json!({ "text": "present", "kind": "string" })
    );

    // The load path materializes the same cells for the initial page.
    let loaded = load_project(&store, ProjectRequest { project_id: id }).expect("load");
    let loaded_json = serde_json::to_value(&loaded.initial_rows).expect("serialize rows");
    let big = ids.get("/big").expect("big column");
    assert_eq!(
        loaded_json[0]["cells"][big],
        json!({ "text": "9007199254740993", "kind": "int" })
    );
}

//! Duplicate JSON keys must fail import before either value can be discarded.

use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use trivium_engine::import::jsonl::{BatchLimits, JsonImporter, RecordFormat};
use trivium_engine::import::{json, jsonl};
use trivium_engine::jobs::JobContext;
use trivium_engine::ops::cell::{get_cell, GetCellPayload};
use trivium_engine::ops::flags::{update_flag, UpdateFlagPayload};
use trivium_engine::ops::projects::{create_project, CreateProjectPayload};
use trivium_engine::ops::rows::{query_project_rows, QueryRowsPayload, QuerySpec};
use trivium_engine::project_io::{read_original_records, read_project_dataframe};
use trivium_engine::storage::load_flags;
use trivium_engine::store::ProjectsStore;

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("trivium-json-duplicates-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).expect("create temp dir");
    root
}

fn payload(path: &Path) -> CreateProjectPayload {
    CreateProjectPayload {
        import_options: None,
        path: path.to_string_lossy().to_string(),
        description: None,
        delimiter: None,
        restore_annotations: None,
    }
}

#[test]
fn duplicate_keys_report_the_source_record_and_decoded_key() {
    let root = temp_root();
    for (name, source, location, pointer) in [
        (
            "object.json",
            r#"{"user":"alice","user":"bob"}"#,
            "Failed to parse JSON:",
            "/user",
        ),
        (
            "array.json",
            "[{\"user\":\"ok\"},\n{\"user\":\"alice\",\"\\u0075ser\":\"bob\"}]",
            "record 2 in the JSON array",
            "/1/user",
        ),
        (
            "records.jsonl",
            "{\"user\":\"ok\"}\n\n{\"user\":\"alice\",\"user\":\"bob\"}\n",
            "JSONL line 3:",
            "/user",
        ),
        (
            "records.ndjson",
            "{\"user\":\"ok\"}\n\n{\"user\":\"alice\",\"\\u0075ser\":\"bob\"}\n",
            "JSONL line 3:",
            "/user",
        ),
    ] {
        let path = root.join(name);
        fs::write(&path, source).expect("write source");
        let error = if name.ends_with(".json") {
            json::load(&path).err()
        } else {
            jsonl::load(&path).err()
        }
        .expect("duplicate keys must fail")
        .to_string();
        assert!(error.contains(location), "{name}: {error}");
        assert!(
            error.contains("Duplicate JSON key \"user\""),
            "{name}: {error}"
        );
        assert!(error.contains(pointer), "{name}: {error}");
        assert_eq!(fs::read_to_string(&path).expect("source bytes"), source);
    }
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn nested_objects_and_objects_inside_arrays_reject_duplicates() {
    let root = temp_root();
    for (source, pointer) in [
        (r#"{"outer":{"id":1,"id":2}}"#, "/outer/id"),
        (r#"{"items":[{"id":1,"id":2}]}"#, "/items/0/id"),
        (
            r#"{"a/b":{"~":[{"user":1,"\u0075ser":2}]}}"#,
            "/a~1b/~0/0/user",
        ),
    ] {
        let path = root.join("nested.json");
        fs::write(&path, source).expect("write source");
        let error = json::load(&path)
            .err()
            .expect("nested duplicate")
            .to_string();
        assert!(error.contains("Duplicate JSON key"), "{error}");
        assert!(error.contains(pointer), "{error}");
    }
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn separate_objects_keep_same_keys_and_exact_numbers() {
    let root = temp_root();
    let record = r#"{"first":{"id":1},"second":{"id":2},"items":[{"id":3},{"id":4}],"n":170141183460469231731687303715884105728,"decimal":1.2345678901234567890123456789,"exponent":1e+400,"nil":null,"empty":""}"#;
    for (name, source) in [
        ("single.json", record.to_owned()),
        ("array.json", format!("[{record},{{}}]")),
        ("records.jsonl", format!("{record}\n{{}}\n")),
    ] {
        let path = root.join(name);
        fs::write(&path, source).expect("write source");
        let store_root = root.join(format!("store-{name}"));
        let store = ProjectsStore::new(store_root).expect("store");
        let project = create_project(&store, payload(&path)).expect("unique keys import");
        let originals = read_original_records(&store.project_dir(&project.meta.id))
            .expect("stored original records");
        assert_eq!(originals[0]["first"]["id"], 1);
        assert_eq!(originals[0]["second"]["id"], 2);
        assert_eq!(originals[0]["items"][1]["id"], 4);
        assert_eq!(
            originals[0]["n"].to_string(),
            "170141183460469231731687303715884105728"
        );
        assert_eq!(
            originals[0]["decimal"].to_string(),
            "1.2345678901234567890123456789"
        );
        assert_eq!(originals[0]["exponent"].to_string(), "1e+400");
        assert!(originals[0]["nil"].is_null());
        assert_eq!(originals[0]["empty"], "");
        if originals.len() > 1 {
            assert!(
                originals[1].get("nil").is_none(),
                "missing stays distinct from null"
            );
        }
    }
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn failed_duplicate_import_preserves_existing_case_and_removes_staging() {
    let root = temp_root();
    let store = ProjectsStore::new(root.join("store")).expect("store");
    let existing_source = root.join("existing.json");
    fs::write(&existing_source, r#"{"user":"existing"}"#).expect("existing source");
    let existing = create_project(&store, payload(&existing_source)).expect("existing project");
    update_flag(
        &store,
        UpdateFlagPayload {
            project_id: existing.meta.id,
            row_index: 0,
            flag: Some("critical".to_owned()),
            memo: Some(Some("Existing investigation note".to_owned())),
            tags: Some(vec!["review".to_owned()]),
            bookmarked: Some(true),
        },
    )
    .expect("annotate existing case");
    let original_annotations =
        load_flags(&store.flags_path(&existing.meta.id)).expect("existing annotations");
    let original_data =
        fs::read(store.project_dir(&existing.meta.id).join("data.parquet")).expect("existing data");

    for (name, source) in [
        ("bad.json", r#"[{"ok":1},{"user":"alice","user":"bob"}]"#),
        (
            "bad.jsonl",
            "{\"ok\":1}\n{\"user\":\"alice\",\"user\":\"bob\"}\n",
        ),
        (
            "bad.ndjson",
            "{\"ok\":1}\n{\"user\":\"alice\",\"user\":\"bob\"}\n",
        ),
    ] {
        let path = root.join(name);
        fs::write(&path, source).expect("write duplicate source");
        let error = create_project(&store, payload(&path)).expect_err("reject duplicate import");
        assert!(error.to_string().contains("Duplicate JSON key"), "{error}");
        assert_eq!(fs::read_to_string(&path).expect("source bytes"), source);
        assert_eq!(store.all().len(), 1, "no failed project registered");
        assert_eq!(store.all()[0].id, existing.meta.id);
        assert_eq!(
            load_flags(&store.flags_path(&existing.meta.id)).expect("existing annotations"),
            original_annotations
        );
        assert_eq!(
            fs::read_dir(store.staging_root())
                .expect("staging entries")
                .count(),
            0
        );
        assert_eq!(
            fs::read_dir(root.join("store/projects"))
                .expect("project entries")
                .count(),
            1
        );
        assert_eq!(
            fs::read(store.project_dir(&existing.meta.id).join("data.parquet"))
                .expect("existing data"),
            original_data
        );
        let frame =
            read_project_dataframe(&store.project_dir(&existing.meta.id).join("data.parquet"))
                .expect("existing case still readable");
        assert_eq!(frame.height(), 1);
    }
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn second_pass_also_rejects_duplicate_keys() {
    let root = temp_root();
    for (name, format) in [
        ("data.json", RecordFormat::Json),
        ("data.jsonl", RecordFormat::JsonLines),
    ] {
        let path = root.join(name);
        fs::write(&path, r#"{"a":1,"b":2}"#).expect("valid first pass");
        let importer =
            JsonImporter::open(&path, format, BatchLimits::default(), &JobContext::none())
                .expect("scan unique keys");
        // Same byte count intentionally exercises the parser in pass 2 rather
        // than the intervening source-size check.
        fs::write(&path, r#"{"a":1,"a":2}"#).expect("same-size duplicate");
        let mut writes = 0;
        let error = importer
            .write_batches(&JobContext::none(), |_| {
                writes += 1;
                Ok(())
            })
            .err()
            .expect("second pass rejects duplicate");
        assert!(
            error.to_string().contains("Duplicate JSON key \"a\""),
            "{error}"
        );
        assert_eq!(writes, 0);
    }
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn literal_number_marker_survives_import_originals_and_cell_details() {
    let root = temp_root();
    let record = r#"{"$serde_json::private::Number":"not a number","nested":{"$serde_json::private::Number":true},"nil":null,"n":1.234567890123456789}"#;
    for (name, source) in [
        ("single.json", record.to_owned()),
        ("array.json", format!("[{record},{{}}]")),
        ("records.jsonl", format!("{record}\n{{}}\n")),
    ] {
        let path = root.join(name);
        fs::write(&path, source).expect("write source");
        let store = ProjectsStore::new(root.join(format!("store-{name}"))).expect("store");
        let project = create_project(&store, payload(&path)).expect("import marker key");
        let originals = read_original_records(&store.project_dir(&project.meta.id))
            .expect("original records with marker key");
        assert_eq!(originals[0]["$serde_json::private::Number"], "not a number");
        assert_eq!(originals[0]["nested"]["$serde_json::private::Number"], true);

        let rows = query_project_rows(
            &store,
            QueryRowsPayload {
                project_id: project.meta.id,
                spec: QuerySpec::default(),
                offset: None,
                limit: None,
            },
        )
        .expect("row previews");
        for (source, expected_text, expected_kind) in [
            (
                "/$serde_json::private::Number",
                Some("not a number"),
                "string",
            ),
            ("/nested/$serde_json::private::Number", Some("true"), "bool"),
            ("/nil", None, "null"),
            ("/n", Some("1.234567890123456789"), "float"),
        ] {
            let column = project
                .meta
                .columns
                .iter()
                .find(|column| column.source == source)
                .expect("source column");
            let preview = &rows.rows[0].cells[&column.id];
            assert_eq!(preview.text.as_deref(), expected_text, "{name}: {source}");
            assert_eq!(preview.kind, expected_kind, "{name}: {source}");
            let detail = get_cell(
                &store,
                GetCellPayload {
                    project_id: project.meta.id,
                    row_index: 0,
                    column_id: column.id.clone(),
                },
            )
            .expect("full cell details");
            assert_eq!(detail.text.as_deref(), expected_text, "{name}: {source}");
            assert_eq!(detail.kind, expected_kind, "{name}: {source}");
            if rows.rows.len() > 1 {
                assert_eq!(
                    rows.rows[1].cells[&column.id].kind, "missing",
                    "{name}: {source}"
                );
            }
        }
    }
    fs::remove_dir_all(root).expect("cleanup");
}

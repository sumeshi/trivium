//! Format filters use the persisted query path shared by pages, exports and bulk.
use std::{collections::HashMap, fs, path::PathBuf};
use trivium_engine::{
    annotation::AnnotationPatch,
    condition::{Condition, FormatCondition, Group, GroupKind, Node, Op},
    formats::{list_formats, CompiledFormat, FormatDefinition},
    ops::{
        self,
        bulk::BulkAnnotatePayload,
        export::ExportProjectPayload,
        preset::{LoadPresetPayload, SavePresetPayload},
        projects::CreateProjectPayload,
        rows::{QueryRowsPayload, QuerySpec},
    },
    store::ProjectsStore,
};
use uuid::Uuid;
mod common;

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    columns: HashMap<String, String>,
}
impl Fixture {
    fn new(extension: &str, contents: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("trivium-format-condition-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let source = root.join(format!("input.{extension}"));
        fs::write(&source, contents).unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let id = ops::projects::create_project(
            &store,
            CreateProjectPayload {
                path: source.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        drop(store);
        let store = ProjectsStore::new(root.clone()).unwrap();
        let columns = common::column_ids(&store, id);
        Self {
            root,
            store,
            id,
            columns,
        }
    }
    fn query(&self, node: Node) -> Vec<usize> {
        ops::rows::query_project_rows(
            &self.store,
            QueryRowsPayload {
                project_id: self.id,
                spec: QuerySpec {
                    condition: Some(node),
                    ..Default::default()
                },
                offset: Some(0),
                limit: Some(20),
            },
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.row_index)
        .collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn ipv4(root: &std::path::Path) -> FormatDefinition {
    list_formats(&root.join("formats"))
        .unwrap()
        .into_iter()
        .find(|entry| entry.definition.id == "ipv4")
        .unwrap()
        .definition
}
fn format(
    columns: Vec<String>,
    definition: &FormatDefinition,
    equals: Option<&str>,
    negated: bool,
) -> Node {
    Node::Format(FormatCondition {
        columns,
        format: definition.clone(),
        equals: equals.map(str::to_string),
        negated,
    })
}

#[test]
fn validated_patterns_have_the_same_raw_values_in_persisted_search_and_extracted_csv() {
    for (id, values, expected, occurrences, source_rows) in [
        (
            "ipv6",
            vec!["2001:db8:::1", "src=2001:db8::1,2001:db8::1", "::1"],
            "2001:db8::1",
            "2",
            "1",
        ),
        (
            "email",
            vec![
                "bad..user@example.com",
                "o'brien+review@example.technology",
                "o'brien+review@example.technology",
            ],
            "o'brien+review@example.technology",
            "2",
            "2",
        ),
    ] {
        let contents = values
            .iter()
            .map(|value| serde_json::json!({"value":value}).to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let fixture = Fixture::new("jsonl", &contents);
        let column = fixture.columns["/value"].clone();
        let definition = list_formats(&fixture.root.join("formats"))
            .unwrap()
            .into_iter()
            .find(|entry| entry.definition.id == id)
            .unwrap()
            .definition;
        assert!(definition.validation.is_some());
        assert_eq!(
            fixture.query(format(vec![column.clone()], &definition, None, false)),
            [1, 2]
        );
        let result = ops::extract::extract_values(
            &fixture.store,
            ops::extract::ExtractPayload {
                project_id: fixture.id,
                columns: vec![column],
                format: definition,
                scope: ops::extract::ExtractScope::All,
                spec: QuerySpec::default(),
            },
            &trivium_engine::jobs::JobContext::none(),
        )
        .unwrap();
        let columns = common::column_ids(&fixture.store, result.meta.id);
        let page = ops::rows::query_project_rows(
            &fixture.store,
            QueryRowsPayload {
                project_id: result.meta.id,
                spec: QuerySpec::default(),
                offset: Some(0),
                limit: Some(10),
            },
        )
        .unwrap();
        let row = page
            .rows
            .iter()
            .find(|row| row.cells[&columns["Value"]].text.as_deref() == Some(expected))
            .unwrap();
        assert_eq!(
            row.cells[&columns["Count"]].text.as_deref(),
            Some(occurrences)
        );
        assert_eq!(
            row.cells[&columns["Rows"]].text.as_deref(),
            Some(source_rows)
        );
        assert_eq!(
            ops::projects::list_projects(&fixture.store).unwrap().len(),
            1
        );
        let exported = fixture.root.join("extracted.csv");
        ops::export::export_project(
            &fixture.store,
            ExportProjectPayload {
                project_id: result.meta.id,
                destination: exported.to_string_lossy().into(),
                scope: Some("all".into()),
                spec: QuerySpec::default(),
                columns: None,
                row_indices: None,
                include_annotations: Some(false),
                allow_overwrite: false,
            },
        )
        .unwrap();
        let mut csv = csv::Reader::from_path(exported).unwrap();
        assert!(csv
            .records()
            .map(Result::unwrap)
            .any(|row| &row[0] == expected && &row[1] == occurrences && &row[2] == source_rows));
    }
}

#[test]
fn saved_csv_and_jsonl_queries_match_any_source_cell_then_negate_the_aggregate() {
    for (extension,contents,prefix) in [
        ("csv","source_id,alpha,beta\nr0,src=10.0.0.1,phone\nr1,none,\"8.8.8.8,8.8.4.4\"\nr2,none,256.1.2.3\nr3,10.0.0.1,09012345678\nr4,AA,BB\n",""),
        ("jsonl", "{\"source_id\":\"r0\",\"alpha\":\"src=10.0.0.1\",\"beta\":\"phone\"}\n{\"source_id\":\"r1\",\"alpha\":\"none\",\"beta\":\"8.8.8.8,8.8.4.4\"}\n{\"source_id\":\"r2\",\"alpha\":\"none\",\"beta\":\"256.1.2.3\"}\n{\"source_id\":\"r3\",\"alpha\":\"10.0.0.1\",\"beta\":\"09012345678\"}\n{\"source_id\":\"r4\",\"alpha\":\"AA\",\"beta\":\"BB\"}\n", "/"),
    ] {
        let f=Fixture::new(extension,contents);
        let def=ipv4(&f.root);
        let a=common::id(&f.columns,&format!("{prefix}alpha"));
        let b=common::id(&f.columns,&format!("{prefix}beta"));
        let id=common::id(&f.columns,&format!("{prefix}source_id"));
        let columns=vec![a.clone(),b.clone()];
        let node=format(columns.clone(),&def,None,false);
        assert_eq!(f.query(node.clone()),[0,1,3]);
        assert_eq!(f.query(format(columns.clone(),&def,None,true)),[2,4],"NOT applies after checking every selected column");
        assert_eq!(f.query(format(vec![a.clone()],&def,None,false)),[0,3]);
        assert_eq!(f.query(format(columns.clone(),&def,Some("8.8.8.8"),false)),[1]);
        assert!(f.query(format(columns.clone(),&def,Some("8.8.8"),false)).is_empty());
        let manual=Node::Condition(Condition {column:id.clone(),op:Op::Ne,values:vec!["r3".into()],negated:false});
        let combined=Node::Group(Group {kind:GroupKind::And,children:vec![manual,node.clone()]});
        assert_eq!(f.query(combined.clone()),[0,1]);
        let spec=QuerySpec{condition:Some(combined),..Default::default()};
        let destination=f.root.join("matched.csv");
        ops::export::export_project(&f.store,ExportProjectPayload {
            project_id:f.id,destination:destination.to_string_lossy().into_owned(),scope:Some("filtered".into()),
            spec:spec.clone(),columns:Some(vec![id]),row_indices:None,include_annotations:Some(false),allow_overwrite:false,
        }).unwrap();
        let mut output=csv::Reader::from_path(destination).unwrap();
        assert_eq!(output.records().map(|record|record.unwrap()[0].to_owned()).collect::<Vec<_>>(),["r0","r1"]);
        let preview=ops::bulk::bulk_preview(&f.store,BulkAnnotatePayload {
            project_id:f.id,target:"filtered".into(),spec,row_indices:None,
            patch:AnnotationPatch{flag:Some("critical".into()),..Default::default()},preview_token:None,
        }).unwrap();
        assert_eq!(preview.count,2);
        let mut cross=def.clone(); cross.id="cross-cell".into(); cross.name="Cross cell".into();
        cross.pattern="(?i)aa bb".into();cross.value_group=None;cross.examples=None;
        assert!(f.query(format(columns,&cross,None,false)).is_empty(),"formats never search a joined or lowercased row blob");
    }
}

#[test]
fn format_snapshots_and_all_column_references_survive_portable_presets() {
    let first = Fixture::new("csv", "alpha,beta\n10.0.0.1,none\nnone,8.8.8.8\n");
    let second = Fixture::new("csv", "beta,alpha\nnone,10.0.0.1\n8.8.8.8,none\n");
    let definition = ipv4(&first.root);
    let node = format(
        vec![
            common::id(&first.columns, "alpha"),
            common::id(&first.columns, "beta"),
        ],
        &definition,
        Some("10.0.0.1"),
        false,
    );
    let path = first.root.join("format-preset.json");
    ops::preset::save_preset(
        &first.store,
        SavePresetPayload {
            project_id: first.id,
            name: None,
            condition: Some(node),
            hidden_columns: vec![],
            sort_key: None,
            sort_direction: None,
            column_order: None,
            column_widths: None,
            destination: path.to_string_lossy().into_owned(),
            allow_overwrite: false,
        },
    )
    .unwrap();
    // Older saved views/presets can still carry removed classification metadata.
    // Loading must retain their frozen matcher and columns without reviving it.
    let mut saved: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    saved["condition"]["format"]["classifier"] = serde_json::json!({
        "kind": "ipv4-cidr", "default": "Unclassified",
        "rules": [{"cidr": "10.0.0.0/8", "label": "Private"}]
    });
    fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
    let loaded = ops::preset::load_preset(
        &second.store,
        LoadPresetPayload {
            project_id: Some(second.id),
            source: path.to_string_lossy().into_owned(),
        },
    )
    .unwrap();
    assert!(loaded.unknown_columns.is_empty());
    let node = loaded.condition.unwrap();
    let Node::Format(condition) = &node else {
        panic!("format predicate was lost")
    };
    assert_eq!(
        condition.format, definition,
        "preset stores its applied definition rather than a mutable catalog lookup"
    );
    assert!(serde_json::to_value(&node).unwrap()["format"]
        .get("classifier")
        .is_none());
    assert_eq!(
        condition.columns,
        vec![
            common::id(&second.columns, "alpha"),
            common::id(&second.columns, "beta")
        ]
    );
    assert_eq!(second.query(node), [0]);
}

#[test]
fn invalid_format_scope_is_rejected_and_missing_cells_have_normal_negation_semantics() {
    let f = Fixture::new("jsonl", "{\"a\":\"10.0.0.1\"}\n{\"a\":null}\n{}\n");
    let def = ipv4(&f.root);
    let a = common::id(&f.columns, "/a");
    assert_eq!(f.query(format(vec![a], &def, None, true)), [1, 2]);
    for columns in [vec![], vec!["not-a-column".into()], vec!["".into()]] {
        let result = ops::rows::query_project_rows(
            &f.store,
            QueryRowsPayload {
                project_id: f.id,
                spec: QuerySpec {
                    condition: Some(format(columns, &def, None, false)),
                    ..Default::default()
                },
                offset: None,
                limit: None,
            },
        );
        assert!(result.is_err());
    }
    assert!(CompiledFormat::new(&def)
        .unwrap()
        .has_match("10.0.0.1", None));
}

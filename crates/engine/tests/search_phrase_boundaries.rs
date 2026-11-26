//! A phrase must occur in one source cell; row-wide AND may span cells.
use std::{collections::HashMap, fs, path::PathBuf};

use polars::prelude::*;
use serde_json::{json, Value};
use uuid::Uuid;

use trivium_engine::{
    annotation::AnnotationPatch,
    condition::{eval, search_to_node, Condition, Node, Op},
    jobs::JobContext,
    ops::{
        bulk::{bulk_preview, bulk_update, BulkAnnotatePayload},
        export::{export_project, ExportProjectPayload},
        flags::{set_hidden_columns, HiddenColumnsPayload},
        projects::{create_project, CreateProjectPayload},
        rows::{query_project_rows, resolve_target, QueryRowsPayload, QuerySpec},
    },
    storage::load_flags,
    store::ProjectsStore,
};

mod common;

fn unscoped(op: Op, text: &str, negated: bool) -> Node {
    Node::Condition(Condition {
        column: String::new(),
        op,
        values: vec![text.into()],
        negated,
    })
}

#[test]
fn phrase_boundaries_negation_and_selected_scope_share_the_prepared_query() {
    let frame = DataFrame::new(vec![
        Series::new(
            "first",
            [
                Some("failed"),
                Some("failed login"),
                Some("none"),
                Some("failed "),
                Some(""),
                Some("failed failed"),
                Some(""),
                Some("ΟΣ"),
                None,
            ],
        ),
        Series::new(
            "second",
            [
                Some("login"),
                Some("none"),
                Some("FAILED LOGIN"),
                Some(" login"),
                Some(""),
                Some("login"),
                Some(""),
                Some("Α Β"),
                None,
            ],
        ),
        Series::new(
            "hidden",
            [
                Some("nothing"),
                Some("other"),
                Some("other"),
                Some("other"),
                Some("failed login"),
                Some(""),
                Some(""),
                Some(""),
                None,
            ],
        ),
    ])
    .unwrap();
    let columns = vec!["first".into(), "second".into(), "hidden".into()];
    for (query, expected) in [
        ("\"failed login\"", vec![1, 2, 4]),
        ("failed login", vec![0, 1, 2, 3, 4, 5]),
        ("-\"failed login\"", vec![0, 3, 5, 6, 7, 8]),
        ("\"failed failed\"", vec![5]),
        ("\"failed login\" | first:ΟΣ", vec![1, 2, 4, 7]),
        ("\"ΟΣ Α\"", vec![]),
        ("\"α β\"", vec![7]),
    ] {
        for converted in [false, true] {
            let spec = QuerySpec {
                search: (!converted).then(|| query.into()),
                condition: if converted {
                    search_to_node(query).unwrap()
                } else {
                    None
                },
                ..Default::default()
            };
            let target = resolve_target(
                &frame,
                &spec,
                &columns,
                &HashMap::new(),
                &JobContext::none(),
            )
            .unwrap();
            assert_eq!(target.filtered, expected, "{spec:?}");
        }
    }
    for (scope, expected) in [
        (None, vec![1, 2, 4]),
        (Some(vec![]), vec![1, 2, 4]),
        (Some(vec!["FIRST".into(), "second".into()]), vec![1, 2]),
        (Some(vec!["first".into()]), vec![1]),
        (Some(vec!["hidden".into()]), vec![4]),
    ] {
        let spec = QuerySpec {
            condition: Some(unscoped(Op::Contains, "failed login", false)),
            search_columns: scope,
            ..Default::default()
        };
        let target = resolve_target(
            &frame,
            &spec,
            &columns,
            &HashMap::new(),
            &JobContext::none(),
        )
        .unwrap();
        assert_eq!(target.filtered, expected, "{spec:?}");
    }
    let explicit = QuerySpec {
        search: Some("second:\"failed login\"".into()),
        search_columns: Some(vec!["first".into()]),
        ..Default::default()
    };
    assert_eq!(
        resolve_target(
            &frame,
            &explicit,
            &columns,
            &HashMap::new(),
            &JobContext::none()
        )
        .unwrap()
        .filtered,
        [2]
    );
    let joined_regex = QuerySpec {
        condition: Some(unscoped(Op::Regex, "^failed login", false)),
        ..Default::default()
    };
    assert_eq!(
        resolve_target(
            &frame,
            &joined_regex,
            &columns,
            &HashMap::new(),
            &JobContext::none()
        )
        .unwrap()
        .filtered,
        [0, 1, 4]
    );
}

#[test]
fn owned_condition_evaluation_does_not_search_its_joined_cache_for_contains() {
    let split = HashMap::from([
        ("first".into(), json!("failed")),
        ("second".into(), json!("login")),
        (String::new(), json!("failed login")),
    ]);
    let missing = HashMap::from([
        ("first".into(), Value::Null),
        ("second".into(), Value::Null),
        (String::new(), json!("")),
    ]);
    for row in [&split, &missing] {
        assert!(!eval(&unscoped(Op::Contains, "failed login", false), row).unwrap());
        assert!(eval(&unscoped(Op::Contains, "failed login", true), row).unwrap());
    }
    assert!(eval(&unscoped(Op::Regex, "^failed login$", false), &split).unwrap());
}

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    columns: HashMap<String, String>,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-phrase-cells-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("source.csv");
        fs::write(
            &path,
            concat!(
                "id,first,second,hidden\n",
                "0,failed,login,nothing\n",
                "1,failed login,none,other\n",
                "2,none,FAILED LOGIN,other\n",
                "3,failed , login,other\n",
                "4,,,failed login\n",
                "5,failed failed,login,\n",
                "6,,,\n",
            ),
        )
        .unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let id = create_project(
            &store,
            CreateProjectPayload {
                import_options: None,
                path: path.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap()
        .meta
        .id;
        let columns = common::column_ids(&store, id);
        Self {
            root,
            store,
            id,
            columns,
        }
    }

    fn rows(&self, spec: QuerySpec) -> Vec<usize> {
        query_project_rows(
            &self.store,
            QueryRowsPayload {
                project_id: self.id,
                spec,
                offset: Some(0),
                limit: Some(100),
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

#[test]
fn imported_phrase_membership_is_identical_in_hidden_projected_query_export_and_bulk() {
    let fixture = Fixture::new();
    let id_column = common::id(&fixture.columns, "id");
    set_hidden_columns(
        &fixture.store,
        HiddenColumnsPayload {
            project_id: fixture.id,
            hidden_columns: ["first", "second", "hidden"]
                .map(|name| common::id(&fixture.columns, name))
                .into(),
        },
    )
    .unwrap();
    let spec = QuerySpec {
        search: Some("\"failed login\"".into()),
        sort_key: Some(id_column.clone()),
        sort_direction: Some("desc".into()),
        ..Default::default()
    };
    assert_eq!(fixture.rows(spec.clone()), [4, 2, 1]);
    assert_eq!(
        fixture.rows(QuerySpec {
            search: Some("SeCoNd:\"failed login\"".into()),
            ..Default::default()
        }),
        [2]
    );
    assert_eq!(
        fixture.rows(QuerySpec {
            search: None,
            condition: search_to_node("\"failed login\"").unwrap(),
            search_columns: Some(vec![common::id(&fixture.columns, "first")]),
            ..Default::default()
        }),
        [1]
    );
    let destination = fixture.root.join("phrase.csv");
    let exported = export_project(
        &fixture.store,
        ExportProjectPayload {
            project_id: fixture.id,
            destination: destination.to_string_lossy().into_owned(),
            allow_overwrite: false,
            scope: Some("filtered".into()),
            spec: spec.clone(),
            columns: Some(vec![id_column]),
            row_indices: None,
            include_annotations: Some(false),
        },
    )
    .unwrap();
    assert_eq!(exported.rows, 3);
    let mut reader = csv::Reader::from_path(destination).unwrap();
    assert_eq!(reader.headers().unwrap().iter().collect::<Vec<_>>(), ["id"]);
    assert_eq!(
        reader
            .records()
            .map(|row| row.unwrap()[0].to_owned())
            .collect::<Vec<_>>(),
        ["4", "2", "1"]
    );
    let mut payload = BulkAnnotatePayload {
        project_id: fixture.id,
        target: "filtered".into(),
        row_indices: None,
        spec,
        patch: AnnotationPatch {
            flag: Some("critical".into()),
            ..Default::default()
        },
        preview_token: None,
    };
    let preview = bulk_preview(&fixture.store, payload.clone()).unwrap();
    assert_eq!(preview.count, 3);
    payload.preview_token = Some(preview.preview_token);
    assert_eq!(bulk_update(&fixture.store, payload).unwrap().updated, 3);
    let flags = load_flags(&fixture.store.flags_path(&fixture.id)).unwrap();
    let mut annotated = flags
        .into_iter()
        .map(|(index, entry)| {
            assert_eq!(entry.flag, "critical");
            index
        })
        .collect::<Vec<_>>();
    annotated.sort_unstable();
    assert_eq!(annotated, [1, 2, 4]);
}

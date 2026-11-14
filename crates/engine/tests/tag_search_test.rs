//! Reserved tag operands share the normal Boolean query and annotation revision.
use std::{collections::HashMap, fs, path::PathBuf, sync::Mutex};

use trivium_engine::{
    annotation::AnnotationPatch,
    condition::{Condition, Node, Op},
    jobs::JobContext,
    models::IocEntry,
    ops::{
        self,
        bulk::{BulkAnnotatePayload, BulkUndoPayload},
        export::ExportProjectPayload,
        flags::UpdateFlagPayload,
        group::GroupTreePayload,
        iocs::SaveIocsPayload,
        projects::CreateProjectPayload,
        rows::{
            clear_result_cache, resolve_project_target, QueryRowsPayload, QueryRowsResponse,
            QuerySpec,
        },
        stats::ColumnStatsPayload,
    },
    project_io::{project_read_stats, reset_project_read_stats},
    store::ProjectsStore,
    value_utils::{reset_stringified_columns, stringified_column_count},
};
use uuid::Uuid;

mod common;
static SERIAL: Mutex<()> = Mutex::new(());

struct Fixture {
    root: PathBuf,
    store: ProjectsStore,
    id: Uuid,
    columns: HashMap<String, String>,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("trivium-tag-search-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("events.csv");
        fs::write(&path, "message,tag,kind\nalpha alert,source,a\nbeta normal,review source,b\nalpha normal,source,a\ngamma alert,review,c\nliteral @tag:review,other,d\nbeta alert,tag,b\n").unwrap();
        let store = ProjectsStore::new(root.clone()).unwrap();
        let project = ops::projects::create_project(
            &store,
            CreateProjectPayload {
                path: path.to_string_lossy().into_owned(),
                description: None,
                delimiter: None,
                restore_annotations: None,
            },
        )
        .unwrap();
        let id = project.meta.id;
        let columns = common::column_ids(&store, id);
        let fixture = Self {
            root,
            store,
            id,
            columns,
        };
        for (row, tags) in [
            (0, vec!["Review", "triage: mail|review"]),
            (1, vec!["reviewed"]),
            (
                2,
                vec![
                    "review",
                    "*",
                    "A \"quote\" \\ path %20 | 日本語",
                    "%20",
                    " spaced ",
                    "日本語",
                ],
            ),
            (4, vec!["todo"]),
            (5, vec!["literal%20", "NUL\0tag"]),
        ] {
            fixture.tags(row, tags);
        }
        fixture
    }

    fn tags(&self, row_index: usize, tags: Vec<&str>) {
        ops::flags::update_flag(
            &self.store,
            UpdateFlagPayload {
                project_id: self.id,
                row_index,
                flag: None,
                memo: None,
                tags: Some(tags.into_iter().map(str::to_owned).collect()),
                bookmarked: None,
            },
        )
        .unwrap();
    }

    fn query(&self, search: &str) -> QueryRowsResponse {
        self.query_spec(QuerySpec {
            search: Some(search.into()),
            ..Default::default()
        })
    }

    fn query_spec(&self, spec: QuerySpec) -> QueryRowsResponse {
        ops::rows::query_project_rows(
            &self.store,
            QueryRowsPayload {
                project_id: self.id,
                spec,
                offset: Some(0),
                limit: Some(50),
            },
        )
        .unwrap()
    }

    fn ids(&self, search: &str) -> Vec<usize> {
        self.query(search)
            .rows
            .into_iter()
            .map(|row| row.row_index)
            .collect()
    }

    fn bulk(&self, search: &str, patch: AnnotationPatch) -> usize {
        let mut payload = BulkAnnotatePayload {
            project_id: self.id,
            target: "filtered".into(),
            row_indices: None,
            spec: QuerySpec {
                search: Some(search.into()),
                ..Default::default()
            },
            patch,
            preview_token: None,
        };
        let preview = ops::bulk::bulk_preview(&self.store, payload.clone()).unwrap();
        payload.preview_token = Some(preview.preview_token);
        ops::bulk::bulk_update(&self.store, payload)
            .unwrap()
            .updated
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn encoded_tag(tag: &str) -> String {
    tag.as_bytes()
        .iter()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(byte) {
                char::from(*byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

#[test]
fn boolean_tag_search_keeps_source_terms_scope_and_exact_tag_identity() {
    let _serial = SERIAL.lock().unwrap();
    let f = Fixture::new();
    for (search, expected) in [
        ("@tag:review", vec![0, 2]),
        ("@TAG:\"REVIEW\"", vec![0, 2]),
        ("@tag:rev", vec![]),
        ("@tag:reviewed", vec![1]),
        ("-@tag:review", vec![1, 3, 4, 5]),
        ("@tag:*", vec![0, 1, 2, 4, 5]),
        ("-@tag:*", vec![3]),
        ("@tag:\"*\"", vec![2]),
        ("@tag:%2A", vec![2]),
        ("tag:review", vec![1, 3]),
        ("review", vec![1, 3, 4]),
        ("message:alert @tag:review", vec![0]),
        ("@tag:review|message:gamma", vec![0, 2, 3]),
        ("@tag:review message:alert|message:gamma", vec![0, 3]),
        ("message:alpha|-@tag:review", vec![0, 1, 2, 3, 4, 5]),
        ("@tag:review|todo", vec![0, 2, 4]),
        ("@tag:review|:gamma", vec![0, 2, 3]),
    ] {
        assert_eq!(f.ids(search), expected, "{search}");
    }
    assert!(f
        .query("@tag:review")
        .rows
        .iter()
        .all(|row| row.memo.is_none()));
    let unflagged = f.query_spec(QuerySpec {
        search: Some("@tag:review".into()), flag_filter: Some("none".into()), ..Default::default()
    });
    assert_eq!(unflagged.rows.iter().map(|row| row.row_index).collect::<Vec<_>>(), [0, 2]);
    assert!(f.query_spec(QuerySpec {
        search: Some("@tag:review".into()), flag_filter: Some("safe".into()), ..Default::default()
    }).rows.is_empty(), "tag-only rows must not be treated as Safe");
    let chosen_source = f.query_spec(QuerySpec {
        search: Some("@tag:review|:gamma".into()),
        search_columns: Some(vec![f.columns["tag"].clone()]),
        ..Default::default()
    });
    assert_eq!(
        chosen_source
            .rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        [0, 2]
    );
    let condition = Node::Condition(Condition {
        column: f.columns["kind"].clone(),
        op: Op::Eq,
        values: vec!["a".into()],
        negated: false,
    });
    let filtered = f.query_spec(QuerySpec {
        search: Some("@tag:review|message:gamma".into()),
        condition: Some(condition),
        ..Default::default()
    });
    assert_eq!(
        filtered
            .rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        [0, 2]
    );
    let descending = f.query_spec(QuerySpec {
        search: Some("@tag:review".into()),
        sort_key: Some(f.columns["message"].clone()),
        sort_direction: Some("desc".into()),
        ..Default::default()
    });
    assert_eq!(
        descending
            .rows
            .iter()
            .map(|row| row.row_index)
            .collect::<Vec<_>>(),
        [2, 0]
    );
}

#[test]
fn quoted_and_encoded_tags_preserve_arbitrary_literals_without_syntax_injection() {
    let _serial = SERIAL.lock().unwrap();
    let f = Fixture::new();
    assert_eq!(f.ids("@tag:\"triage: mail|review\""), [0]);
    assert_eq!(f.ids("@tag:\"%20\""), [2]);
    for (tag, row) in [
        ("A \"quote\" \\ path %20 | 日本語", 2),
        ("%20", 2),
        (" spaced ", 2),
        ("日本語", 2),
        ("literal%20", 5),
        ("NUL\0tag", 5),
    ] {
        assert_eq!(
            f.ids(&format!("@tag:{}", encoded_tag(tag))),
            [row],
            "{tag:?}"
        );
    }
    assert!(f.ids("@tag:literal%20").is_empty());
    for search in ["@tag:", "@tag:%", "@tag:%GG", "@tag:%FF"] {
        assert!(
            ops::rows::query_project_rows(
                &f.store,
                QueryRowsPayload {
                    project_id: f.id,
                    spec: QuerySpec {
                        search: Some(search.into()),
                        ..Default::default()
                    },
                    offset: None,
                    limit: None
                }
            )
            .is_err(),
            "{search}"
        );
    }
    assert_eq!(
        f.ids("@tag:review"),
        [0, 2],
        "invalid operands cannot poison a later valid query"
    );
}

#[test]
fn tag_only_resolution_reads_annotations_without_source_columns_or_full_text() {
    let _serial = SERIAL.lock().unwrap();
    let f = Fixture::new();
    clear_result_cache();
    reset_project_read_stats();
    reset_stringified_columns();
    let resolved = resolve_project_target(
        &f.store,
        f.id,
        &QuerySpec {
            search: Some("@tag:review".into()),
            search_columns: Some(vec!["unused-source-scope".into()]),
            ..Default::default()
        },
        &JobContext::none(),
    )
    .unwrap();
    assert_eq!(*resolved.ids, [0, 2]);
    assert_eq!(
        resolved.annotated_filtered, 0,
        "tag-only rows are not flagged"
    );
    assert_eq!(project_read_stats().column_bytes, 0);
    assert_eq!(project_read_stats().decoded_rows, 0);
    for id in f.columns.values() {
        assert_eq!(stringified_column_count(id), 0);
    }
}

#[test]
fn tag_cache_membership_changes_after_single_bulk_undo_and_ioc_application() {
    let _serial = SERIAL.lock().unwrap();
    let f = Fixture::new();
    assert_eq!(f.ids("@tag:review"), [0, 2]);
    assert_eq!(f.ids("@tag:review"), [0, 2]);
    f.tags(1, vec!["review"]);
    assert_eq!(f.ids("@tag:review"), [0, 1, 2]);
    f.tags(0, vec![]);
    assert_eq!(f.ids("@tag:review"), [1, 2]);
    assert_eq!(
        f.bulk(
            "@tag:review",
            AnnotationPatch {
                add_tags: vec!["batch".into()],
                ..Default::default()
            }
        ),
        2
    );
    assert_eq!(f.ids("@tag:batch"), [1, 2]);
    assert_eq!(
        f.bulk(
            "@tag:review",
            AnnotationPatch {
                remove_tags: vec!["review".into()],
                ..Default::default()
            }
        ),
        2
    );
    assert!(f.ids("@tag:review").is_empty());
    ops::bulk::bulk_undo(&f.store, BulkUndoPayload { project_id: f.id }).unwrap();
    assert_eq!(f.ids("@tag:review"), [1, 2]);
    assert_eq!(f.ids("@tag:batch"), [1, 2]);
    assert!(f.ids("@tag:ioc").is_empty());
    let source_query = format!("{}:b", f.columns["kind"]);
    ops::iocs::save_iocs(
        &f.store,
        SaveIocsPayload {
            project_id: f.id,
            entries: vec![IocEntry {
                flag: "critical".into(),
                tag: "ioc".into(),
                query: source_query.clone(),
            }],
        },
    )
    .unwrap();
    assert_eq!(
        f.ids("@tag:ioc"),
        Vec::<usize>::new(),
        "saving a definition does not annotate rows"
    );
    assert_eq!(
        f.bulk(
            &source_query,
            AnnotationPatch {
                add_tags: vec!["ioc".into()],
                flag: Some("critical".into()),
                ..Default::default()
            }
        ),
        2
    );
    assert_eq!(f.ids("@tag:ioc"), [1, 5]);
    ops::bulk::bulk_undo(&f.store, BulkUndoPayload { project_id: f.id }).unwrap();
    assert!(f.ids("@tag:ioc").is_empty());
}

#[test]
fn tag_result_scope_is_shared_by_group_stats_export_and_bulk_preview() {
    let _serial = SERIAL.lock().unwrap();
    let f = Fixture::new();
    let search = "@tag:review";
    let stats = ops::stats::column_stats(
        &f.store,
        ColumnStatsPayload {
            project_id: f.id,
            column: f.columns["kind"].clone(),
            rare_max: 1,
            spec: QuerySpec {
                search: Some(search.into()),
                ..Default::default()
            },
        },
    )
    .unwrap();
    assert_eq!(stats.top.len(), 1);
    assert_eq!(stats.top[0].value, "a");
    assert_eq!(stats.top[0].count, 2);
    let grouped = ops::group::group_tree(
        &f.store,
        GroupTreePayload {
            project_id: f.id,
            search: Some(search.into()),
            columns: None,
            search_columns: None,
            flag_filter: None,
            bookmarked_only: None,
            condition: None,
            group_columns: vec![f.columns["kind"].clone()],
            group_granularities: vec![],
        },
    )
    .unwrap();
    assert_eq!(grouped.total, 2);
    assert_eq!(grouped.nodes[0].label, "a");
    assert_eq!(grouped.nodes[0].count, 2);
    let destination = f.root.join("tags.csv");
    let exported = ops::export::export_project(
        &f.store,
        ExportProjectPayload {
            project_id: f.id,
            destination: destination.to_string_lossy().into_owned(),
            scope: Some("filtered".into()),
            spec: QuerySpec {
                search: Some(search.into()),
                ..Default::default()
            },
            columns: Some(vec![f.columns["message"].clone()]),
            row_indices: None,
            include_annotations: Some(false),
        },
    )
    .unwrap();
    assert_eq!(exported.rows, 2);
    let output = fs::read_to_string(destination).unwrap();
    assert_eq!(
        output.lines().collect::<Vec<_>>(),
        ["message", "alpha alert", "alpha normal"]
    );
    let preview = ops::bulk::bulk_preview(
        &f.store,
        BulkAnnotatePayload {
            project_id: f.id,
            target: "filtered".into(),
            row_indices: None,
            spec: QuerySpec {
                search: Some(search.into()),
                ..Default::default()
            },
            patch: AnnotationPatch::default(),
            preview_token: None,
        },
    )
    .unwrap();
    assert_eq!(preview.count, 2);
}

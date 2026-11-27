use std::collections::HashMap;

use polars::prelude::*;
use trivium_engine::{
    condition::{search_to_node, Condition, Group, GroupKind, Node, Op},
    jobs::{queue_job, request_cancel, JobContext, JobKind},
    models::FlagEntry,
    ops::rows::{resolve_target, QuerySpec},
};

fn general(node: Node) -> Node {
    Node::Group(Group {
        kind: GroupKind::And,
        children: vec![Node::Group(Group {
            kind: GroupKind::Or,
            children: vec![node],
        })],
    })
}

fn columns(frame: &DataFrame) -> Vec<String> {
    frame
        .get_column_names()
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn entry(flag: &str, bookmarked: bool) -> FlagEntry {
    FlagEntry {
        flag: flag.into(),
        memo: None,
        tags: vec!["review".into()],
        bookmarked,
    }
}

#[test]
fn simple_and_or_matches_general_evaluation_with_scopes_and_null_chunks() {
    let mut first = Series::new(
        "first",
        [Some("PowerShell"), None, Some("power"), Some("foo bar")],
    );
    first
        .append(&Series::new(
            "first",
            [Some("ΟΣ"), Some("İstanbul"), Some("aaaab"), None],
        ))
        .unwrap();
    let mut second = Series::new(
        "second",
        [
            Some("event"),
            Some("powershell EVENT"),
            Some("shell event"),
            None,
        ],
    );
    second
        .append(&Series::new(
            "second",
            [Some("β"), Some("event"), Some(""), Some("POWERSHELL EVENT")],
        ))
        .unwrap();
    let frame = DataFrame::new(vec![first, second]).unwrap();
    let flags = HashMap::from([
        (0, entry("critical", true)),
        (4, entry("safe", false)),
        (7, entry("critical", true)),
    ]);
    for search in [
        "powershell event",
        "powershell event -missing",
        "powershell|event",
        "powershell|event|missing",
        "-powershell event",
        "-powershell|-event",
        "first:powershell second:event",
        "first:powershell|second:event",
        "\"powershell event\"|ΟΣ",
        "ς|i",
        "powershell|event foo",
        "@tag:review event",
    ] {
        let node = search_to_node(search).unwrap().unwrap();
        for scope in [
            None,
            Some(vec![]),
            Some(vec!["first".into()]),
            Some(vec!["second".into()]),
        ] {
            for (flag_filter, bookmarked_only) in [
                (None, None),
                (Some("critical"), None),
                (Some("none"), None),
                (None, Some(true)),
            ] {
                let query = |condition| {
                    resolve_target(
                        &frame,
                        &QuerySpec {
                            condition: Some(condition),
                            search_columns: scope.clone(),
                            flag_filter: flag_filter.map(str::to_string),
                            bookmarked_only,
                            ..Default::default()
                        },
                        &columns(&frame),
                        &flags,
                        &JobContext::none(),
                    )
                    .unwrap()
                };
                if search.starts_with("@tag:") {
                    // Metadata markers belong to the search tokenizer.
                    let result = resolve_target(
                        &frame,
                        &QuerySpec {
                            search: Some(search.into()),
                            search_columns: scope.clone(),
                            flag_filter: flag_filter.map(str::to_string),
                            bookmarked_only,
                            ..Default::default()
                        },
                        &columns(&frame),
                        &flags,
                        &JobContext::none(),
                    )
                    .unwrap();
                    assert!(result.filtered.iter().all(|row| flags.contains_key(row)));
                } else {
                    assert_eq!(
                        query(node.clone()),
                        query(general(node.clone())),
                        "{search}, {scope:?}, {flag_filter:?}"
                    );
                }
            }
        }
    }
    for kind in [GroupKind::And, GroupKind::Or] {
        let node = Node::Group(Group {
            kind,
            children: vec![],
        });
        let query = |condition| {
            resolve_target(
                &frame,
                &QuerySpec {
                    condition: Some(condition),
                    ..Default::default()
                },
                &columns(&frame),
                &flags,
                &JobContext::none(),
            )
            .unwrap()
        };
        assert_eq!(query(node.clone()), query(general(node)));
    }
}

#[test]
fn typed_columns_keep_the_general_text_adapter() {
    let frame = DataFrame::new(vec![
        Series::new("number", [7i64, 8, 9]),
        Series::new("text", [Some("event"), Some("powershell"), None]),
    ])
    .unwrap();
    for search in [
        "7 event",
        "9|powershell",
        "-7 -event",
        "number:8 text:powershell",
    ] {
        let node = search_to_node(search).unwrap().unwrap();
        let query = |condition| {
            resolve_target(
                &frame,
                &QuerySpec {
                    condition: Some(condition),
                    ..Default::default()
                },
                &columns(&frame),
                &HashMap::new(),
                &JobContext::none(),
            )
            .unwrap()
        };
        assert_eq!(query(node.clone()), query(general(node)));
    }
}

#[test]
fn no_eligible_rows_still_validate_conditions_and_observe_cancellation() {
    let frame = DataFrame::new(vec![Series::new("text", ["powershell event"])]).unwrap();
    for node in [
        Node::Condition(Condition {
            column: "missing".into(),
            op: Op::Contains,
            values: vec!["x".into()],
            negated: false,
        }),
        Node::Condition(Condition {
            column: "text".into(),
            op: Op::Regex,
            values: vec!["[".into()],
            negated: false,
        }),
        Node::Condition(Condition {
            column: "text".into(),
            op: Op::Gt,
            values: vec!["invalid bound".into()],
            negated: false,
        }),
    ] {
        assert!(resolve_target(
            &frame,
            &QuerySpec {
                condition: Some(node),
                flag_filter: Some("critical".into()),
                ..Default::default()
            },
            &columns(&frame),
            &HashMap::new(),
            &JobContext::none()
        )
        .is_err());
    }
    let guard = queue_job(JobKind::Query, None).unwrap();
    request_cancel(guard.info().id);
    let result = resolve_target(
        &frame,
        &QuerySpec {
            search: Some("-missing event".into()),
            flag_filter: Some("critical".into()),
            ..Default::default()
        },
        &columns(&frame),
        &HashMap::new(),
        &JobContext::with_job(guard, None),
    );
    assert_eq!(result.unwrap_err().to_string(), "Cancelled.");
}

#[test]
fn annotation_prefilter_keeps_source_ids_and_tags_across_reader_batches() {
    use trivium_engine::{
        ops::{
            flags::{update_flag, UpdateFlagPayload},
            projects::{create_project, CreateProjectPayload},
            rows::{query_project_rows, QueryRowsPayload},
        },
        store::ProjectsStore,
    };

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.csv");
    std::fs::write(&path, format!("text\n{}", "event\n".repeat(16_400))).unwrap();
    let store = ProjectsStore::new(directory.path().join("cases")).unwrap();
    let id = create_project(
        &store,
        CreateProjectPayload {
            path: path.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
            import_options: None,
        },
    )
    .unwrap()
    .meta
    .id;
    let expected = vec![8191, 8192, 16_384];
    for index in &expected {
        update_flag(
            &store,
            UpdateFlagPayload {
                project_id: id,
                row_index: *index,
                flag: Some("critical".into()),
                memo: None,
                tags: Some(vec!["review".into()]),
                bookmarked: Some(true),
            },
        )
        .unwrap();
    }
    for search in [
        None,
        Some("-missing event"),
        Some("@tag:review event"),
        Some("-missing|-absent"),
    ] {
        for (flag_filter, bookmarked_only) in [(Some("critical"), None), (None, Some(true))] {
            let result = query_project_rows(
                &store,
                QueryRowsPayload {
                    project_id: id,
                    spec: QuerySpec {
                        search: search.map(str::to_string),
                        flag_filter: flag_filter.map(str::to_string),
                        bookmarked_only,
                        ..Default::default()
                    },
                    offset: Some(0),
                    limit: Some(10),
                },
            )
            .unwrap();
            assert_eq!(result.total_filtered_rows, 3);
            assert_eq!(result.annotated_filtered, 3);
            assert_eq!(result.annotated_total, 3);
            assert_eq!(
                result
                    .rows
                    .iter()
                    .map(|row| row.row_index)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }
}

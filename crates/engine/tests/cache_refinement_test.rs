//! Cached AND refinements retain source identities and the public query contract.

use std::{collections::HashMap, fs, sync::Mutex, time::Duration};

use polars::prelude::{NamedFrom, Series};
use trivium_engine::{
    annotation::AnnotationPatch,
    condition::{Condition, Group, GroupKind, Node, Op},
    ops::{
        bulk::{bulk_preview, BulkAnnotatePayload},
        export::{export_project, ExportProjectPayload},
        flags::{update_flag, UpdateFlagPayload},
        projects::{create_project, CreateProjectPayload},
        rows::{
            clear_result_cache, locate_project_row, query_project_rows, result_cache_stats,
            LocateRowPayload, LocateRowResponse, QueryRowsPayload, QueryRowsResponse, QuerySpec,
        },
    },
    project_io::{
        last_column_read_projection, project_full_reads, project_page_reads, project_read_stats,
        read_project_dataframe, reset_project_read_counters, write_project_dataframe,
    },
    store::ProjectsStore,
};
use uuid::Uuid;

// Cache and I/O instrumentation are process-wide within this integration binary.
static LOCK: Mutex<()> = Mutex::new(());

struct Fixture {
    root: tempfile::TempDir,
    store: ProjectsStore,
    id: Uuid,
    columns: HashMap<String, String>,
}

impl Fixture {
    fn new(extension: &str, source: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join(format!("evidence.{extension}"));
        fs::write(&path, source).unwrap();
        let store = ProjectsStore::new(root.path().join("cases")).unwrap();
        let meta = create_project(
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
        .meta;
        let columns = meta.columns.into_iter().map(|c| (c.source, c.id)).collect();
        Self {
            root,
            store,
            id: meta.id,
            columns,
        }
    }

    fn csv() -> Self {
        Self::new(
            "csv",
            concat!(
                "label,command,detail,status,rank\n",
                "r0,powershell,encoded,keep,2\n",
                "r1,powershell,plain,keep,2\n",
                "r2,bash,encoded,keep,9\n",
                "r3,powershell,encoded,drop,1\n",
                "r4,curl,powershell,keep,3\n",
                "r5,powershell,encoded,keep,3\n",
                "r6,powershell,plain,drop,4\n",
                "r7,powershell,encoded,keep,3\n",
            ),
        )
    }

    fn col(&self, source: &str) -> String {
        self.columns
            .get(source)
            .or_else(|| self.columns.get(&format!("/{source}")))
            .unwrap()
            .clone()
    }

    fn base(&self) -> QuerySpec {
        QuerySpec {
            search: Some("powershell".into()),
            search_columns: Some(vec![self.col("command")]),
            ..Default::default()
        }
    }

    fn locate(&self, row_index: usize, spec: &QuerySpec) -> LocateRowResponse {
        locate_project_row(
            &self.store,
            LocateRowPayload {
                project_id: self.id,
                row_index,
                spec: spec.clone(),
            },
        )
        .unwrap()
    }

    fn query(&self, spec: &QuerySpec) -> QueryRowsResponse {
        query_project_rows(
            &self.store,
            QueryRowsPayload {
                project_id: self.id,
                spec: spec.clone(),
                offset: Some(0),
                limit: Some(100),
            },
        )
        .unwrap()
    }

    fn annotate(&self, row_index: usize, tag: &str, bookmarked: bool) {
        update_flag(
            &self.store,
            UpdateFlagPayload {
                project_id: self.id,
                row_index,
                flag: Some("critical".into()),
                memo: None,
                tags: Some(vec![tag.into()]),
                bookmarked: Some(bookmarked),
            },
        )
        .unwrap();
    }

    fn export_labels(&self, spec: &QuerySpec, name: &str) -> Vec<String> {
        let destination = self.root.path().join(format!("{name}.csv"));
        export_project(
            &self.store,
            ExportProjectPayload {
                project_id: self.id,
                destination: destination.to_string_lossy().into(),
                allow_overwrite: false,
                scope: Some("filtered".into()),
                spec: spec.clone(),
                columns: Some(vec![self.col("label")]),
                row_indices: None,
                include_annotations: Some(false),
            },
        )
        .unwrap();
        csv::Reader::from_path(destination)
            .unwrap()
            .records()
            .map(|row| row.unwrap().get(0).unwrap().to_owned())
            .collect()
    }
}

fn clause(column: String, op: Op, value: &str) -> Node {
    Node::Condition(Condition {
        column,
        op,
        values: vec![value.into()],
        negated: false,
    })
}

fn group(kind: GroupKind, children: Vec<Node>) -> Node {
    Node::Group(Group { kind, children })
}

fn ids(response: &QueryRowsResponse) -> Vec<usize> {
    response.rows.iter().map(|row| row.row_index).collect()
}

fn assert_projection(expected: &[String]) {
    let mut actual = last_column_read_projection();
    let mut expected = expected.to_vec();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected, "membership projection");
    assert_eq!(project_page_reads(), 0, "locate materialized a page");
    assert_eq!(project_full_reads(), 0, "membership read the full frame");
}

fn compare_fresh(fixture: &Fixture, spec: &QuerySpec, expected: &[usize]) {
    let cached = fixture.query(spec);
    assert_eq!(ids(&cached), expected);
    clear_result_cache();
    let fresh = fixture.query(spec);
    assert_eq!(
        serde_json::to_value(cached).unwrap(),
        serde_json::to_value(fresh).unwrap()
    );
}

fn assert_contracts(fixture: &Fixture, spec: &QuerySpec, expected: &[usize]) {
    let cached = fixture.query(spec);
    assert_eq!(ids(&cached), expected);
    assert_eq!(cached.total_filtered_rows, expected.len());
    for row in 0..cached.total_rows {
        let found = fixture.locate(row, spec);
        assert_eq!(found.position, expected.iter().position(|id| *id == row));
        assert_eq!(found.total_filtered_rows, expected.len());
    }
    let exported = fixture.export_labels(spec, "cached");
    assert_eq!(
        exported,
        expected
            .iter()
            .map(|row| format!("r{row}"))
            .collect::<Vec<_>>()
    );
    let preview = bulk_preview(
        &fixture.store,
        BulkAnnotatePayload {
            project_id: fixture.id,
            target: "filtered".into(),
            row_indices: None,
            spec: spec.clone(),
            patch: AnnotationPatch {
                flag: Some("safe".into()),
                ..Default::default()
            },
            preview_token: None,
        },
    )
    .unwrap();
    assert_eq!(preview.count, expected.len());
    assert!(!preview.preview_token.is_empty());
    clear_result_cache();
    let fresh = fixture.query(spec);
    assert_eq!(
        serde_json::to_value(cached).unwrap(),
        serde_json::to_value(fresh).unwrap()
    );
    assert_eq!(fixture.export_labels(spec, "fresh"), exported);
}

#[test]
fn appended_search_and_conditions_read_only_residual_columns_and_share_operation_targets() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::csv();
    let mut spec = f.base();
    assert_eq!(f.locate(7, &spec).total_filtered_rows, 6);
    let keep = clause(f.col("status"), Op::Eq, "keep");
    spec.condition = Some(keep.clone());
    reset_project_read_counters();
    assert_eq!(f.locate(7, &spec).position, Some(3));
    assert_projection(&[f.col("status")]);
    let residual_bytes = project_read_stats().column_bytes;
    assert!(residual_bytes > 0);

    spec.search = Some(format!("powershell {}:encoded", f.col("detail")));
    reset_project_read_counters();
    assert_eq!(f.locate(7, &spec).position, Some(2));
    assert_projection(&[f.col("detail")]);

    spec.condition = Some(group(
        GroupKind::And,
        vec![keep, clause(f.col("rank"), Op::Ge, "3")],
    ));
    reset_project_read_counters();
    assert_eq!(f.locate(7, &spec).position, Some(1));
    assert_projection(&[f.col("rank")]);
    reset_project_read_counters();
    assert_eq!(f.locate(5, &spec).position, Some(0));
    assert_eq!(
        project_read_stats().decoded_rows,
        0,
        "exact cache hit decoded values"
    );
    assert_contracts(&f, &spec, &[5, 7]);

    clear_result_cache();
    let mut first_refinement = f.base();
    first_refinement.condition = Some(clause(f.col("status"), Op::Eq, "keep"));
    reset_project_read_counters();
    f.locate(7, &first_refinement);
    assert!(
        project_read_stats().column_bytes > residual_bytes,
        "fresh membership did not read the prefix column"
    );
}

#[test]
fn all_matching_bases_skip_satisfied_columns_without_a_candidate_mask() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::csv();
    let mut spec = QuerySpec {
        search: Some("r".into()),
        search_columns: Some(vec![f.col("label")]),
        ..Default::default()
    };
    assert_eq!(f.locate(7, &spec).total_filtered_rows, 8);
    f.annotate(2, "review", false);
    spec.condition = Some(clause(f.col("status"), Op::Eq, "keep"));
    reset_project_read_counters();
    assert_eq!(f.locate(7, &spec).position, Some(5));
    assert_projection(&[f.col("status")]);
    let page = f.query(&spec);
    assert_eq!((page.annotated_total, page.annotated_filtered), (1, 1));
    assert_contracts(&f, &spec, &[0, 1, 2, 4, 5, 7]);
}

#[test]
fn unqualified_appended_word_and_opaque_or_children_keep_their_meaning() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::csv();
    let mut base = f.base();
    base.search_columns = Some(vec![f.col("command"), f.col("detail")]);
    assert_eq!(f.locate(4, &base).total_filtered_rows, 7);
    let mut refined = base.clone();
    refined.search = Some("powershell encoded".into());
    assert_eq!(f.locate(7, &refined).total_filtered_rows, 4);
    compare_fresh(&f, &refined, &[0, 3, 5, 7]);

    clear_result_cache();
    let opaque = group(
        GroupKind::Or,
        vec![
            clause(f.col("status"), Op::Eq, "keep"),
            clause(f.col("status"), Op::Eq, "drop"),
        ],
    );
    base = f.base();
    base.condition = Some(opaque.clone());
    assert_eq!(f.locate(7, &base).total_filtered_rows, 6);
    refined = base.clone();
    refined.condition = Some(group(
        GroupKind::And,
        vec![opaque, clause(f.col("rank"), Op::Ge, "3")],
    ));
    reset_project_read_counters();
    assert_eq!(f.locate(7, &refined).position, Some(2));
    assert_projection(&[f.col("rank")]);
    compare_fresh(&f, &refined, &[5, 6, 7]);
}

#[test]
fn widened_or_edited_or_removed_predicates_never_lose_rows_outside_the_old_base() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::csv();
    let base = f.base();
    let keep = clause(f.col("status"), Op::Eq, "keep");
    let high = clause(f.col("rank"), Op::Ge, "3");
    let constrained = QuerySpec {
        condition: Some(group(GroupKind::And, vec![keep.clone(), high.clone()])),
        ..base.clone()
    };
    let mut widened = base.clone();
    widened.search = Some("powershell|bash".into());
    let mut edited = base.clone();
    edited.search = Some("bash".into());
    let removed = QuerySpec {
        condition: Some(keep.clone()),
        ..base.clone()
    };
    let changed = QuerySpec {
        condition: Some(group(
            GroupKind::And,
            vec![clause(f.col("status"), Op::Eq, "drop"), high],
        )),
        ..base.clone()
    };
    let nested_base = QuerySpec {
        condition: Some(group(
            GroupKind::And,
            vec![
                keep.clone(),
                group(GroupKind::Or, vec![clause(f.col("rank"), Op::Eq, "2")]),
            ],
        )),
        ..base.clone()
    };
    let nested_widened = QuerySpec {
        condition: Some(group(
            GroupKind::And,
            vec![
                keep,
                group(
                    GroupKind::Or,
                    vec![
                        clause(f.col("rank"), Op::Eq, "2"),
                        clause(f.col("rank"), Op::Eq, "3"),
                    ],
                ),
            ],
        )),
        ..base.clone()
    };
    for (old, new, expected) in [
        (base.clone(), widened, vec![0, 1, 2, 3, 5, 6, 7]),
        (base, edited, vec![2]),
        (constrained.clone(), removed, vec![0, 1, 5, 7]),
        (constrained, changed, vec![6]),
        (nested_base, nested_widened, vec![0, 1, 5, 7]),
    ] {
        clear_result_cache();
        f.locate(7, &old);
        reset_project_read_counters();
        assert_eq!(
            f.locate(*expected.last().unwrap(), &new)
                .total_filtered_rows,
            expected.len()
        );
        assert!(
            last_column_read_projection().contains(&f.col("command")),
            "changed predicate reused an unsafe prefix: {new:?}"
        );
        compare_fresh(&f, &new, &expected);
    }
}

#[test]
fn scope_flag_bookmark_and_sort_changes_require_a_new_population() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::csv();
    f.annotate(0, "review", true);
    let base = f.base();
    let keep = Some(clause(f.col("status"), Op::Eq, "keep"));
    let scope = QuerySpec {
        condition: keep.clone(),
        search_columns: Some(vec![f.col("command"), f.col("detail")]),
        ..base.clone()
    };
    let scope_old = QuerySpec {
        search_columns: Some(vec![f.col("command"), f.col("detail")]),
        ..base.clone()
    };
    let scope_reordered = QuerySpec {
        condition: keep.clone(),
        search_columns: Some(vec![f.col("detail"), f.col("command")]),
        ..base.clone()
    };
    let flagged = QuerySpec {
        flag_filter: Some("critical".into()),
        ..base.clone()
    };
    let bookmarked = QuerySpec {
        bookmarked_only: Some(true),
        ..base.clone()
    };
    let plain = QuerySpec {
        condition: keep.clone(),
        ..base.clone()
    };
    let descending = QuerySpec {
        sort_key: Some(f.col("rank")),
        sort_direction: Some("desc".into()),
        ..base.clone()
    };
    let ascending = QuerySpec {
        condition: keep.clone(),
        sort_direction: Some("asc".into()),
        ..descending.clone()
    };
    let other_sort = QuerySpec {
        condition: keep,
        sort_key: Some(f.col("label")),
        ..descending.clone()
    };
    for (old, new, expected) in [
        (base, scope, vec![0, 1, 4, 5, 7]),
        (scope_old, scope_reordered, vec![0, 1, 4, 5, 7]),
        (flagged, plain.clone(), vec![0, 1, 5, 7]),
        (bookmarked, plain, vec![0, 1, 5, 7]),
        (descending.clone(), ascending, vec![0, 1, 5, 7]),
        (descending, other_sort, vec![7, 5, 1, 0]),
    ] {
        clear_result_cache();
        f.locate(0, &old);
        reset_project_read_counters();
        f.locate(7, &new);
        assert!(
            last_column_read_projection().contains(&f.col("command")),
            "changed query options reused a prefix: {new:?}"
        );
        compare_fresh(&f, &new, &expected);
    }
}

#[test]
fn descending_refinement_reads_sort_keys_and_keeps_source_order_for_ties() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::csv();
    let mut spec = QuerySpec {
        sort_key: Some(f.col("rank")),
        sort_direction: Some("desc".into()),
        ..f.base()
    };
    assert_eq!(f.locate(6, &spec).position, Some(0));
    spec.condition = Some(clause(f.col("status"), Op::Eq, "keep"));
    reset_project_read_counters();
    assert_eq!(f.locate(0, &spec).position, Some(2));
    assert_projection(&[f.col("status"), f.col("rank")]);
    assert_contracts(&f, &spec, &[5, 7, 0, 1]);
}

#[test]
fn json_numeric_refinements_preserve_exact_decimals_null_missing_and_cell_text() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::new(
        "jsonl",
        concat!(
        "{\"command\":\"powershell\",\"n\":9007199254740993,\"note\":null}\n",
        "{\"command\":\"powershell\",\"n\":9007199254740993}\n",
        "{\"command\":\"powershell\",\"n\":9007199254740993.000000000000000001,\"note\":\"\"}\n",
        "{\"command\":\"powershell\",\"n\":18446744073709551615,\"note\":\"  0007  \"}\n",
        "{\"command\":\"bash\",\"n\":18446744073709551615,\"note\":\"outside\"}\n",
        "{\"command\":\"powershell\",\"n\":9007199254740992}\n",
    ),
    );
    let mut spec = f.base();
    f.locate(3, &spec);
    spec.condition = Some(clause(f.col("n"), Op::Gt, "9007199254740992"));
    reset_project_read_counters();
    assert_eq!(f.locate(3, &spec).total_filtered_rows, 4);
    assert_projection(&[f.col("n")]);
    let page = f.query(&spec);
    assert_eq!(ids(&page), [0, 1, 2, 3]);
    let note = f.col("note");
    let n = f.col("n");
    assert_eq!(page.rows[0].cells[&note].kind, "null");
    assert_eq!(page.rows[1].cells[&note].kind, "missing");
    assert_eq!(page.rows[2].cells[&note].text.as_deref(), Some(""));
    assert_eq!(
        page.rows[2].cells[&n].text.as_deref(),
        Some("9007199254740993.000000000000000001")
    );
    assert_eq!(
        page.rows[3].cells[&n].text.as_deref(),
        Some("18446744073709551615")
    );
    assert_eq!(page.rows[3].cells[&note].text.as_deref(), Some("  0007  "));
    compare_fresh(&f, &spec, &[0, 1, 2, 3]);
    clear_result_cache();
    f.locate(3, &spec);
    spec.condition = Some(group(
        GroupKind::And,
        vec![
            clause(f.col("n"), Op::Gt, "9007199254740992"),
            clause(f.col("n"), Op::Gt, "9007199254740993"),
        ],
    ));
    assert_eq!(f.locate(2, &spec).position, Some(0));
    compare_fresh(&f, &spec, &[2, 3]);
}

#[test]
fn fresh_annotations_can_refine_data_only_bases_but_not_stale_annotation_populations() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::csv();
    let base = f.base();
    f.locate(7, &base);
    f.annotate(0, "triage", true);
    let mut tagged = base.clone();
    tagged.search = Some("powershell @tag:triage".into());
    reset_project_read_counters();
    assert_eq!(f.locate(0, &tagged).position, Some(0));
    assert_projection(&[]);
    assert_eq!(
        project_read_stats().decoded_rows,
        0,
        "tag-only residual read source values"
    );
    let page = f.query(&tagged);
    assert_eq!((page.annotated_total, page.annotated_filtered), (1, 1));
    assert_eq!(page.rows[0].tags, ["triage"]);
    assert!(page.rows[0].bookmarked);
    f.annotate(5, "triage", false);
    reset_project_read_counters();
    assert_eq!(f.locate(5, &tagged).position, Some(1));
    assert_eq!(project_read_stats().decoded_rows, 0);
    compare_fresh(&f, &tagged, &[0, 5]);

    clear_result_cache();
    f.locate(5, &tagged);
    f.annotate(7, "triage", false);
    tagged.condition = Some(clause(f.col("status"), Op::Eq, "keep"));
    reset_project_read_counters();
    assert_eq!(f.locate(7, &tagged).position, Some(2));
    assert_projection(&[f.col("command"), f.col("status")]);
    let page = f.query(&tagged);
    assert_eq!((page.annotated_total, page.annotated_filtered), (3, 3));
    assert!(page
        .rows
        .iter()
        .all(|row| row.tags == ["triage"] && row.flag == "critical"));
    compare_fresh(&f, &tagged, &[0, 5, 7]);
}

#[test]
fn changed_data_rejects_both_exact_and_refinement_results_without_reassigning_row_ids() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::csv();
    let base = f.base();
    f.locate(7, &base);
    let path = f.store.project_dir(&f.id).join("data.parquet");
    let modified = fs::metadata(&path).unwrap().modified().unwrap();
    let mut frame = read_project_dataframe(&path).unwrap();
    frame
        .replace(
            &f.col("command"),
            Series::new(f.col("command").into(), ["powershell"; 8]).into(),
        )
        .unwrap();
    write_project_dataframe(&path, &mut frame).unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(modified + Duration::from_secs(1))
        .unwrap();
    let refined = QuerySpec {
        condition: Some(clause(f.col("status"), Op::Eq, "keep")),
        ..base.clone()
    };
    reset_project_read_counters();
    assert_eq!(f.locate(2, &refined).position, Some(2));
    assert_projection(&[f.col("command"), f.col("status")]);
    // The exact old base is still cached under the previous data signature.
    reset_project_read_counters();
    assert_eq!(f.locate(4, &base).position, Some(4));
    assert_projection(&[f.col("command")]);
    compare_fresh(&f, &refined, &[0, 1, 2, 4, 5, 7]);
    compare_fresh(&f, &base, &[0, 1, 2, 3, 4, 5, 6, 7]);
}

#[test]
fn empty_cached_bases_still_validate_complete_new_predicates() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::csv();
    let mut base = f.base();
    base.search = Some("does-not-exist".into());
    assert_eq!(f.locate(0, &base).total_filtered_rows, 0);
    let mut deep = clause(f.col("status"), Op::Eq, "keep");
    for _ in 0..64 {
        deep = group(GroupKind::And, vec![deep]);
    }
    let many_patterns = Node::Condition(Condition {
        column: f.col("detail"),
        op: Op::Regex,
        values: vec!["valid".into(); 33],
        negated: false,
    });
    for invalid in [
        clause(f.col("detail"), Op::Regex, "["),
        clause(f.col("rank"), Op::Gt, "not a number"),
        clause("unknown-column".into(), Op::Eq, "keep"),
        many_patterns,
        deep,
    ] {
        let spec = QuerySpec {
            condition: Some(invalid),
            ..base.clone()
        };
        let error = locate_project_row(
            &f.store,
            LocateRowPayload {
                project_id: f.id,
                row_index: 0,
                spec,
            },
        )
        .expect_err("empty cached base bypassed validation");
        assert!(!error.to_string().is_empty());
    }
    let spec = QuerySpec {
        search: Some(vec!["does-not-exist"; 65].join(" ")),
        ..base
    };
    assert!(query_project_rows(
        &f.store,
        QueryRowsPayload {
            project_id: f.id,
            spec,
            offset: None,
            limit: None
        }
    )
    .is_err());
}

#[test]
fn bounded_cache_evicts_old_queries_and_retains_recent_exact_results() {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let f = Fixture::csv();
    let specs: Vec<_> = (0..10)
        .map(|row| QuerySpec {
            condition: Some(clause(f.col("label"), Op::Ne, &format!("r{row}"))),
            ..f.base()
        })
        .collect();
    for spec in &specs {
        f.locate(7, spec);
    }
    let stats = result_cache_stats();
    assert_eq!(stats.max_entries, 8);
    assert_eq!(stats.max_bytes, 64 * 1024 * 1024);
    assert!(stats.entries <= stats.max_entries && stats.total_bytes <= stats.max_bytes);
    assert!(stats.evictions >= 2);
    reset_project_read_counters();
    f.locate(7, &specs[9]);
    assert_eq!(
        project_read_stats().decoded_rows,
        0,
        "recent exact result was discarded"
    );
    reset_project_read_counters();
    f.locate(7, &specs[0]);
    assert!(
        project_read_stats().decoded_rows > 0,
        "evicted query retained an unbounded result"
    );
    compare_fresh(&f, &specs[0], &[1, 3, 5, 6, 7]);
}

#[test]
fn sparse_refinement_preserves_original_ids_across_decoded_batch_boundaries() {
    use std::fmt::Write;
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_result_cache();
    let mut source = String::from("label,command,status\n");
    for row in 0..16_410 {
        writeln!(
            source,
            "r{row},{},{}",
            if [1, 8195, 16409].contains(&row) {
                "powershell"
            } else {
                "bash"
            },
            if row == 8195 { "drop" } else { "keep" }
        )
        .unwrap();
    }
    let f = Fixture::new("csv", &source);
    let mut spec = f.base();
    assert_eq!(f.locate(16409, &spec).position, Some(2));
    spec.condition = Some(clause(f.col("status"), Op::Eq, "keep"));
    reset_project_read_counters();
    let found = f.locate(16409, &spec);
    assert_eq!((found.position, found.total_filtered_rows), (Some(1), 2));
    assert_projection(&[f.col("status")]);
    assert!(project_read_stats().max_batch_rows <= 8192);
    assert_eq!(f.locate(8195, &spec).position, None);
    compare_fresh(&f, &spec, &[1, 16409]);
}

//! Compare the borrowed Polars path with the previous owned-row adapter and
//! measure real heap allocations independently of timing and mocked counters.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    collections::HashMap,
};

use polars::prelude::*;
use serde_json::Value;
use trivium_engine::{
    condition::{
        and_nodes, search_tokens_to_node, Condition, Group, GroupKind, Node, Op, PreparedNode,
    },
    jobs::JobContext,
    ops::rows::{resolve_target, QuerySpec},
    search::{push_searchable_value, tokenize_search_query},
    value_utils::anyvalue_to_search_string,
};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}
struct CountAlloc;
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        TRACK.with(|track| {
            if track.get() {
                ALLOCS.with(|count| count.set(count.get() + 1));
            }
        });
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        TRACK.with(|track| {
            if track.get() {
                ALLOCS.with(|count| count.set(count.get() + 1));
            }
        });
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountAlloc = CountAlloc;

fn condition(column: &str, op: Op, values: &[&str], negated: bool) -> Node {
    Node::Condition(Condition {
        column: column.into(),
        op,
        values: values.iter().map(|value| (*value).into()).collect(),
        negated,
    })
}

// This deliberately constructs the former per-row HashMap + JSON strings. It
// exercises the compatibility evaluator, not the new bound-slot adapter.
fn owned_rows(df: &DataFrame, spec: &QuerySpec) -> Result<Vec<usize>, String> {
    let tokens = tokenize_search_query(spec.search.as_deref().unwrap_or_default());
    let search = search_tokens_to_node(&tokens);
    let node = match (search, spec.condition.clone()) {
        (Some(search), Some(condition)) => Some(and_nodes(search, condition)),
        (Some(node), None) | (None, Some(node)) => Some(node),
        (None, None) => None,
    };
    let Some(node) = node else {
        return Ok((0..df.height()).collect());
    };
    let prepared = PreparedNode::prepare(&node).map_err(|error| error.to_string())?;
    let needs_joined = prepared.columns().contains(&"");
    let scope = spec
        .search_columns
        .as_ref()
        .filter(|scope| !scope.is_empty());
    let mut output = Vec::new();
    for index in 0..df.height() {
        let mut row = HashMap::new();
        for column in df.get_columns() {
            if let Some(text) = anyvalue_to_search_string(&column.get(index).unwrap()) {
                row.insert(column.name().to_string(), Value::String(text));
            }
        }
        if needs_joined {
            let mut joined = String::new();
            for column in df.get_columns() {
                if scope.is_none_or(|scope| scope.iter().any(|name| name == column.name())) {
                    push_searchable_value(&mut joined, &column.get(index).unwrap());
                }
            }
            row.insert(String::new(), Value::String(joined));
        }
        if prepared.eval(&row).map_err(|error| error.to_string())? {
            output.push(index);
        }
    }
    Ok(output)
}

fn borrowed_rows(df: &DataFrame, spec: &QuerySpec) -> Result<Vec<usize>, String> {
    let columns = df
        .get_column_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    resolve_target(df, spec, &columns, &HashMap::new(), &JobContext::none())
        .map(|target| target.filtered)
        .map_err(|error| error.to_string())
}

#[test]
fn strings_null_unicode_negation_groups_and_legacy_types_match_owned_adapter() {
    let df = DataFrame::new(vec![
        Series::new(
            "text",
            [
                Some(""),
                None,
                Some("ΟΣ"),
                Some("İSTANBUL"),
                Some("Alpha beta"),
                Some("0007"),
            ],
        ),
        Series::new("integer", [0i64, -1, 7, i64::MAX, 9007199254740993, 6]),
        Series::new("bool", [true, false, true, false, true, false]),
    ])
    .unwrap();
    for op in [
        Op::Eq,
        Op::Ne,
        Op::In,
        Op::Contains,
        Op::StartsWith,
        Op::EndsWith,
        Op::Regex,
    ] {
        for negated in [false, true] {
            for values in [
                &[""][..],
                &["ος", "i̇stan"][..],
                &["0007", "Alpha beta"][..],
                &["^$", "β"][..],
                &[][..],
            ] {
                let spec = QuerySpec {
                    condition: Some(condition("TEXT", op, values, negated)),
                    ..Default::default()
                };
                assert_eq!(
                    borrowed_rows(&df, &spec),
                    owned_rows(&df, &spec),
                    "{spec:?}"
                );
            }
        }
    }
    for search in [
        "ΟΣ",
        "text:alpha|0007",
        "-text:alpha",
        "\"Alpha beta\"",
        "text:alpha |0007",
        "bool:true|text:0007",
        "integer:9007199254740993",
    ] {
        let spec = QuerySpec {
            search: Some(search.into()),
            ..Default::default()
        };
        assert_eq!(
            borrowed_rows(&df, &spec),
            owned_rows(&df, &spec),
            "{spec:?}"
        );
    }
    let spec = QuerySpec {
        condition: Some(Node::Group(Group {
            kind: GroupKind::Or,
            children: vec![
                condition("integer", Op::Gt, &["9007199254740992"], false),
                Node::Group(Group {
                    kind: GroupKind::And,
                    children: vec![
                        condition("text", Op::Eq, &["0007"], false),
                        condition("bool", Op::Eq, &["true"], true),
                    ],
                }),
            ],
        })),
        ..Default::default()
    };
    assert_eq!(borrowed_rows(&df, &spec), owned_rows(&df, &spec));
    assert_eq!(borrowed_rows(&df, &spec).unwrap(), vec![3, 4, 5]);
}

#[test]
fn raw_large_integers_datetime_offsets_ns_and_errors_match_owned_adapter() {
    for (values, operands) in [
        (
            vec![
                Some("0007"),
                Some("9007199254740993"),
                Some("1700000000123456789"),
                None,
            ],
            vec!["7", "9007199254740992"],
        ),
        (
            vec![
                Some("2026-11-08T01:02:03.000000001+09:00"),
                Some("2026-11-07T16:02:03.000000002Z"),
                None,
                None,
            ],
            vec!["2026-11-07T16:02:03.000000001Z"],
        ),
        (
            vec![
                Some("170141183460469231731687303715884105728"),
                Some("1"),
                None,
                None,
            ],
            vec!["1"],
        ),
        (
            vec![Some("2"), None, None, None],
            vec!["1", "not-an-integer"],
        ),
    ] {
        let df = DataFrame::new(vec![Series::new("value", values)]).unwrap();
        for op in [Op::Eq, Op::Ne, Op::Gt, Op::Ge, Op::Lt, Op::Le] {
            for negated in [false, true] {
                let spec = QuerySpec {
                    condition: Some(condition("value", op, &operands, negated)),
                    ..Default::default()
                };
                assert_eq!(
                    borrowed_rows(&df, &spec),
                    owned_rows(&df, &spec),
                    "{spec:?}"
                );
            }
        }
    }
    let df = DataFrame::new(vec![Series::new("value", ["a"])]).unwrap();
    let bad = QuerySpec {
        condition: Some(condition("value", Op::Regex, &["["], false)),
        ..Default::default()
    };
    assert_eq!(borrowed_rows(&df, &bad), owned_rows(&df, &bad));
}

#[test]
fn exact_column_names_take_precedence_over_case_insensitive_aliases() {
    let df = DataFrame::new(vec![
        Series::new("Name", ["a", "b"]),
        Series::new("name", ["x", "y"]),
    ])
    .unwrap();
    let spec = QuerySpec {
        condition: Some(Node::Group(Group {
            kind: GroupKind::And,
            children: vec![
                condition("Name", Op::Eq, &["a"], false),
                condition("name", Op::Eq, &["x"], false),
            ],
        })),
        ..Default::default()
    };
    assert_eq!(borrowed_rows(&df, &spec).unwrap(), vec![0]);
}

fn allocations<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ALLOCS.with(|count| count.set(0));
    TRACK.with(|track| track.set(true));
    let output = operation();
    TRACK.with(|track| track.set(false));
    (output, ALLOCS.with(Cell::get))
}

#[test]
fn string_query_allocations_are_bounded_as_row_count_increases() {
    let small = DataFrame::new(vec![Series::new("value", vec!["keep needle"; 1000])]).unwrap();
    let large = DataFrame::new(vec![Series::new("value", vec!["keep needle"; 20000])]).unwrap();
    for spec in [
        QuerySpec::default(),
        QuerySpec {
            condition: Some(condition("value", Op::Contains, &["NEEDLE"], false)),
            ..Default::default()
        },
        QuerySpec {
            condition: Some(condition("value", Op::Eq, &["keep needle"], false)),
            ..Default::default()
        },
        QuerySpec {
            search: Some("needle".into()),
            ..Default::default()
        },
    ] {
        // Warm lazy instrumentation before counting; all frames are prebuilt.
        borrowed_rows(&small, &spec).unwrap();
        let (small_result, small_allocations) =
            allocations(|| borrowed_rows(&small, &spec).unwrap());
        let (large_result, large_allocations) =
            allocations(|| borrowed_rows(&large, &spec).unwrap());
        assert_eq!(small_result.len(), 1000);
        assert_eq!(large_result.len(), 20000);
        assert!(
            large_allocations <= small_allocations + 8,
            "per-row allocations returned: {small_allocations} -> {large_allocations}, {spec:?}"
        );
        let (_, former_allocations) = allocations(|| owned_rows(&large, &spec).unwrap());
        if spec.condition.is_some() || spec.search.is_some() {
            assert!(
                former_allocations > large_allocations * 100,
                "owned={former_allocations}, borrowed={large_allocations}"
            );
        }
        eprintln!("1000/20000-row allocations: {small_allocations}/{large_allocations}; owned adapter={former_allocations}");
    }
}

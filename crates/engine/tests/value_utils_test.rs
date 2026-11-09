use polars::prelude::{AnyValue, NamedFrom, Series, TimeUnit};

use trivium_engine::value_utils::anyvalue_to_search_string;

#[test]
fn anyvalue_to_search_string_converts_scalar_variants() {
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::Boolean(true)),
        Some("true".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::Boolean(false)),
        Some("false".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::Int8(-8)),
        Some("-8".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::Int64(123_456_789_0123)),
        Some("1234567890123".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::UInt64(42)),
        Some("42".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::Float32(1.5)),
        Some("1.5".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::Float64(2.25)),
        Some("2.25".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::String("hello")),
        Some("hello".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::StringOwned("owned".into())),
        Some("owned".to_string())
    );
    assert_eq!(anyvalue_to_search_string(&AnyValue::Null), None);
}

#[test]
fn anyvalue_to_search_string_converts_temporal_variants() {
    let no_timezone: Option<String> = None;

    let datetime = AnyValue::Datetime(0, TimeUnit::Milliseconds, &no_timezone);
    assert_eq!(
        anyvalue_to_search_string(&datetime),
        Some("1970-01-01 00:00:00".to_string())
    );

    assert_eq!(
        anyvalue_to_search_string(&AnyValue::Date(0)),
        Some("1970-01-01".to_string())
    );
    assert_eq!(
        anyvalue_to_search_string(&AnyValue::Time(0)),
        Some("00:00:00".to_string())
    );
}

#[test]
fn anyvalue_to_search_string_joins_list_values_with_commas() {
    // AnyValue::List owns its Series, so build the Series with Series::new and
    // move it into the variant rather than trying to obtain it from a frame.
    let list = AnyValue::List(Series::new("values", &[1i32, 2, 3]));
    assert_eq!(
        anyvalue_to_search_string(&list),
        Some("1,2,3".to_string())
    );

    // Null elements are skipped by the recursive conversion.
    let list_with_null = AnyValue::List(Series::new(
        "values",
        &[Some(1i32), None, Some(3i32)],
    ));
    assert_eq!(
        anyvalue_to_search_string(&list_with_null),
        Some("1,3".to_string())
    );

    // An empty list has no parts to join, so it converts to None.
    let empty = AnyValue::List(Series::new("values", &[] as &[i32]));
    assert_eq!(anyvalue_to_search_string(&empty), None);
}

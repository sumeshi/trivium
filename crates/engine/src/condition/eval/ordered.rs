use std::cmp::Ordering;

use crate::{analysis::datetime::parse_datetime_nanos, number::ExactNumber};

use super::EvalError;

/// A validated user bound decides whether a source value is a number or an
/// instant. Keeping it prepared avoids reparsing the same bound for every row.
#[derive(Debug)]
pub(super) enum OrderedBound {
    Integer(i128),
    Number(ExactNumber),
    Datetime(i64),
}

impl OrderedBound {
    pub(super) fn parse(raw: &str) -> Result<Self, EvalError> {
        let text = raw.trim();
        if let Ok(integer) = text.parse::<i128>() {
            return Ok(Self::Integer(integer));
        }
        if let Some(number) = ExactNumber::parse(text) {
            return Ok(Self::Number(number));
        }
        parse_datetime_nanos(text)
            .map(Self::Datetime)
            .ok_or_else(|| EvalError::ordered(text))
    }

    fn compare(&self, cell: &str) -> Option<Ordering> {
        match self {
            Self::Integer(bound) => match cell.parse::<i128>() {
                // Preserve the allocation-free common integer path.
                Ok(cell) => Some(cell.cmp(bound)),
                Err(_) => ExactNumber::parse(cell).map(|cell| cell.cmp_integer(*bound)),
            },
            Self::Number(bound) => match cell.parse::<i128>() {
                Ok(cell) => Some(bound.cmp_integer(cell).reverse()),
                Err(_) => ExactNumber::parse(cell).map(|cell| cell.cmp(bound)),
            },
            Self::Datetime(bound) => parse_datetime_nanos(cell).map(|cell| cell.cmp(bound)),
        }
    }
}

pub(super) fn ordered_matches(
    cell: Option<&str>,
    bounds: &[OrderedBound],
    want: Ordering,
    or_equal: bool,
) -> bool {
    let Some(cell) = cell.map(str::trim).filter(|cell| !cell.is_empty()) else {
        return false;
    };
    bounds.iter().any(|bound| {
        bound
            .compare(cell)
            .is_some_and(|ordering| ordering == want || (or_equal && ordering == Ordering::Equal))
    })
}

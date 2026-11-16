//! Preserve existing statistics text without allocating unbounded cell copies.
use polars::prelude::AnyValue;
use std::{
    borrow::Cow,
    fmt::{self, Write},
};

pub(crate) fn present_text<'a>(
    value: &'a AnyValue<'_>,
    limit: usize,
) -> Result<Option<Cow<'a, str>>, ()> {
    match value {
        AnyValue::Null => Ok(None),
        // Admission happens before cloning a new key. Repeated long strings
        // can therefore increment their existing count without an allocation.
        AnyValue::String(text) => Ok((!text.is_empty()).then_some(Cow::Borrowed(text))),
        AnyValue::StringOwned(text) => {
            Ok((!text.is_empty()).then_some(Cow::Borrowed(text.as_str())))
        }
        _ => {
            let mut output = Text {
                value: String::new(),
                limit,
            };
            append(value, &mut output).map_err(|_| ())?;
            Ok((!output.value.is_empty()).then_some(Cow::Owned(output.value)))
        }
    }
}

struct Text {
    value: String,
    limit: usize,
}
impl Write for Text {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if text.len() > self.limit.saturating_sub(self.value.len()) {
            return Err(fmt::Error);
        }
        self.value
            .try_reserve_exact(text.len())
            .map_err(|_| fmt::Error)?;
        self.value.push_str(text);
        Ok(())
    }
}

/// Lists use the legacy comma-join representation; null elements disappear,
/// while present empty strings retain their separators. Format into one
/// bounded buffer instead of allocating every element and a joined copy.
fn append(value: &AnyValue, output: &mut Text) -> Result<bool, fmt::Error> {
    match value {
        AnyValue::Null => return Ok(false),
        AnyValue::String(text) => output.write_str(text)?,
        AnyValue::StringOwned(text) => output.write_str(text.as_str())?,
        AnyValue::Float32(value) => write!(output, "{}", f64::from(*value))?,
        AnyValue::Float64(value) => write!(output, "{value}")?,
        AnyValue::Boolean(value) => write!(output, "{value}")?,
        AnyValue::Int8(value) => write!(output, "{value}")?,
        AnyValue::Int16(value) => write!(output, "{value}")?,
        AnyValue::Int32(value) => write!(output, "{value}")?,
        AnyValue::Int64(value) => write!(output, "{value}")?,
        AnyValue::UInt8(value) => write!(output, "{value}")?,
        AnyValue::UInt16(value) => write!(output, "{value}")?,
        AnyValue::UInt32(value) => write!(output, "{value}")?,
        AnyValue::UInt64(value) => write!(output, "{value}")?,
        AnyValue::List(series) => {
            let mut present = false;
            for inner in series.iter() {
                if !is_present(&inner) {
                    continue;
                }
                if present {
                    output.write_char(',')?;
                }
                append(&inner, output)?;
                present = true;
            }
            return Ok(present);
        }
        other => write!(output, "{other}")?,
    }
    Ok(true)
}

fn is_present(value: &AnyValue) -> bool {
    match value {
        AnyValue::Null => false,
        AnyValue::List(series) => series.iter().any(|value| is_present(&value)),
        _ => true,
    }
}

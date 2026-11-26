use std::fmt;

use serde::de::{Deserializer as _, SeqAccess, Visitor};
use serde_json::{value::RawValue, Value};

use super::{ImportPreview, MAX_COLUMNS, MAX_ROWS};
use crate::{
    error::AppError,
    import::{parse_json_value, value_to_cell, visit_leaves},
    jobs::JobContext,
};

const MAX_SAMPLE_RECORDS: usize = 200;

fn record(raw: &[u8]) -> Result<Value, String> {
    let value = parse_json_value(raw).map_err(|error| error.to_string())?;
    if value.is_object() {
        Ok(value)
    } else {
        Err("Record is not a JSON object.".into())
    }
}

fn lines(
    text: &str,
    complete: bool,
    recover: bool,
    preview: &mut ImportPreview,
    job: &JobContext,
) -> Result<Vec<Value>, AppError> {
    let mut values = Vec::new();
    for (index, line) in text.split_inclusive('\n').enumerate() {
        job.check_cancelled()?;
        if index == MAX_SAMPLE_RECORDS {
            preview.truncated = true;
            break;
        }
        if !complete && !line.ends_with('\n') {
            preview.truncated = true;
            preview.warning("An incomplete JSONL line at the end of the sample is not shown.");
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match record(line.as_bytes()) {
            Ok(value) => {
                if values.len() == MAX_ROWS {
                    preview.truncated = true;
                    break;
                }
                values.push(value);
            }
            Err(error) => {
                preview.warning(format!("JSONL line {}: {error}", index + 1));
                if !recover {
                    break;
                }
            }
        }
    }
    Ok(values)
}

struct Array<'a> {
    values: &'a mut Vec<Value>,
    preview: &'a mut ImportPreview,
    job: &'a JobContext,
    recover: bool,
    stopped: &'a mut bool,
    fatal: &'a mut Option<AppError>,
}

impl<'de> Visitor<'de> for Array<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an array of JSON objects")
    }
    fn visit_seq<A>(self, mut sequence: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut index = 0;
        loop {
            if let Err(error) = self.job.check_cancelled() {
                *self.fatal = Some(error);
                return Err(serde::de::Error::custom("preview cancelled"));
            }
            let Some(raw) = sequence.next_element::<Box<RawValue>>()? else {
                return Ok(());
            };
            index += 1;
            if self.values.len() == MAX_ROWS || index > MAX_SAMPLE_RECORDS {
                self.preview.truncated = true;
                *self.stopped = true;
                return Err(serde::de::Error::custom("preview sample complete"));
            }
            match record(raw.get().as_bytes()) {
                Ok(value) => self.values.push(value),
                Err(error) => {
                    self.preview
                        .warning(format!("JSON array element {index}: {error}"));
                    if !self.recover {
                        *self.stopped = true;
                        return Err(serde::de::Error::custom("invalid sampled record"));
                    }
                }
            }
        }
    }
}

fn array(
    text: &str,
    recover: bool,
    preview: &mut ImportPreview,
    job: &JobContext,
) -> Result<Vec<Value>, AppError> {
    let mut values = Vec::new();
    let mut stopped = false;
    let mut fatal = None;
    let mut parser = serde_json::Deserializer::from_reader(text.as_bytes());
    let parsed = parser.deserialize_seq(Array {
        values: &mut values,
        preview,
        job,
        recover,
        stopped: &mut stopped,
        fatal: &mut fatal,
    });
    if let Some(error) = fatal {
        return Err(error);
    }
    if !stopped {
        if let Err(error) = parsed.and_then(|()| parser.end()) {
            preview.warning(format!("The JSON sample is incomplete or invalid: {error}"));
            preview.truncated = true;
        }
    }
    Ok(values)
}

pub(super) fn preview(
    text: &str,
    complete: bool,
    jsonl: bool,
    recover: bool,
    preview: &mut ImportPreview,
    job: &JobContext,
) -> Result<(), AppError> {
    let values = if jsonl {
        lines(text, complete, recover, preview, job)?
    } else if text.trim_start().starts_with('[') {
        array(text, recover, preview, job)?
    } else {
        match record(text.as_bytes()) {
            Ok(value) => vec![value],
            Err(error) => {
                preview.warning(format!("JSON sample: {error}"));
                Vec::new()
            }
        }
    };
    let mut pointers = Vec::new();
    let mut wide = false;
    for value in &values {
        job.check_cancelled()?;
        visit_leaves(value, "", &mut |pointer, _| {
            if pointers.contains(&pointer) {
                return;
            }
            if pointers.len() == MAX_COLUMNS {
                wide = true;
            } else {
                pointers.push(pointer);
            }
        });
    }
    if wide {
        preview.truncated = true;
        preview.warning("Only the first 20 sampled columns are shown.");
    }
    preview.columns = pointers
        .iter()
        .map(|pointer| preview.clip(pointer.strip_prefix('/').unwrap_or(pointer), 80))
        .collect();
    for value in values {
        if !preview.add_row(
            pointers
                .iter()
                .map(|pointer| value.pointer(pointer).and_then(value_to_cell)),
        ) {
            break;
        }
    }
    Ok(())
}

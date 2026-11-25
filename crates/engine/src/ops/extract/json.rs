//! Scan selected JSON source values, never overlapping table serialization copies.
use std::{borrow::Cow, collections::BTreeMap, fmt, path::Path};

use polars::prelude::AnyValue;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

use crate::{
    error::AppError,
    formats::CompiledFormat,
    jobs::JobContext,
    models::ProjectMeta,
    project_io::{read_project_schema_columns, ProjectBatchReader, INTERNAL_ORIGINAL},
};

use super::aggregate::{limit_error, Accumulator};

#[derive(Default)]
struct Selection {
    selected: bool,
    children: BTreeMap<String, Selection>,
}

impl Selection {
    fn insert(&mut self, tokens: &[String]) {
        if self.selected {
            return;
        }
        if let Some((first, rest)) = tokens.split_first() {
            self.children.entry(first.clone()).or_default().insert(rest);
        } else {
            self.selected = true;
            self.children.clear();
        }
    }

    fn child(&self, key: &str) -> Option<&Self> {
        if self.selected {
            Some(self)
        } else {
            self.children.get(key)
        }
    }
}

pub(super) struct JsonSelection(Selection);

impl JsonSelection {
    pub fn from_project(
        meta: &ProjectMeta,
        columns: &[String],
        path: &Path,
        job: &JobContext,
    ) -> Result<Option<Self>, AppError> {
        let known_json = matches!(
            meta.source_format.as_deref(),
            Some("json" | "jsonl" | "ndjson")
        );
        let infer = meta.source_format.is_none() && meta.storage_version >= 2;
        if !known_json && !infer {
            return Ok(None);
        }
        if !read_project_schema_columns(path)
            .map_err(AppError::from)?
            .iter()
            .any(|column| column == INTERNAL_ORIGINAL)
        {
            return Err(missing_original());
        }
        if infer && !original_is_json(path, job)? {
            return Ok(None);
        }
        let mut root = Selection::default();
        for id in columns {
            let source = meta
                .columns
                .iter()
                .find(|column| column.id == *id)
                .map(|column| column.source.as_str())
                .and_then(|pointer| pointer.strip_prefix('/'))
                .ok_or_else(|| {
                    AppError::Message(
                        "JSON extraction requires source column paths. Reimport this project."
                            .into(),
                    )
                })?;
            let tokens = source
                .split('/')
                .map(|token| token.replace("~1", "/").replace("~0", "~"))
                .collect::<Vec<_>>();
            root.insert(&tokens);
        }
        Ok(Some(Self(root)))
    }

    pub fn scan(
        &self,
        text: &str,
        row: usize,
        matcher: &CompiledFormat,
        accumulator: &mut Accumulator,
        job: &JobContext,
    ) -> Result<(), AppError> {
        job.check_cancelled()?;
        let mut context = Context {
            row,
            matcher,
            accumulator,
            job,
            error: None,
        };
        let mut parser = serde_json::Deserializer::from_str(text);
        let result = parser.deserialize_map(Container {
            selection: &self.0,
            context: &mut context,
        });
        if let Err(error) = result {
            return Err(context
                .error
                .take()
                .unwrap_or_else(|| source_error(row, error)));
        }
        parser.end().map_err(|error| source_error(row, error))?;
        job.check_cancelled()
    }
}

fn missing_original() -> AppError {
    AppError::Message("Extraction requires stored original records to identify the JSON source. Reimport this project.".into())
}

/// Version-2 CSV originals are arrays, while JSON originals are objects.
/// Old metadata can omit the format; column labels alone cannot distinguish
/// JSON Pointers from legitimate CSV headers that happen to start with `/`.
fn original_is_json(path: &Path, job: &JobContext) -> Result<bool, AppError> {
    let mut reader = ProjectBatchReader::for_range(path, &[INTERNAL_ORIGINAL.into()], 0, 1)
        .map_err(AppError::from)?;
    let Some(batch) = reader.next_with_job(job) else {
        return Ok(false);
    };
    let batch = batch.map_err(AppError::from)?;
    let original = batch
        .frame
        .column(INTERNAL_ORIGINAL)
        .map_err(|error| AppError::Other(error.into()))?
        .get(0)
        .map_err(|error| AppError::Other(error.into()))?;
    let classify = |text: &str| match text.trim_start().as_bytes().first() {
        Some(b'{') => Ok(true),
        Some(b'[') => Ok(false),
        _ => Err(missing_original()),
    };
    match original {
        AnyValue::String(text) => classify(text),
        AnyValue::StringOwned(text) => classify(text.as_str()),
        _ => Err(missing_original()),
    }
}

fn source_error(row: usize, error: serde_json::Error) -> AppError {
    AppError::Message(format!(
        "Could not extract JSON source row {}: {error}",
        row + 1
    ))
}

struct Context<'a> {
    row: usize,
    matcher: &'a CompiledFormat,
    accumulator: &'a mut Accumulator,
    job: &'a JobContext,
    error: Option<AppError>,
}

impl Context<'_> {
    fn stop<E: de::Error>(&mut self, error: AppError) -> E {
        self.error = Some(error);
        de::Error::custom("JSON extraction stopped")
    }

    fn check<E: de::Error>(&mut self) -> Result<(), E> {
        self.job.check_cancelled().map_err(|error| self.stop(error))
    }

    fn emit(&mut self, text: &str, transient: usize) -> Result<(), AppError> {
        self.job.check_cancelled()?;
        let mut occurrences = self.matcher.occurrences(text);
        while let Some(found) = occurrences.next_with_check(&mut || self.job.check_cancelled())? {
            self.accumulator.observe(self.row, found, transient)?;
        }
        Ok(())
    }

    fn scan(&mut self, raw: &RawValue, selection: &Selection) -> Result<(), AppError> {
        self.job.check_cancelled()?;
        let text = raw.get();
        let mut parser = serde_json::Deserializer::from_str(text);
        let result = match text.as_bytes()[0] {
            b'{' => parser.deserialize_map(Container {
                selection,
                context: self,
            }),
            b'[' => parser.deserialize_seq(Container {
                selection,
                context: self,
            }),
            b'"' if selection.selected => {
                if text.contains('\\') {
                    // The decoded string lives only in serde's scratch buffer.
                    // Bound that transient allocation before asking it to decode.
                    let transient = text.len().saturating_mul(2);
                    if transient > self.accumulator.available() {
                        return Err(limit_error());
                    }
                    parser.deserialize_str(Text {
                        context: self,
                        transient,
                    })
                } else {
                    return self.emit(&text[1..text.len() - 1], 0);
                }
            }
            b'n' => return Ok(()),
            _ if selection.selected => return self.emit(text, 0),
            _ => return Ok(()),
        };
        result.map_err(|error| {
            self.error
                .take()
                .unwrap_or_else(|| source_error(self.row, error))
        })
    }
}

struct SelectedValue<'node, 'context, 'work> {
    selection: &'node Selection,
    context: &'context mut Context<'work>,
}

impl<'de> DeserializeSeed<'de> for SelectedValue<'_, '_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        let raw = <&RawValue>::deserialize(deserializer)?;
        self.context
            .scan(raw, self.selection)
            .map_err(|error| self.context.stop(error))
    }
}

struct Container<'node, 'context, 'work> {
    selection: &'node Selection,
    context: &'context mut Context<'work>,
}

impl<'de> Visitor<'de> for Container<'_, '_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a stored JSON source object or array")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<Key<'de>>()? {
            self.context.check()?;
            if let Some(selection) = self.selection.child(&key.0) {
                map.next_value_seed(SelectedValue {
                    selection,
                    context: &mut *self.context,
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        let mut index = 0usize;
        loop {
            self.context.check()?;
            let selection = if self.selection.selected {
                Some(self.selection)
            } else {
                self.selection.children.get(&index.to_string())
            };
            let present = if let Some(selection) = selection {
                sequence
                    .next_element_seed(SelectedValue {
                        selection,
                        context: &mut *self.context,
                    })?
                    .is_some()
            } else {
                sequence.next_element::<IgnoredAny>()?.is_some()
            };
            if !present {
                return Ok(());
            }
            index += 1;
        }
    }
}

struct Text<'context, 'work> {
    context: &'context mut Context<'work>,
    transient: usize,
}

impl<'de> Visitor<'de> for Text<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON string")
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<(), E> {
        self.context
            .emit(text, self.transient)
            .map_err(|error| self.context.stop(error))
    }
}

struct Key<'de>(Cow<'de, str>);

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;
        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = Key<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON object key")
            }

            fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Borrowed(value)))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(value.into())))
            }
        }
        deserializer.deserialize_str(KeyVisitor)
    }
}

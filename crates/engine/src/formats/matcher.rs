use regex::{Regex, RegexBuilder};

use super::{catalog::validate_format_id, FormatDefinition};
use crate::error::AppError;

#[derive(Debug)]
pub struct CompiledFormat {
    definition: FormatDefinition,
    regex: Regex,
    capture: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatOccurrence<'a> {
    pub value: &'a str,
    /// UTF-8 byte offsets in the decoded original cell, not in the source file.
    pub start: usize,
    pub end: usize,
}

pub struct FormatOccurrences<'a> {
    matcher: &'a CompiledFormat,
    text: &'a str,
    offset: usize,
    finished: bool,
}

impl CompiledFormat {
    pub fn new(definition: &FormatDefinition) -> Result<Self, AppError> {
        validate_format_id(&definition.id)?;
        if serde_json::to_vec(definition)
            .map_err(|error| AppError::Other(error.into()))?
            .len()
            > 256 * 1024
        {
            return Err(AppError::Message(
                "Format definition exceeds 256 KiB.".into(),
            ));
        }
        if definition.schema_version != 1 {
            return Err(AppError::Message(
                "Unsupported format schemaVersion; expected 1.".into(),
            ));
        }
        if definition.name.trim().is_empty()
            || definition.name.len() > 128
            || definition.pattern.is_empty()
            || definition.pattern.len() > 32 * 1024
        {
            return Err(AppError::Message("A format needs a name (maximum 128 bytes) and a nonempty pattern (maximum 32 KiB).".into()));
        }
        let regex = RegexBuilder::new(&definition.pattern)
            .size_limit(2 * 1024 * 1024)
            .dfa_size_limit(2 * 1024 * 1024)
            .build()
            .map_err(|error| AppError::Message(format!("Invalid format regex: {error}")))?;
        let capture = if let Some(name) = &definition.value_group {
            regex
                .capture_names()
                .position(|known| known == Some(name.as_str()))
                .ok_or_else(|| {
                    AppError::Message(format!("Format valueGroup does not name a capture: {name}"))
                })?
        } else {
            0
        };
        let compiled = Self {
            definition: definition.clone(),
            regex,
            capture,
        };
        let examples = definition.examples.as_deref().unwrap_or(&[]);
        if examples.len() > 128 {
            return Err(AppError::Message(
                "A format supports at most 128 examples.".into(),
            ));
        }
        for (index, example) in examples.iter().enumerate() {
            if example.text.len() > 32 * 1024 || example.values.len() > 128 {
                return Err(AppError::Message(
                    "Format examples exceed their size limit.".into(),
                ));
            }
            let actual: Vec<_> = compiled
                .occurrences(&example.text)
                .map(|found| found.value)
                .collect();
            if actual != example.values {
                return Err(AppError::Message(format!(
                    "Format example {} produced unexpected values.",
                    index + 1
                )));
            }
        }
        Ok(compiled)
    }

    pub fn definition(&self) -> &FormatDefinition {
        &self.definition
    }

    pub fn occurrences<'a>(&'a self, text: &'a str) -> FormatOccurrences<'a> {
        FormatOccurrences {
            matcher: self,
            text,
            offset: 0,
            finished: false,
        }
    }

    /// The same accepted, nonempty captures used by extraction determine membership.
    pub fn has_match(&self, text: &str, equals: Option<&str>) -> bool {
        self.occurrences(text)
            .any(|found| equals.is_none_or(|value| found.value == value))
    }
}

impl<'a> Iterator for FormatOccurrences<'a> {
    type Item = FormatOccurrence<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.finished {
            let Some(captures) = self.matcher.regex.captures_at(self.text, self.offset) else {
                self.finished = true;
                return None;
            };
            let whole = captures.get(0).expect("a regex capture has a full match");
            if let Some(value) = captures
                .get(self.matcher.capture)
                .filter(|value| value.start() < value.end())
            {
                if value.end() > self.offset {
                    // Reuse delimiter context, e.g. `IP,IP`, without duplicating
                    // or overlapping captured values. Anchors keep full-cell meaning.
                    self.offset = value.end();
                    if self
                        .matcher
                        .definition
                        .validation
                        .is_some_and(|kind| !super::validation::accepts(kind, value.as_str()))
                    {
                        continue;
                    }
                    return Some(FormatOccurrence {
                        value: value.as_str(),
                        start: value.start(),
                        end: value.end(),
                    });
                }
            }
            let end = whole.end().max(self.offset);
            if end > self.offset {
                self.offset = end;
            } else if end >= self.text.len() {
                self.finished = true;
            } else {
                self.offset = end
                    + self.text[end..]
                        .chars()
                        .next()
                        .expect("character boundary")
                        .len_utf8();
            }
        }
        None
    }
}

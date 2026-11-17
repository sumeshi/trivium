use serde_json::Value;
use std::{borrow::Cow, collections::HashMap};

use super::{lookup, EvalError};
use crate::{condition::model::FormatCondition, error::AppError, formats::CompiledFormat};

#[derive(Debug)]
pub struct PreparedFormat {
    pub(super) columns: Vec<String>,
    matcher: CompiledFormat,
    equals: Option<String>,
    negated: bool,
}

impl PreparedFormat {
    pub(super) fn new(condition: &FormatCondition) -> Result<Self, EvalError> {
        if condition.columns.is_empty()
            || condition
                .columns
                .iter()
                .any(|column| column.trim().is_empty())
        {
            return Err(EvalError {
                message: "A format condition needs at least one source column.".into(),
            });
        }
        let matcher = CompiledFormat::new(&condition.format).map_err(|error| EvalError {
            message: error.to_string(),
        })?;
        Ok(Self {
            columns: condition.columns.clone(),
            matcher,
            equals: condition.equals.clone(),
            negated: condition.negated,
        })
    }

    pub(super) fn eval(&self, row: &HashMap<String, Value>) -> bool {
        let values: Vec<_> = self
            .columns
            .iter()
            .map(|column| {
                lookup(row, column).and_then(|value| match value {
                    Value::Null => None,
                    Value::String(text) => Some(Cow::Borrowed(text.as_str())),
                    other => Some(Cow::Owned(other.to_string())),
                })
            })
            .collect();
        self.eval_cells(values.iter().map(|value| value.as_deref()))
    }

    pub(super) fn eval_cells<'a>(&self, cells: impl Iterator<Item = Option<&'a str>>) -> bool {
        let matched = cells
            .flatten()
            .any(|cell| self.matcher.has_match(cell, self.equals.as_deref()));
        matched ^ self.negated
    }

    pub(super) fn eval_cells_with_check<'a>(
        &self,
        cells: impl Iterator<Item = Option<&'a str>>,
        check: &mut impl FnMut() -> Result<(), AppError>,
    ) -> Result<bool, AppError> {
        for cell in cells.flatten() {
            if self
                .matcher
                .has_match_with_check(cell, self.equals.as_deref(), check)?
            {
                return Ok(!self.negated);
            }
        }
        Ok(self.negated)
    }
}

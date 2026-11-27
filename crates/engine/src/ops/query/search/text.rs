use polars::prelude::*;

use crate::{
    condition::{eval::PreparedCondition, GroupKind, PreparedNode},
    error::AppError,
    jobs::JobContext,
    value_utils::record_stringified_column_count,
};

use super::EVAL_BATCH_ROWS;

/// Reuse the same string scan for a literal and a flat AND/OR of literals.
/// Other predicates and mixed/nested groups stay with the general evaluator.
pub(super) fn evaluate(
    node: &PreparedNode,
    required: &[(&str, &Series)],
    unscoped: &[(&str, &Series)],
    rows: usize,
    eligible: Option<&[bool]>,
    job: &JobContext,
) -> Result<Option<Vec<bool>>, AppError> {
    let Some((kind, terms)) = node.contains_text_group() else {
        return Ok(None);
    };
    if !required
        .iter()
        .chain(unscoped)
        .all(|(_, series)| series.dtype() == &DataType::String)
    {
        return Ok(None);
    }
    let run = |term: &PreparedNode, active: Option<&[bool]>| {
        let (column, condition, inverted) = term.contains_text_comparison().expect("literal group");
        let source = if column.is_empty() {
            unscoped
        } else {
            std::slice::from_ref(
                required
                    .iter()
                    .find(|(name, _)| *name == column)
                    .or_else(|| {
                        required
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case(column))
                    })
                    .expect("validated query column"),
            )
        };
        scan(condition, source, rows, active, job).map(|mask| (mask, inverted))
    };
    if terms.len() == 1 {
        let (mut mask, inverted) = run(&terms[0], eligible)?;
        if inverted {
            for (index, matched) in mask.iter_mut().enumerate() {
                *matched = eligible.is_none_or(|eligible| eligible[index]) && !*matched;
            }
        }
        job.check_cancelled()?;
        return Ok(Some(mask));
    }

    let mut result = if kind == GroupKind::And {
        eligible.map_or_else(|| vec![true; rows], <[bool]>::to_vec)
    } else {
        vec![false; rows]
    };
    let mut remaining = vec![false; rows];
    for term in terms {
        job.check_cancelled()?;
        let active = if kind == GroupKind::And {
            &result
        } else {
            for (index, active) in remaining.iter_mut().enumerate() {
                *active = !result[index] && eligible.is_none_or(|eligible| eligible[index]);
            }
            &remaining
        };
        if !active.iter().any(|active| *active) {
            break;
        }
        let (mask, inverted) = run(term, Some(active))?;
        for (index, child) in mask.into_iter().enumerate() {
            result[index] = if kind == GroupKind::And {
                result[index] && (child ^ inverted)
            } else {
                result[index] || (remaining[index] && (child ^ inverted))
            };
        }
    }
    job.check_cancelled()?;
    Ok(Some(result))
}

fn scan(
    condition: &PreparedCondition,
    source: &[(&str, &Series)],
    rows: usize,
    eligible: Option<&[bool]>,
    job: &JobContext,
) -> Result<Vec<bool>, AppError> {
    job.check_cancelled()?;
    let mut mask = vec![false; rows];
    // Candidate IDs live only for this decoded batch. Full scans keep the
    // sequential string iterator; selective scans do not revisit other rows.
    let selected = eligible
        .filter(|mask| mask.iter().any(|eligible| !*eligible))
        .map(|mask| {
            mask.iter()
                .enumerate()
                .filter_map(|(row, eligible)| eligible.then_some(row))
                .collect::<Vec<_>>()
        });
    let mut remaining = selected.as_ref().map_or(rows, Vec::len);
    if remaining == 0 {
        return Ok(mask);
    }
    for (name, series) in source {
        let strings = series.str().expect("validated string column");
        let count = if let Some(selected) = &selected {
            scan_column(
                condition,
                selected.iter().map(|row| (*row, strings.get(*row))),
                &mut mask,
                &mut remaining,
                job,
            )?
        } else {
            scan_column(
                condition,
                strings.into_iter().enumerate(),
                &mut mask,
                &mut remaining,
                job,
            )?
        };
        record_stringified_column_count(name, count);
        if remaining == 0 {
            break;
        }
    }
    job.check_cancelled()?;
    Ok(mask)
}

fn scan_column<'a>(
    condition: &PreparedCondition,
    cells: impl Iterator<Item = (usize, Option<&'a str>)>,
    mask: &mut [bool],
    remaining: &mut usize,
    job: &JobContext,
) -> Result<u64, AppError> {
    let mut count = 0;
    for (scanned, (index, cell)) in cells.enumerate() {
        if scanned.is_multiple_of(EVAL_BATCH_ROWS) {
            job.check_cancelled()?;
        }
        if mask[index] {
            continue;
        }
        if let Some(cell) = cell {
            count += 1;
            job.check_cancelled()?;
            if condition.contains_text(Some(cell)) {
                mask[index] = true;
                *remaining -= 1;
            }
        }
    }
    Ok(count)
}

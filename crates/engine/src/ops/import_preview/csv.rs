use super::{ImportPreview, MAX_COLUMNS, MAX_ROWS};
use crate::{error::AppError, jobs::JobContext};

#[derive(Clone, Copy)]
enum State {
    Start,
    Plain,
    Quoted,
    AfterQuote,
}

/// Keep only records whose boundary is known when the sample ends mid-file.
/// This mirrors CSV quoting; it does not split a multiline quoted value.
fn complete_prefix(text: &str, complete: bool, separator: u8) -> (&str, bool) {
    let mut state = State::Start;
    let mut end = 0;
    for (offset, byte) in text.bytes().enumerate() {
        let newline = matches!(byte, b'\r' | b'\n');
        state = match state {
            State::Start if byte == b'"' => State::Quoted,
            State::Start if byte == separator || newline => State::Start,
            State::Start => State::Plain,
            State::Plain if byte == separator || newline => State::Start,
            State::Plain => State::Plain,
            State::Quoted if byte == b'"' => State::AfterQuote,
            State::Quoted => State::Quoted,
            State::AfterQuote if byte == b'"' => State::Quoted,
            State::AfterQuote if byte == separator || newline => State::Start,
            State::AfterQuote => State::Plain,
        };
        if newline && !matches!(state, State::Quoted) {
            end = offset + 1;
        }
    }
    if complete && !matches!(state, State::Quoted) {
        (text, false)
    } else {
        (&text[..end], end != text.len())
    }
}

fn reader(text: &str, separator: u8) -> csv::Reader<std::io::Cursor<Vec<u8>>> {
    // csv-core consumes one UTF-8 BOM. Supply a marker so a literal leading
    // U+FEFF in already-decoded source text remains evidence.
    let mut bytes = Vec::with_capacity(text.len() + 3);
    bytes.extend_from_slice(b"\xef\xbb\xbf");
    bytes.extend_from_slice(text.as_bytes());
    csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(separator)
        .from_reader(std::io::Cursor::new(bytes))
}

fn candidate(text: &str, complete: bool, separator: u8) -> Option<usize> {
    let (text, _) = complete_prefix(text, complete, separator);
    let rows = reader(text, separator)
        .records()
        .take(MAX_ROWS + 1)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    let width = rows.first()?.len();
    (rows.len() >= 2 && width > 1 && rows.iter().all(|row| row.len() == width)).then_some(width)
}

pub(super) fn preview(
    text: &str,
    complete: bool,
    delimiter: Option<&str>,
    has_headers: bool,
    result: &mut ImportPreview,
    job: &JobContext,
) -> Result<(), AppError> {
    let separator = crate::import::csv::build_delimiter(delimiter)?.unwrap_or(b',');
    let current = candidate(text, complete, separator);
    if current.is_none() {
        let candidates = b",\t;|"
            .iter()
            .copied()
            .filter(|candidate| *candidate != separator)
            .filter_map(|separator| {
                candidate(text, complete, separator).map(|width| (separator, width))
            })
            .collect::<Vec<_>>();
        if candidates.len() == 1 {
            result.suggested_delimiter = Some(char::from(candidates[0].0).to_string());
        }
    }
    let (text, omitted) = complete_prefix(text, complete, separator);
    if omitted {
        result.truncated = true;
        result.warning("An incomplete CSV record at the end of the sample is not shown.");
    }
    let mut records = reader(text, separator);
    let mut records = records.records();
    let mut headers = Vec::new();
    let mut expected = 0;
    if has_headers {
        if let Some(record) = records.next() {
            let record = record.map_err(|error| {
                AppError::Message(format!("Failed to preview CSV header: {error}"))
            })?;
            expected = record.len();
            headers = record.iter().take(MAX_COLUMNS).map(str::to_owned).collect();
        }
    }
    let mut width = expected;
    let mut rows = Vec::new();
    let mut unequal = false;
    for record in records.take(MAX_ROWS + 1) {
        job.check_cancelled()?;
        match record {
            Ok(record) => {
                if rows.len() == MAX_ROWS {
                    result.truncated = true;
                    break;
                }
                if !has_headers && rows.is_empty() {
                    expected = record.len();
                }
                unequal |= record.len() != expected;
                width = width.max(record.len());
                rows.push(
                    record
                        .iter()
                        .take(MAX_COLUMNS)
                        .map(|text| Some(text.to_owned()))
                        .collect::<Vec<_>>(),
                );
            }
            Err(error) => {
                result.warning(format!("CSV sample error: {error}"));
                break;
            }
        }
    }
    if unequal {
        result.warning("Sampled CSV records have different field counts. Strict import will reject unequal records.");
    }
    if width > MAX_COLUMNS {
        result.truncated = true;
        result.warning("Only the first 20 columns are shown.");
    }
    result.columns = (0..width.min(MAX_COLUMNS))
        .map(|index| {
            let label = headers
                .get(index)
                .filter(|label| !label.is_empty())
                .cloned()
                .unwrap_or_else(|| format!("column {}", index + 1));
            result.clip(&label, 80)
        })
        .collect();
    for mut row in rows {
        row.resize(result.columns.len(), None);
        if !result.add_row(row) {
            break;
        }
    }
    Ok(())
}

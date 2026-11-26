//! Schema discovery for explicitly requested CSV recovery. The normal strict
//! path reads only the header before handing control back to the batch reader.
use std::{
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use super::{
    invalid_delimiter, valid_delimiter, validate_column_count, validate_record_width, BatchLimits,
};
use crate::{
    error::AppError,
    import::{
        column_id,
        options::{ImportIssueAction, ImportOptions, ImportReport},
        utf16::{detect_bom, Encoding, Utf16Reader},
    },
    jobs::JobContext,
    models::ColumnMeta,
};

mod quotes;
pub(super) use quotes::unreadable_tail;

pub(super) struct Prepared {
    pub reader: csv::Reader<Box<dyn Read>>,
    pub columns: Vec<ColumnMeta>,
    pub report: ImportReport,
}

fn open_reader(
    path: &Path,
    separator: u8,
    limits: BatchLimits,
    options: &ImportOptions,
    job: &JobContext,
) -> Result<csv::Reader<Box<dyn Read>>, AppError> {
    let mut file =
        File::open(path).map_err(|e| AppError::Message(format!("Failed to open file: {e}")))?;
    let (encoding, skip) = detect_bom(&mut file)
        .map_err(|e| AppError::Message(format!("Failed to read file: {e}")))?;
    // csv-core removes one leading UTF-8 BOM itself. Do not strip it twice:
    // a second U+FEFF is evidence, not another encoding marker.
    file.seek(SeekFrom::Start(if encoding == Encoding::Utf8 {
        0
    } else {
        skip
    }))
    .map_err(|e| AppError::Message(format!("Failed to read file: {e}")))?;
    let mut input: Box<dyn Read> = match encoding {
        Encoding::Utf8 => Box::new(file),
        Encoding::Utf16Le | Encoding::Utf16Be => {
            Box::new(Cursor::new([0xEF, 0xBB, 0xBF]).chain(Utf16Reader::new(
                BufReader::with_capacity(limits.read_buffer, file),
                encoding == Encoding::Utf16Be,
            )))
        }
    };
    // csv-core treats an input chunk containing only a BOM as EOF. Coalesce
    // the first four bytes, including across the synthetic UTF-16 marker,
    // and ensure the parser can buffer the marker plus one payload byte.
    let mut prefix = [0u8; 4];
    let mut prefix_len = 0;
    while prefix_len < prefix.len() {
        match input.read(&mut prefix[prefix_len..]) {
            Ok(0) => break,
            Ok(read) => prefix_len += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(AppError::Message(format!("Failed to read file: {error}"))),
        }
    }
    let input: Box<dyn Read> = Box::new(Cursor::new(prefix[..prefix_len].to_vec()).chain(input));
    let input: Box<dyn Read> =
        Box::new(quotes::QuoteCheckedReader::new(input, separator, job.id()));
    Ok(csv::ReaderBuilder::new()
        .delimiter(separator)
        .has_headers(options.has_headers)
        .flexible(options.recover)
        .buffer_capacity(limits.read_buffer.max(4))
        .from_reader(input))
}

pub(super) fn prepare(
    path: &Path,
    delimiter: Option<u8>,
    limits: BatchLimits,
    options: &ImportOptions,
    job: &JobContext,
    sink: Option<&crate::import::diagnostics::IssueSink>,
) -> Result<Prepared, AppError> {
    job.check_cancelled()?;
    let separator = delimiter.unwrap_or(b',');
    if !valid_delimiter(separator) {
        return Err(invalid_delimiter());
    }
    let mut reader = open_reader(path, separator, limits, options, job)?;
    let mut headers = if options.has_headers || !options.recover {
        reader
            .headers()
            .map_err(|e| AppError::Message(format!("Failed to parse the CSV header: {e}")))?
            .clone()
    } else {
        csv::StringRecord::new()
    };
    validate_column_count(&headers)?;
    let original_width = headers.len();
    let header_line = headers.position().map(|position| position.line() as usize);
    if options.recover {
        let mut width = original_width;
        // A complete leading annotation block keeps the same allowance as the
        // strict importer. Generated ordinary names never grant an allowance.
        for record in reader.byte_records() {
            job.check_cancelled()?;
            let record = match record {
                Ok(record) => record,
                Err(error) if unreadable_tail(&error).is_some() => break,
                Err(error) => {
                    return Err(AppError::Message(format!(
                        "Failed to parse the CSV data: {error}"
                    )))
                }
            };
            let location = format!(
                "CSV record at line {}",
                record.position().map_or(1, csv::Position::line)
            );
            validate_record_width(&headers, record.len(), &location)?;
            if record
                .iter()
                .any(|field| std::str::from_utf8(field).is_err())
            {
                continue;
            }
            if record.len() > width {
                width = record.len();
            }
        }
        while headers.len() < width {
            headers.push_field("");
        }
        reader = open_reader(path, separator, limits, options, job)?;
        if options.has_headers {
            reader
                .headers()
                .map_err(|e| AppError::Message(format!("Failed to parse the CSV header: {e}")))?;
        }
    }
    if headers.is_empty() {
        return Err(AppError::Message(
            "The CSV data has no readable columns.".into(),
        ));
    }
    let mut report = ImportReport {
        has_headers: Some(options.has_headers),
        ..ImportReport::default()
    };
    let columns = headers
        .iter()
        .enumerate()
        .map(|(order, header)| {
            let generated = !options.has_headers || header.is_empty();
            if generated {
                report.generated_columns += 1;
            }
            let label = if generated {
                format!("column {}", order + 1)
            } else {
                header.to_owned()
            };
            ColumnMeta {
                id: column_id(order),
                source: if !options.has_headers || order >= original_width {
                    label.clone()
                } else {
                    header.to_owned()
                },
                label,
                order,
            }
        })
        .collect();
    if report.generated_columns > 0 {
        let generated_columns = report.generated_columns;
        crate::import::diagnostics::issue(
            &mut report,
            sink,
            if options.has_headers {
                header_line
            } else {
                None
            },
            ImportIssueAction::Repaired,
            format!(
                "Generated names for {} columns; source values were retained.",
                generated_columns
            ),
        );
    }
    Ok(Prepared {
        reader,
        columns,
        report,
    })
}

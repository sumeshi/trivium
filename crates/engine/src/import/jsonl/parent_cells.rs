//! Bound text duplicated into parent columns of a mixed-structure schema.
use std::io::{self, Write};

use serde_json::Value;

use crate::error::AppError;

pub(super) struct ParentCells {
    limit: usize,
    remaining: usize,
}

impl ParentCells {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: limit,
        }
    }

    pub fn serialize(&mut self, value: &Value, record: usize) -> Result<String, AppError> {
        let mut writer = BoundedText {
            bytes: Vec::new(),
            limit: self.remaining,
            full: false,
        };
        if let Err(error) = serde_json::to_writer(&mut writer, value) {
            let message = if writer.full {
                format!("JSON record {record} exceeds the {}-byte per-record budget for additional parent-object cell text. No fields were discarded.", self.limit)
            } else {
                format!("Could not serialize JSON record {record} parent-object cell: {error}")
            };
            return Err(AppError::Message(message));
        }
        self.remaining -= writer.bytes.len();
        String::from_utf8(writer.bytes)
            .map_err(|_| AppError::Message("Could not encode JSON parent-object cell text.".into()))
    }
}

struct BoundedText {
    bytes: Vec<u8>,
    limit: usize,
    full: bool,
}

impl Write for BoundedText {
    fn write(&mut self, text: &[u8]) -> io::Result<usize> {
        if text.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.full = true;
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "parent-object text budget exceeded",
            ));
        }
        let needed = self.bytes.len() + text.len();
        if needed > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(needed)
                .max(64)
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        self.bytes.extend_from_slice(text);
        Ok(text.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parent_budget_is_cumulative_and_preserves_exact_json_text() {
        let value = json!({"path":"a/b", "text":"紫\n\""});
        let text = value.to_string();
        let mut cells = ParentCells::new(text.len() * 2);
        assert_eq!(cells.serialize(&value, 1).unwrap(), text);
        assert_eq!(cells.serialize(&value, 1).unwrap(), text);
        let error = cells.serialize(&value, 1).unwrap_err().to_string();
        assert!(
            error.contains("record 1") && error.contains("parent-object"),
            "{error}"
        );
    }

    #[test]
    fn serialization_refuses_writes_before_reserving_over_budget() {
        let mut writer = BoundedText {
            bytes: Vec::new(),
            limit: 80,
            full: false,
        };
        writer.write_all(&[b'x'; 70]).unwrap();
        assert!(writer.bytes.capacity() <= 80);
        assert!(writer.write_all(&[b'x'; 11]).is_err());
        assert_eq!(writer.bytes.len(), 70);
        assert!(writer.bytes.capacity() <= 80);
    }
}

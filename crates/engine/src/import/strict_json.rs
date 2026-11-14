//! JSON values that reject duplicate object keys, including nested objects.
//!
//! Use a reader-backed serde_json deserializer: its input strings are copied,
//! whereas arbitrary-precision numbers use a borrowed synthetic map key. This
//! distinction preserves exact number text without treating a real input key
//! named `$serde_json::private::Number` as a number.

use std::fmt;
use std::io::Read;

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Number, Value};

pub(crate) fn from_reader<R: Read>(reader: R) -> serde_json::Result<Value> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let value = ValueSeed::new(String::new()).deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value)
}

/// Recursively reads one value at its JSON Pointer path. Only use with
/// serde_json::Deserializer::from_reader; see the module-level invariant.
pub(super) struct ValueSeed {
    path: String,
}

impl ValueSeed {
    pub(super) fn new(path: String) -> Self {
        Self { path }
    }

    fn child(&self, key: &str) -> Self {
        Self::new(format!(
            "{}/{}",
            self.path,
            key.replace('~', "~0").replace('/', "~1")
        ))
    }
}

impl<'de> DeserializeSeed<'de> for ValueSeed {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ValueSeed {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value with unique object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(self.child(&values.len().to_string()))? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = map.next_key::<ObjectKey>()? {
            match key {
                ObjectKey::Number => {
                    // serde_json's arbitrary_precision representation supplies
                    // the original decimal/exponent text as one string value.
                    let text = map.next_value::<String>()?;
                    return text
                        .parse::<Number>()
                        .map(Value::Number)
                        .map_err(de::Error::custom);
                }
                ObjectKey::Input(key) => {
                    let child = self.child(&key);
                    if values.contains_key(&key) {
                        return Err(de::Error::custom(format!(
                            "Duplicate JSON key {key:?} at {}",
                            child.path
                        )));
                    }
                    let value = map.next_value_seed(child)?;
                    values.insert(key, value);
                }
            }
        }
        Ok(Value::Object(values))
    }
}

enum ObjectKey {
    Input(String),
    Number,
}

impl<'de> Deserialize<'de> for ObjectKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct KeyVisitor;

        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = ObjectKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON object key")
            }

            fn visit_str<E>(self, key: &str) -> Result<ObjectKey, E> {
                Ok(ObjectKey::Input(key.to_owned()))
            }

            fn visit_string<E>(self, key: String) -> Result<ObjectKey, E> {
                Ok(ObjectKey::Input(key))
            }

            fn visit_borrowed_str<E>(self, key: &'de str) -> Result<ObjectKey, E> {
                if key == "$serde_json::private::Number" {
                    Ok(ObjectKey::Number)
                } else {
                    Ok(ObjectKey::Input(key.to_owned()))
                }
            }
        }

        deserializer.deserialize_str(KeyVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::from_reader;

    #[test]
    fn exact_numbers_keep_their_text_even_inside_arrays() {
        let source = br#"{"n":[170141183460469231731687303715884105728,1.2345678901234567890123456789,1e+400,-0]}"#;
        let value = from_reader(&source[..]).expect("exact numbers");
        assert_eq!(
            value["n"].to_string(),
            "[170141183460469231731687303715884105728,1.2345678901234567890123456789,1e+400,-0]"
        );
    }

    #[test]
    fn literal_number_marker_is_an_ordinary_input_key() {
        let source = br#"{"$serde_json::private::Number":"not a number","n":1.25}"#;
        let value = from_reader(&source[..]).expect("ordinary input object");
        assert_eq!(value["$serde_json::private::Number"], "not a number");
        assert_eq!(value["n"].to_string(), "1.25");

        let duplicate =
            br#"{"$serde_json::private::Number":"1","\u0024serde_json::private::Number":"2"}"#;
        let error = from_reader(&duplicate[..]).expect_err("duplicate marker key");
        assert!(error.to_string().contains("Duplicate JSON key"));
    }

    #[test]
    fn duplicate_error_uses_decoded_keys_and_json_pointer_escaping() {
        let source = br#"{"a/b":{"~":[{"user":"alice","\u0075ser":"bob"}]}}"#;
        let error = from_reader(&source[..]).expect_err("nested duplicate");
        assert!(error
            .to_string()
            .contains("Duplicate JSON key \"user\" at /a~1b/~0/0/user"));
    }
}

use serde::Deserialize;
use serde_json::{json, Value};
use trivium_engine::store::ProjectsStore;

use super::dispatch;

fn version_one() -> u32 {
    1
}
fn empty_params() -> Value {
    json!({})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(default = "version_one")]
    version: u32,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default = "empty_params")]
    params: Value,
}

pub(super) struct Failure {
    pub code: &'static str,
    pub message: String,
}

impl Failure {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub(super) fn error(id: Option<Value>, code: &str, message: &str) -> Value {
    json!({"version":1, "id":id, "ok":false, "error":{"code":code,"message":message}})
}

pub(super) fn respond(store: &ProjectsStore, line: &[u8]) -> Value {
    let value: Value = match serde_json::from_slice(line) {
        Ok(value) => value,
        Err(failure) => return error(None, "invalid_json", &failure.to_string()),
    };
    let id = value.get("id").cloned();
    let request: Request = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(failure) => return error(id, "invalid_request", &failure.to_string()),
    };
    if request.version != 1 {
        return error(
            request.id,
            "unsupported_version",
            "Only protocol version 1 is supported.",
        );
    }
    if !request.params.is_object() {
        return error(
            request.id,
            "invalid_params",
            "params must be a JSON object.",
        );
    }
    if request
        .id
        .as_ref()
        .is_some_and(|id| !id.is_null() && !id.is_string() && !id.is_number())
    {
        return error(
            None,
            "invalid_request",
            "id must be a string, number, or null.",
        );
    }
    match dispatch::dispatch(store, &request.method, request.params) {
        Ok(result) => json!({"version":1, "id":request.id, "ok":true, "result":result}),
        Err(failure) => error(request.id, failure.code, &failure.message),
    }
}

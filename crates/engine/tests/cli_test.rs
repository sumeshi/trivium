//! Exercise the shipped binary over real OS pipes, including a new process
//! reopening the same workspace. No mocked IPC or alternate business logic.
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

use serde_json::{json, Value};
use uuid::Uuid;

struct Session {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    id: u64,
}

impl Session {
    fn start(workspace: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_trivium-cli"))
            .arg("--workspace")
            .arg(workspace)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        Self {
            input: child.stdin.take(),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
            id: 0,
        }
    }

    fn raw(&mut self, bytes: &[u8]) -> Value {
        let input = self.input.as_mut().unwrap();
        input.write_all(bytes).unwrap();
        input.flush().unwrap();
        self.receive()
    }

    fn receive(&mut self) -> Value {
        let mut line = String::new();
        assert!(
            self.output.read_line(&mut line).unwrap() > 0,
            "CLI exited without a response"
        );
        serde_json::from_str(&line)
            .unwrap_or_else(|error| panic!("stdout is not JSON protocol: {line:?}: {error}"))
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        let mut request =
            serde_json::to_vec(&json!({"version":1,"id":self.id,"method":method,"params":params}))
                .unwrap();
        request.push(b'\n');
        let response = self.raw(&request);
        assert_eq!(response["version"], 1);
        assert_eq!(response["id"], self.id);
        assert_eq!(response["ok"], true, "{response}");
        response["result"].clone()
    }

    fn finish(mut self) {
        drop(self.input.take());
        assert!(self.child.wait().unwrap().success());
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn column(loaded: &Value, source: &str) -> String {
    loaded["columns_meta"]
        .as_array()
        .unwrap()
        .iter()
        .find(|column| column["source"] == source)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn import_query_annotations_bulk_export_view_and_reopen_use_the_real_engine() {
    let root = std::env::temp_dir().join(format!("trivium-cli-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("source.jsonl");
    fs::write(&source, "{\"name\":\"alpha\",\"number\":9007199254740993,\"time\":\"2026-11-08T01:02:03.000000001+09:00\"}\n{\"name\":\"beta\",\"number\":9007199254740992}\n{\"name\":null}\n{}\n{\"name\":\"\"}\n").unwrap();
    let workspace = root.join("workspace");
    let mut cli = Session::start(&workspace);
    let hello = cli.call("hello", json!({}));
    assert_eq!(hello["protocolVersion"], 1);
    assert!(hello["methods"]
        .as_array()
        .unwrap()
        .contains(&json!("bulk_apply")));
    assert_eq!(cli.call("list", json!({})), json!([]));
    let imported = cli.call("import", json!({"path":source}));
    let id = imported["meta"]["id"].as_str().unwrap().to_string();
    let loaded = cli.call("load", json!({"projectId":id}));
    assert_eq!(
        loaded["initial_rows"],
        json!([]),
        "CLI load is metadata-only"
    );
    let name = column(&loaded, "/name");
    let number = column(&loaded, "/number");
    let time = column(&loaded, "/time");
    let condition = json!({"column":number,"op":"eq","values":["9007199254740993"]});
    let query = cli.call("query", json!({"projectId":id,"condition":condition}));
    assert_eq!(query["total_filtered_rows"], 1);
    assert_eq!(query["rows"][0]["row_index"], 0);
    assert_eq!(
        query["rows"][0]["cells"][&number]["text"],
        "9007199254740993"
    );
    let location = cli.call(
        "locate",
        json!({"projectId":id,"rowIndex":0,"condition":condition}),
    );
    assert_eq!(location["position"], 0);
    let cell = cli.call("cell", json!({"projectId":id,"rowIndex":0,"columnId":time}));
    assert_eq!(cell["text"], "2026-11-08T01:02:03.000000001+09:00");
    let updated = cli.call("update_flag", json!({"projectId":id,"row_index":0,"flag":"critical","memo":"reviewed","tags":["manual"],"bookmarked":true}));
    assert_eq!(updated["entry"]["flag"], "critical");
    let stats = cli.call(
        "stats",
        json!({"projectId":id,"column":number,"flagFilter":"critical"}),
    );
    assert_eq!(stats["unique"], 1);
    assert_eq!(stats["missing"], 0);
    let grouping = cli.call(
        "group_tree",
        json!({"projectId":id,"groupColumns":[name],"flagFilter":"critical"}),
    );
    assert_eq!(grouping["total"], 1);
    assert_eq!(grouping["nodes"][0]["label"], "alpha");
    let histogram = cli.call(
        "histogram",
        json!({"projectId":id,"column":number,"flagFilter":"critical","buckets":4}),
    );
    assert_eq!(
        histogram["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| bucket["count"].as_u64().unwrap())
            .sum::<u64>(),
        1
    );
    assert_eq!(
        histogram["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| bucket["flags"]["critical"].as_u64().unwrap())
            .sum::<u64>(),
        1
    );
    let analysis = cli.call(
        "time_analysis",
        json!({"projectId":id,"column":time,"flagFilter":"critical"}),
    );
    assert_eq!(analysis["column"], time);
    assert_eq!(analysis["gaps"], json!([]));
    let surrounding = cli.call(
        "surround",
        json!({"projectId":id,"rowIndex":0,"column":time,"mode":"neighbors","count":1}),
    );
    assert_eq!(surrounding["rows"][0]["row_index"], 0);
    let unique = cli.call(
        "dupes",
        json!({"projectId":id,"columns":[number],"mode":"unique"}),
    );
    assert_eq!(unique["rows"].as_array().unwrap().len(), 2);
    let diff = cli.call(
        "diff",
        json!({"projectIdA":id,"projectIdB":id,"keyColumns":[name]}),
    );
    assert!(diff["status_counts"]["same"].as_u64().unwrap() > 0);
    let report_path = root.join("report.html");
    let report = cli.call(
        "report",
        json!({"projectId":id,"destination":report_path,"flagFilter":"critical"}),
    );
    assert_eq!(report["hit_rows"], 1);
    assert!(fs::metadata(report_path).unwrap().len() > 0);
    let mut bulk =
        json!({"projectId":id,"target":"selection","rowIndices":[1],"flag":"suspicious"});
    let preview = cli.call("bulk_preview", bulk.clone());
    assert_eq!(preview["count"], 1);
    bulk["previewToken"] = preview["previewToken"].clone();
    assert_eq!(cli.call("bulk_apply", bulk)["updated"], 1);
    assert_eq!(cli.call("bulk_undo", json!({"projectId":id}))["updated"], 1);
    cli.call(
        "save_view",
        json!({"projectId":id,"state":{"version":1,"search":"alpha","logicalRow":2}}),
    );
    assert_eq!(
        cli.call("load_view", json!({"projectId":id}))["state"]["search"],
        "alpha"
    );
    cli.call(
        "set_hidden_columns",
        json!({"projectId":id,"hidden_columns":[number]}),
    );
    let hidden = cli.call("query", json!({"projectId":id,"condition":condition}));
    assert!(hidden["rows"][0]["cells"].get(&number).is_none());
    assert_eq!(
        hidden["total_filtered_rows"], 1,
        "hidden columns changed query membership"
    );
    cli.call(
        "set_hidden_columns",
        json!({"projectId":id,"hidden_columns":[]}),
    );
    let exported = root.join("selected.csv");
    assert_eq!(cli.call("export", json!({"projectId":id,"destination":exported,"scope":"selected","rowIndices":[0],"columns":[name,number,time]}))["rows"], 1);
    let mut csv = csv::Reader::from_path(exported).unwrap();
    let header = csv.headers().unwrap().clone();
    let row = csv.records().next().unwrap().unwrap();
    let number_index = header.iter().position(|name| name == "number").unwrap();
    assert_eq!(&row[number_index], "9007199254740993");
    cli.finish();

    let mut reopened = Session::start(&workspace);
    assert_eq!(
        reopened.call("list", json!({})).as_array().unwrap().len(),
        1
    );
    let flags = reopened.call("query", json!({"projectId":id,"flagFilter":"critical"}));
    assert_eq!(flags["rows"][0]["memo"], "reviewed");
    assert_eq!(flags["annotated_total"], 1);
    assert_eq!(
        reopened.call("load_view", json!({"projectId":id}))["state"]["logicalRow"],
        2
    );
    let empty = reopened.call(
        "query",
        json!({"projectId":id,"condition":{"column":name,"op":"eq","values":[""]}}),
    );
    assert_eq!(
        empty["total_filtered_rows"], 1,
        "null/missing became an empty string"
    );
    assert_eq!(empty["rows"][0]["row_index"], 4);
    reopened.finish();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn protocol_errors_are_structured_and_do_not_desynchronize_following_requests() {
    let root = std::env::temp_dir().join(format!("trivium-cli-errors-{}", Uuid::new_v4()));
    let missing = Command::new(env!("CARGO_BIN_EXE_trivium-cli"))
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(2));
    assert!(missing.stdout.is_empty());
    assert!(String::from_utf8(missing.stderr)
        .unwrap()
        .contains("--workspace"));
    let mut cli = Session::start(&root);
    for (request, code, id) in [
        ("broken\n", "invalid_json", Value::Null),
        (
            "{\"id\":\"missing\",\"method\":\"missing\"}\n",
            "unknown_method",
            json!("missing"),
        ),
        (
            "{\"id\":2,\"version\":2,\"method\":\"hello\"}\n",
            "unsupported_version",
            json!(2),
        ),
        (
            "{\"id\":3,\"method\":\"query\",\"params\":{}}\n",
            "invalid_params",
            json!(3),
        ),
        (
            "{\"id\":4,\"method\":\"hello\",\"params\":[]}\n",
            "invalid_params",
            json!(4),
        ),
        (
            "{\"id\":[],\"method\":\"hello\"}\n",
            "invalid_request",
            Value::Null,
        ),
    ] {
        let error = cli.raw(request.as_bytes());
        assert_eq!(error["ok"], false);
        assert_eq!(error["id"], id);
        assert_eq!(error["error"]["code"], code);
    }
    let mut oversized = vec![b'x'; 8 * 1024 * 1024 + 1];
    oversized.push(b'\n');
    assert_eq!(cli.raw(&oversized)["error"]["code"], "request_too_large");
    assert_eq!(cli.call("hello", json!({}))["protocolVersion"], 1);
    // A final JSON value without a newline is still processed at EOF.
    cli.input
        .as_mut()
        .unwrap()
        .write_all(b"{\"id\":\"eof\",\"method\":\"hello\"}")
        .unwrap();
    drop(cli.input.take());
    assert_eq!(cli.receive()["id"], "eof");
    cli.finish();
    fs::remove_dir_all(root).unwrap();
}

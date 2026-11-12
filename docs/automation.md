# Automation with the Rust engine

`trivium-cli` exposes the same import, query, annotation, view and export
operations as the application. It runs without Tauri, a browser, a web server
or a Python installation. Python and other programs communicate over ordinary
stdin/stdout pipes; they do not implement a second filtering engine.

Build it with:

```sh
cargo build --locked -p trivium-engine --release --bin trivium-cli
target/release/trivium-cli --workspace /path/to/workspace
```

On Windows the executable is `target\release\trivium-cli.exe`. Use an explicit
workspace directory, either an existing Trivium workspace or a separate one.
Import creates saved cases there. Closing the CLI preserves cases, annotations,
bulk undo and saved views. Data and annotation compatibility is shared with the
desktop application.

## Protocol version 1

Send one UTF-8 JSON object per line. Requests are processed sequentially, and
each receives one flushed JSON response on stdout. Diagnostics go to stderr.
EOF exits after the final request; its trailing newline is optional. Each
request is limited to 8 MiB; an oversized line is discarded without losing the
following request. Query results are paged; large exports go to a destination
file rather than stdout.

```json
{"version":1,"id":"example","method":"hello","params":{}}
```

`version` defaults to `1`; `id` may be a string, number, null or omitted.
`params` defaults to `{}` and must be an object. Unsupported versions, malformed
requests and operation failures return structured errors and leave the session
available for the next request.

```json
{"version":1,"id":"example","ok":true,"result":{"protocolVersion":1,"engineVersion":"...","methods":["..."]}}
{"version":1,"id":"example","ok":false,"error":{"code":"operation_failed","message":"Project not found."}}
```

Use `ok`, then `result` or `error`; do not infer success from the process exit
status. A session with request errors can exit successfully at EOF. Startup or
transport errors exit with status `2` and a diagnostic on stderr.

## Methods and parameters

The parameter and result objects are the existing engine DTOs. QuerySpec fields
are flattened into `params`, using the same search and condition syntax as the
application. `load` returns metadata and column definitions without an initial
table page; obtain rows with `query`.

| Method | Parameters |
| --- | --- |
| `hello` | `{}`; returns version and the supported method names |
| `list` | `{}`; returns saved case summaries |
| `import` | `path`, optional `description`, `delimiter`, `restore_annotations` |
| `load` | `projectId` |
| `query` | `projectId`, QuerySpec, optional `offset`, `limit` (default 250) |
| `locate` | `projectId`, original `rowIndex`, QuerySpec |
| `cell` | `projectId`, original `rowIndex`, `columnId` |
| `update_flag` | `projectId`, original `row_index`, optional `flag`, `memo`, `tags`, `bookmarked` |
| `bulk_preview` | `projectId`, `target` (`selection`, `filtered`, `all`), optional `rowIndices`, QuerySpec and annotation patch |
| `bulk_apply` | Same as preview, plus its `previewToken` |
| `bulk_undo` | `projectId` |
| `export` | `projectId`, `destination`, optional `scope` (`filtered`, `all`, `selected`), QuerySpec, `columns`, `rowIndices`, `includeAnnotations` |
| `load_view` | `projectId` |
| `save_view` | `projectId`, `state` (including its `version`) |
| `set_hidden_columns` | `projectId`, `hidden_columns` (internal column IDs) |
| `stats` | `projectId`, `column`, QuerySpec, optional `rareMax` |
| `histogram` | `projectId`, `column`, optional query filters, `buckets`, `range: {min, max}`, `focus` |
| `time_analysis` | `projectId`, `column`, optional query filters, `gapSeconds`, `sessionMinutes`, `burstWindowSeconds`, `burstMin` |
| `group_tree` | `projectId`, `groupColumns`, optional query filters |
| `surround` | `projectId`, `rowIndex`, time `column`, `mode` (`neighbors`, `window`), `count` |
| `dupes` | `projectId`, key `columns`, `mode` (`duplicates`, `unique`) |
| `diff` | `projectIdA`, `projectIdB`, `keyColumns`, optional CSV `destination` |
| `report` | `projectId`, HTML `destination`, optional query filters, `columns`, `sortKey`, `sortDirection` |

QuerySpec comprises `search`, `searchColumns`, `condition`, `flagFilter`,
`bookmarkedOnly`, `sortKey` and `sortDirection`. A condition uses `column`, `op`,
`values`, `negated`; AND/OR groups use `kind` and `children`. `flagFilter` can be
`safe|critical`, for example; an empty/all selection means all rows. `columns`
on export and hidden columns affect presentation/output, not search scope.
Statistics, histograms, time analysis and grouping accept the same search,
search scope, conditions, flag filters and bookmark filters as the table.
Surrounding rows and duplicate detection operate on their explicit case/key
parameters, and comparison explicitly selects two cases. Reports accept the
same filters and display order as the table. These methods call the existing
analysis operations; their result shapes and precision rules are shared.

`load.columns_meta` maps stable internal column IDs to their display labels and
source CSV headers / JSON Pointers. Use those IDs for conditions, cells and
output columns. `search` also accepts unambiguous display-label scopes, exactly
as the table does. Original row indices are **zero-based**; the table's `#`
column shows that index plus one. `query.offset` addresses the current result's
position. `locate` returns a result position or null without clearing filters.

Cell `text` preserves raw numeric text and timestamp precision. Null and missing
remain distinct cell kinds and are not empty strings. Large numbers and
nanosecond timestamps should be supplied as **strings in condition values**;
do not convert them to a floating-point value. Query cells may be truncated
previews; `cell` returns the full original text.

Example requests (substitute the imported `meta.id` and actual column ID):

```json
{"id":1,"method":"import","params":{"path":"/evidence/events.jsonl"}}
{"id":2,"method":"load","params":{"projectId":"PROJECT_UUID"}}
{"id":3,"method":"query","params":{"projectId":"PROJECT_UUID","condition":{"column":"c000002","op":"eq","values":["9007199254740993"]},"offset":0,"limit":100}}
{"id":4,"method":"update_flag","params":{"projectId":"PROJECT_UUID","row_index":17,"flag":"critical","memo":"Manually reviewed","bookmarked":true}}
{"id":5,"method":"export","params":{"projectId":"PROJECT_UUID","destination":"/reports/critical.csv","flagFilter":"critical","includeAnnotations":true}}
```

Single-row patches keep omitted fields. An empty flag clears the flag, `memo:
null` clears the memo, and `tags: []` clears tags. Bulk operations require a
preview token: preview the exact target and patch, copy `previewToken` into the
same request, then call `bulk_apply`. If annotations or membership change,
preview again; the CLI uses the same transaction checks as the application.

## Python without third-party packages

[`examples/automate.py`](../examples/automate.py) starts one persistent engine
process, imports an optional source and queries a case. It uses only the Python
standard library:

```sh
python examples/automate.py --cli target/release/trivium-cli --workspace /cases/demo --source /evidence/events.csv --search "needle"
```

The `Trivium.call(method, params)` helper can also be used for explicit flagging,
bulk changes and exports. Keep policy and automation in the client and data
semantics in the Rust engine. No embedded Python/plugin host is required for
this integration, and no third-party code is loaded into the desktop process.

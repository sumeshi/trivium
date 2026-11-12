<h1 align="center"><img src="assets/branding/banner.png" alt="Trivium" width="720"></h1>

A desktop application for reviewing and triaging large CSV, JSON, and JSONL logs.

![Trivium Screenshot](screenshot.jpeg)

## Introduction

Trivium is a desktop tool for reviewing CSV, JSON, and JSONL data that is too large or too structured for a spreadsheet. It provides a simple interface for filtering data, flagging important rows, and recording notes, tags, and bookmarks.

Its core purpose is to make manual data review and analysis faster and easier to resume. Opening a file never assigns a risk level or marks a row as reviewed; the user makes those decisions.

## Download

Pre-built versions for Windows are available for download from the project's **[Releases](https://github.com/sumeshi/Trivium/releases)** page. *(Note: This is a placeholder link.)*

## Who is this for?

This tool is primarily for anyone who needs to sift through large amounts of tabular data, such as:

- Security analysts reviewing logs or threat intelligence data.
- Data analysts performing preliminary data cleaning and exploration.
- Researchers categorizing and annotating datasets.

## Key Features

- **Efficient Import**: Opens CSV, JSON object/array, JSONL, and NDJSON files and stores the working copy as Parquet. CSV values keep their original text; JSON null, missing keys, and empty strings stay distinct.
- **Virtualized Scrolling**: The interface remains responsive with millions of rows.
- **Flag Rows**: Mark any row as `Safe`, `Suspicious`, or `Critical` with a single click.
- **Annotations**: Add memos, tags, and bookmarks independently. Clearing a flag does not clear the other annotations.
- **Filtering & Sorting**: Filter by flag, bookmark, text search, or structured conditions, and sort by any data column.
- **Column Management**: Hide columns or choose how many leading data columns stay pinned, independently from the search scope.
- **Selection & Bulk Annotation**: Select rows across pages by stable row id, annotate only the selection, the current result, or all rows, and undo the last bulk change.
- **Data Export**: Export all rows, the current result, or selected rows to CSV, with optional `trivium-*` annotation columns.
- **Condition Editor**: Build filters from nested AND/OR groups with per-column operators (`=`, `≠`, `>`, `<`, `≥`, `≤`, `contains`, `starts`, `ends`, `regex`, `in`), while the text search bar remains available.
- **Analysis**: Inspect per-column statistics and rare values, histograms, time gaps/sessions/bursts, duplicate or unique rows, and full row details. Time controls explain gaps, sessions, and bursts; panels calculate only while open and use the applied query. Back/forward navigation and per-project view state make it easy to move between filters.
- **Advanced Views**: Grouping, color rules, presets, cross-project search, diffs, and HTML reports remain available as auxiliary tools outside the basic manual-review flow.
- **Flexible Import & Export**: Choose a CSV delimiter, import UTF-16 via BOM detection, and load JSON/JSONL with nested keys flattened using JSON Pointers. The source file name, format, size, and SHA-256 are kept in project metadata. Exports preserve integer text and can include tags and bookmarks as independent columns.

## Search Syntax

Trivium provides a fast, flexible text search with boolean operators and column scoping.

- Basic
  - Case-insensitive substring match
  - Search runs across all data columns by default. Hiding a column changes display only; it does not change the search scope.

- Operators (no keywords; use symbols only)
  - AND: whitespace (implicit between adjacent terms)
  - OR: `|`
  - NOT: leading `-` before a term (outside quotes)
  - Precedence: `NOT` > `AND` > `OR` (no parentheses support)

- Quoted phrases
  - `"exact phrase"` matches the phrase literally
  - Inside quotes, `|` and `-` are treated as literal characters, not operators

- Column scoping
  - `column:term` limits the term to a specific column
  - `column:"two words"` for a quoted phrase in a specific column
  - Column names are case-insensitive: `EventID:4624` == `eventid:4624`

- Special characters
  - To search `|`, `-`, or `-keyword` literally, wrap in quotes (e.g., `"-keyword"`)
  - JSON-like fragments with colons should be quoted: `"hoge:fuga"` or scoped `data:"hoge:fuga"`

- Examples
  - `malware beacon` → `malware` AND `beacon`
  - `error|warn` → `error` OR `warn`
  - `beacon -test` → `beacon` AND NOT `test`
  - `eventid:4624 user:administrator` → scoped to `eventid` and `user`
  - `command:"powershell -enc"` → phrase in `command`

- Legacy IOC definitions
  - IOC definitions use the same syntax and semantics as the main search, but they are preserved as old settings and are not evaluated automatically on import, page load, or export.

Notes
- Regular expressions are not supported in the text search bar; `regex` remains available in structured conditions.
- Repeated page and search requests reuse prepared work; the first full-column search can still take time on very large files.

### Structured Conditions

The condition editor builds a filter tree that the engine evaluates with the same predicate as the text search. It does not replace the search bar: both remain available, and when both are set a row must match the text search and the condition tree (AND).

- Condition rows
  - Each row selects a column, an operator, and a value.
  - Operators: `=` (`Eq`), `≠` (`Ne`), `>` (`Gt`), `<` (`Lt`), `≥` (`Ge`), `≤` (`Le`), `contains`, `starts`, `ends`, `regex`, and `in`.
  - `in` accepts a comma-separated list of values, for example `4624,4625,4688`.
  - `Not` negates an individual row.

- Groups
  - Every group combines its children with AND or OR.
  - Groups can be nested; the root group is an AND group.
  - Rows and groups can be reordered with `Up`/`Down`, and groups can be collapsed.

- Typed values
  - Numeric columns use numeric input and datetime columns use `datetime-local` input.
  - Integer and nanosecond values are sent as text, so values beyond `2^53` are not rounded.

- Scope and notes
  - Conditions name their columns explicitly, so they can target columns that are currently hidden.
  - The text search bar does not support regular expressions; `regex` is available only as a structured condition operator.

## Getting Started

1.  **Import Data**: Click "Import CSV / JSON / JSONL File" in the sidebar and select a file.
2.  **Review & Flag**: Inspect rows in the table and use the flag buttons (`✓`, `?`, `!`) to record your judgment.
3.  **Annotate**: Add a memo, tags, or a bookmark. These fields are independent.
4.  **Narrow the View**: Use text search, flag/bookmark filters, column conditions, and sorting.
5.  **Select & Export**: Select rows across pages if needed, then export all rows, the current result, or the selection as CSV.

## Building from Source

For developers who wish to build or modify the application.

#### Prerequisites

- [Node.js](https://nodejs.org/) 24.22.1 (see `.node-version`; UI tests use built-in TypeScript stripping)
- [Rust](https://www.rust-lang.org/tools/install) 1.99.1 (installed automatically by rustup from `rust-toolchain.toml`)
- [Tauri 2 prerequisites](https://tauri.app/start/prerequisites/)

#### Development

To run the application in development mode with hot-reloading:

```bash
npm install
npm run tauri dev
```

*Note: `npm run dev` will run the Svelte frontend only, without the Rust backend.*

#### Production Build

To build the distributable application for your platform:

```bash
npm install
npm run tauri build
```

#### Development Scripts

- `scripts/gen_fixture.py` generates deterministic synthetic log fixtures for tests and benchmarks.

  ```bash
  python3 scripts/gen_fixture.py --rows 1000 --seed 42 --out fixtures/basic.csv
  ```

  It accepts `--rows N --seed N --out PATH --variant {ascii,utf16,wide,long}`. A fixed seed reproduces the same bytes; `utf16` writes a BOM-prefixed UTF-16LE file, `wide` adds twelve extra columns, and `long` adds many columns with long cells for the T27 benchmark.

- `scripts/check_rust_size.sh` enforces the 500-line limit on Rust source files under `src-tauri/src` and `crates/engine/src`. It exits non-zero when a file exceeds the limit and runs as a quality gate in CI.

  ```bash
  bash scripts/check_rust_size.sh
  ```

Quality gates used locally and in CI:

```bash
cargo test -p trivium-engine
cargo clippy -p trivium-engine --lib -- -D warnings
bash scripts/check_rust_size.sh
npm run lint
npm run lint:test
npm run test:ui
npm run build
```

Benchmark results are recorded in [docs/performance.md](docs/performance.md).
See [the architecture](docs/architecture.md) for read paths and work ownership,
and [automation](docs/automation.md) for the standalone JSONL CLI and Python example.

## Windows System Requirement: WebView2

On Windows, Trivium uses the Microsoft Edge WebView2 runtime to display the user interface.
If you are using the **portable executable** (`.exe`), you may need to install the WebView2 Runtime manually. You can download it from the [official Microsoft website](https://developer.microsoft.com/en-us/microsoft-edge/webview2/).

## Technology Stack

- **Backend**: Rust with the Tauri framework
- **Data Processing**: Polars for high-performance DataFrame operations (CSV/Parquet)
- **Frontend**: Svelte with TypeScript
- **UI**: Tailwind CSS

## Data Storage

Projects are stored in the standard application data directory for your operating system.

- `trivium/projects/<uuid>/data.parquet` — imported evidence, including exact original records.
- `trivium/projects/<uuid>/project.sqlite` — case metadata, annotations, settings and durable bulk undo.
- Older `projects.json`, `flags.db`, and JSON settings are migrated automatically and retained as backups. SQLite becomes authoritative after migration. An active legacy writer or damaged case produces an explicit error; see [storage and migration](docs/storage.md).

The native UI opens cached metadata without decoding evidence rows. Table pages read only the required Parquet row groups. Polars remains the query and analysis engine; projected streaming operations and bounded external sorting keep exports from collecting a whole output frame.

Rust-to-TypeScript wire types are generated in `src/lib/generated/engine.ts`. Run `npm run contracts:generate` after changing public DTOs, and `npm run contracts:check` before committing. Source integers and nanosecond timestamps remain strings on the wire.

---

*Trivium is provided under the MIT License. See [LICENSE](LICENSE) for details.*
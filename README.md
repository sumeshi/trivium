# Trivium

A desktop application for reviewing and triaging large CSV files.

![Trivium Screenshot](screenshot.jpeg)

## Introduction

Trivium is a desktop tool designed to help you work with CSV files that are too large to handle comfortably in standard spreadsheet programs. It provides a simple, efficient interface for filtering data, flagging important rows, and adding notes.

Its core purpose is to make the process of manual data review and analysis faster and more manageable.

## Download

Pre-built versions for Windows are available for download from the project's **[Releases](https://github.com/user/repo/releases)** page. *(Note: This is a placeholder link.)*

## Who is this for?

This tool is primarily for anyone who needs to sift through large amounts of tabular data, such as:

- Security analysts reviewing logs or threat intelligence data.
- Data analysts performing preliminary data cleaning and exploration.
- Researchers categorizing and annotating datasets.

## Key Features

- **Efficient CSV Handling**: Imports large CSV files quickly by converting them into the efficient Parquet format.
- **Virtualized Scrolling**: The interface remains fast and responsive, even with millions of rows.
- **Flag Rows**: Mark any row as `Safe`, `Suspicious`, or `Critical` with a single click.
- **Add Memos**: Annotate rows with detailed notes and observations.
- **IOC Rules**: Define "Indicators of Compromise" (or any custom) rules to automatically flag rows and tag memos based on content, streamlining initial analysis.
- **Filtering & Sorting**: Instantly filter the view by flag status, search for text across all columns, and sort data by any column.
- **Column Management**: Toggle the visibility of columns to focus on the data that matters.
- **Data Export**: Export your work—including all flags and memos—back to a CSV file for use in other tools.

## Search Syntax

Trivium provides a fast, flexible text search with boolean operators and column scoping.

- Basic
  - Case-insensitive substring match
  - Search runs across visible columns (toggling column visibility changes the search target)

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

- IOC rules
  - IOC queries use the same syntax and semantics as the main search

Notes
- Regular expressions are not supported.
- Searching very large datasets is cached; the first run may build masks, subsequent runs are faster.

## Getting Started

1.  **Import Data**: Launch the app and click "Import CSV" from the sidebar to create a new project.
2.  **Review & Flag**: Use the table view to inspect your data. Use the flag buttons (`✓`, `?`, `!`) on each row to categorize them.
3.  **Add Memos**: Click the memo icon to add detailed notes to any row.
4.  **Use Filters**: Use the search bar and flag filter at the top to narrow down the data.
5.  **Configure IOCs**: Open the "IOC Rules" manager to set up rules that automatically flag data for you.
6.  **Export**: When you're done, click the "Export Project" button to save your work as a new CSV file.

## Building from Source

For developers who wish to build or modify the application.

#### Prerequisites

- [Node.js](https://nodejs.org/) (v18 or later)
- [Rust](https://www.rust-lang.org/tools/install) toolchain
- [Tauri CLI setup](https://tauri.app/v1/guides/getting-started/prerequisites)

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

- `trivium/projects/<uuid>/data.parquet` - The imported data in Parquet format.
- `trivium/projects/<uuid>/flags.json` - Row flags and memos.
- `trivium/projects/<uuid>/iocs.json` - IOC rules for the project.
- `trivium/projects.json` - General metadata for all projects.

---

*Trivium is provided under the MIT License. See [LICENSE](LICENSE) for details.*
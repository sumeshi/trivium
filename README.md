# Trivium

**"For the trivial bits of log analysis."**

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat)](LICENSE)

## Introduction

**Trivium** is a desktop application for searching, analyzing, and investigating logs in CSV and JSON formats.

Designed with incident response in mind, it provides search, filtering, flags, annotations, timeline analysis, IOC management, and other tools for working through log data.

It also provides an automation interface that can be used for LLM-assisted investigations. Let's spend as little time as possible on the trivial stuff.

![Trivium Screenshot](screenshot.jpeg)

## Themes

Choose from a variety of color themes to suit your preferences.

<!-- Add the color theme showcase image here. -->

## Features

### Log Viewer

View CSV files and even nested JSON, JSONL, and NDJSON data in a table. Click a cell to inspect the full record without having to scroll across all its columns.

You can open multiple files in tabs, reorder and resize columns, hide columns you don't need, and pin important columns in place.

### Search & Filter

Search for keywords, limit searches to specific columns, or build more complex conditions using AND, OR, and NOT. The view history lets you go backward and forward through your investigation steps.

You can also save your filter and view settings as presets, making it easy to reproduce the same view across different files and compare results.

### Search Syntax

Text search is intended for quickly narrowing down the data. It is case-insensitive and does not interpret parentheses or regular expressions directly. For regular expressions, numeric or datetime comparisons, and more complex AND/OR conditions, use **Filter → Rules**.

| Example | Description |
| --- | --- |
| `powershell` | Find rows containing the specified text |
| `error timeout` | Find rows containing both terms (AND) |
| `error\|warn` | Find rows containing either term (OR) |
| `-test` | Exclude rows containing `test` (NOT) |
| `EventID:4624` | Search only the `EventID` column |
| `"powershell -enc"` | Search for an exact phrase |
| `@tag:review` | Find rows tagged with `review` |
| `@tag:*` | Find rows with any tag |

### Flag & Annotate

Assign one of three flags to rows as you investigate:

- **Safe** — Rows you have determined to be harmless
- **Suspicious** — Rows that may warrant further investigation
- **Critical** — Rows you have identified as significant or problematic

You can also add notes, tags, and bookmarks to individual rows. Select multiple rows to apply annotations in bulk, and undo the most recent bulk operation when needed.

### Timeline & Analysis

For logs with a datetime column, the timeline visualizes when events occurred, including the number of events and their breakdown into Safe, Suspicious, Critical, and unflagged rows.

Zoom in on periods of high activity or select a time range to narrow down the table. Column value counts, uncommon values, time gaps, and grouping tools also help you investigate the data in more detail.

### Save & Export

Your investigation is saved locally and automatically. Closing a tab does not delete its project; you can reopen it later to review your filters and annotations.

Export **all rows, the current results, or selected rows** to CSV. You can also include flags, notes, tags, and bookmarks in the exported data.

## Installation

Prebuilt binaries are available for **Windows (x64)** and **Linux (x64)**:

- **Windows:** Portable `.exe`
- **Linux:** `.deb` and `AppImage`

Download the latest version for your platform from [GitHub Releases](https://github.com/sumeshi/trivium/releases).

The Windows version requires [Microsoft Edge WebView2 Runtime](https://developer.microsoft.com/en-us/microsoft-edge/webview2/). Please install it beforehand if it is not already available.

## Feedback

Bug reports and feature requests are welcome in [GitHub Issues](https://github.com/sumeshi/trivium/issues).

## License

Trivium is released under the [MIT License](LICENSE).

## Third-party Licenses

Trivium includes third-party open-source software, which is distributed under its respective licenses.

Copyright notices and license terms for third-party components are provided in `THIRD_PARTY_LICENSES.txt`, included with each binary distribution.

## Acknowledgments

Trivium was inspired by Eric Zimmerman's [Timeline Explorer](https://ericzimmerman.github.io/). Many thanks to him for his excellent work.

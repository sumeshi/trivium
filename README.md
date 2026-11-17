# Trivium

**"For the trivial bits of log analysis."**

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat)](LICENSE)

![trivium-logo](assets/branding/banner.png)

## Introduction

**Trivium** is a desktop application for searching, analyzing, and investigating logs in CSV and JSON formats.

Designed with incident response in mind, it provides search, filtering, flags, annotations, timeline analysis, IOC management, and other tools for working through log data.

The separate `trivium-cli` executable provides an automation interface for scripts and LLM-assisted investigations. See the [Python example](examples/automate.py) to get started. Let's spend as little time as possible on the trivial stuff.

<img src="assets/screenshots/overview.png" alt="Trivium log viewer" width="720">

## Themes

Choose from a variety of color themes to suit your preferences.

Use **⋮ → Appearance → Edit colors** to enter hex colors, preview them, and save a custom theme. Theme JSON import and export are also available. Choose a local PNG, JPEG, or WebP wallpaper for the table body, including fixed columns; its size always follows the table with cover scaling.

<img src="assets/screenshots/themes-light-dark.png" alt="Light and Dark themes" width="720">

<img src="assets/screenshots/themes-toki-sakura-kohaku-yukari.png" alt="Toki, Sakura, Kohaku, and Yukari themes" width="720">

<img src="assets/screenshots/themes-sora-nagi-hisui-sumire.png" alt="Sora, Nagi, Hisui, and Sumire themes" width="720">

<img src="assets/screenshots/themes-yozora-shion-kasumi.png" alt="Yozora, Shion, and Kasumi themes" width="720">

## Features

### Log Viewer

View CSV, JSON, JSONL, and NDJSON logs in a table, including nested JSON records. Click a cell to inspect the full record without having to scroll across all its columns.

JSON imports support up to 4,096 flattened columns; files exceeding this limit are rejected.

You can open multiple files in tabs, reorder and resize columns, hide columns you don't need, and pin important columns in place.

Drop data files into the window, then review the import settings before opening them.

### Search & Filter

Search for keywords, limit searches to specific columns, or build more complex conditions using AND, OR, and NOT. Use the back and forward buttons to revisit previous filter rules.

Save filter rules, column layouts, and sort order as presets to reuse them across files with matching columns.

Use **Patterns ▾** beside the search box to select saved regular expressions or create your own. Text, selected patterns, and Rules are combined with AND. **All columns ▾** chooses columns for both text and patterns. Applied definitions are saved with the investigation; editing a pattern does not change an existing investigation silently.

Built-in patterns cover network addresses, CIDRs, URLs, email, hashes, paths, Windows identifiers, defanged IOCs, and encoded or structured values. Hash names describe hexadecimal candidates of the corresponding length; they do not identify the algorithm. Structured checks do not verify signatures, ownership, or reachability.

IPv4-like and IPv6-like match complete address, CIDR, or endpoint tokens. Use URL for complete URLs; IP-like patterns do not extract just the host from a URL containing a path. Network patterns accept defanged spellings where supported and preserve the original text in extracted results. Base64 finds encoding candidates; ordinary identifiers and hashes can also match its syntax.

<img src="assets/screenshots/filter-rules.png" alt="Filter rules matching EventID 4624" width="720">

### Search Syntax

Text search is intended for quickly narrowing down the data. It is case-insensitive and does not interpret parentheses or regular expressions directly. Choose **Patterns ▾** for regular expression snippets, or use **Filter → Rules** for grouped AND/OR and numeric or datetime comparisons.

| Example | Description |
| --- | --- |
| `powershell` | Find rows containing the specified text |
| `error timeout` | Find rows containing both terms (AND) |
| `error\|warn` | Find rows containing either term (OR) |
| `-test` | Exclude rows containing `test` (NOT) |
| `EventID:4624` | Find rows containing `4624` in the `EventID` column |
| `"powershell -enc"` | Search for an exact phrase |
| `-"powershell -enc"` | Exclude rows containing the phrase |
| `@tag:review` | Find rows tagged with `review` |
| `@tag:*` | Find rows with any tag |

### Extract

Open **Filter → Extract**, choose all rows or the current results and the source columns, then select a pattern. Trivium creates a temporary CSV and opens it in a normal table tab. It contains the extracted values, occurrence counts, and the number of source rows. Three occurrences in one row count as **3 occurrences / 1 row**. Hidden source columns can be included.

Search, sort, and export use the same controls as any other CSV. Export the result to keep it; temporary result tabs do not appear in Recent and are removed when closed. If an exact aggregation reaches its memory limit, narrow the source rows or columns and run it again.

Bundled patterns and themes are defined in `definitions/formats/` and `definitions/themes/`. Adding one valid JSON file registers it in the next build. Pattern JSON can also be imported from the pattern picker.

### Flag & Annotate

Assign one of three flags to rows as you investigate:

- **Safe** — Rows you have determined to be harmless
- **Suspicious** — Rows that may warrant further investigation
- **Critical** — Rows you have identified as significant or problematic

You can also add notes, tags, and bookmarks to individual rows. Use **Annotate → Bulk annotate** to apply flags, notes, or tags to selected rows, the current results, or all rows. You can undo the most recent bulk operation.

<img src="assets/screenshots/bulk-annotation.png" alt="Bulk annotation of flags and notes" width="720">

### Timeline & Analysis

For logs with a datetime column, the timeline visualizes when events occurred, including the number of events and their breakdown into Safe, Suspicious, Critical, and unflagged rows.

Zoom in on periods of high activity or select a time range to narrow down the table. Column value counts, uncommon values, time gaps, and grouping tools also help you investigate the data in more detail.

<img src="assets/screenshots/timeline-analysis.png" alt="Timeline analysis of log events" width="720">

### Save & Export

Trivium automatically saves each investigation as a local project, including your filters and annotations. Closing a tab keeps the project; reopen it from **Projects** to continue your investigation.

Export **all rows, the current results, or selected rows** to CSV. You can also include flags, notes, tags, and bookmarks in the exported data.

The Save dialog asks before replacing an existing file. Automation requests must set `allowOverwrite: true` to overwrite a file.

## Installation

Prebuilt binaries are available for **Windows (x64)** and **Linux (x64)**:

- **Windows:** Portable `.exe` in a ZIP archive
- **Linux:** `.deb` package or `AppImage` in a ZIP archive

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

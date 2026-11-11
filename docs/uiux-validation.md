# UI/UX investigation workspace validation

Starting revision: `develop` at `fbe3417` (the P0 navigation fix).

## Implemented scope

| Request | Behavior |
| --- | --- |
| Project tabs | A thin filename tab strip; closing a tab leaves its saved project and annotations intact. Only one project view and its bounded page cache stay mounted. |
| Tab state | Search, nested conditions, sort, visible columns/widths, logical/horizontal scroll, selected original IDs and condition history are restored. Stale responses cannot update another tab. |
| Header | `Trivium / filename`, shortened SHA with full tooltip/copy, and a compact menu. Reload reads the saved project, without reimporting its source. |
| Flags | All 16 selections of Safe / Suspicious / Critical / Unflagged share query, analysis, export and bulk targets. Empty selection means all rows. Old string filters remain accepted. |
| Columns | Searchable, bounded list with Select all / Deselect all. Visibility does not change the query. Writes stay ordered across tab remounts. |
| Table controls | Original-row Go / result First and Last, history icons, concise Flagged count. The original 32px rows, 11.5px table text, three flag buttons and Add memo remain. |
| Analysis tools | One nonmodal side drawer overlays the table. Inactive contents are unmounted; the visible table remains interactive. Top-value count bars, collapsed rare values, histograms and sorted time spans replace long default lists. |
| Timeline | Initial focus on a dense period, exact outside counts, Show all, zoom/pan/drag, and four flag counts per bucket. Current annotations and query filters are respected. |
| Themes | Light / Dark / Paper / Midnight use CSS variables and persist across restart. Semantic flag colors stay consistent. |

No cross-project timeline, import-time period restriction or automated risk
classification was added. The storage and annotation formats are unchanged.
Multiselect flags use the existing string field (`safe|critical|none`); legacy
`all`, `none` and `priority` remain compatible.

## Correctness and resource bounds

- Inactive tabs retain control/selection/history snapshots, not loaded rows or
  initial-page responses. View and column writes are ordered per project.
- Tab scroll restoration waits for the spacer/grid DOM update before assigning
  vertical or horizontal positions. The body owns horizontal scroll; the
  header follows it, without a second scrollbar that blocks WebKitGTK clicks.
  Keyboard focus reveals offscreen columns and the selected tab.
- Single-row annotation edits update ordinary cached rows in place. An
  annotation-dependent filter reloads its membership; a bulk write refreshes
  visible data even if its drawer was closed while the write completed.
- Initial timeline focus sorts a deterministic sample of at most 4,096 points,
  rather than the entire input. Exact bucket/outside counts use the already
  parsed timestamps. Wheel input is coalesced for 120ms; one timeline native
  aggregation runs at a time, including across drawer/tab remounts.
- Timestamps and range bounds retain nanoseconds, including RFC3339 `Z` and
  offsets. Display labels are shortened separately. Bucket ends are explicitly
  exclusive except for the final bucket, avoiding boundary double counting.
- Display/source column names in scoped text queries resolve before projection
  and cached row lookup. Explicit internal IDs keep their meaning; ambiguous
  labels return candidate IDs. Quoted literals, negation and OR scope survive.

## Automated checks

Validated on Debian 13 with Node 24.22.1 and Rust 1.99.1:

| Check | Result |
| --- | --- |
| Complete engine suite | 265 passed, 0 failed / ignored |
| UI unit suite | 70 passed |
| Svelte and test TypeScript checks | Passed; 0 errors / 0 Svelte warnings |
| Frontend and embedded-assets native builds | Passed |
| Engine Clippy, all targets, `-D warnings` | Passed |
| Rust 500-line guard and diff whitespace check | Passed |

## Native application acceptance

The final workspace run passed on the actual embedded-assets Tauri/WebKitGTK
application: `target/native-workspace-ui/c46a2eff8828/report.json`.
It exercised five native app sessions and 194 real IPC calls, with zero
JavaScript/native errors, superseded-query cancellations or transport retries.

| Scenario | Verified result |
| --- | --- |
| Flags and bulk/export | All 16 UI subsets agree with counts, CSV output and bulk preview; real tag apply/undo and a bulk write completing after drawer closure update the table. |
| 27-column table | Select all / Deselect all, search, preserved query, source-name scoped search, SHA copy, saved reload after moving the source, native header sort clicks, horizontal scrolling and keyboard access to column 27 pass. |
| Analysis | Opening/closing tools preserves table height; closed tools start zero heavy requests. Dense focus shows 510/512 events and Outside 2; Show all restores 512. Every bucket's four category counts add up to its total; each category totals 128 in this fixture. |
| Timeline interaction | Ten trusted wheel events coalesce into one histogram request. Actual zoom, pan and pointer drag produce exact nanosecond condition bounds. Changing Safe to Critical while the panel is open refreshes distribution even when Flagged is unchanged. |
| Tabs | CSV/JSON switching preserves search, nested AND/OR, descending sort, 26 visible columns, vertical/horizontal position and selected original IDs 242/243. A successful old-tab query is held and released after switching; it cannot overwrite the new tab. Closing/reopening keeps projects and annotations. |
| Edge controls | Last-row flag changes and Add memo are clickable with 27 columns; a 320px overflowing tab strip supports actual pointer and Arrow-key navigation and reveals its selected tab. |
| Themes | Light, Dark, Paper and Midnight each persist through an actual app close/relaunch; the saved theme file is checked. |

The default 1280×827 window with 27 columns has a 590px table viewport and
32px rows. Its horizontal scrollbar occupies a separate 14px footer, keeping
WebKitGTK's native hit region outside row buttons. Overfull tab strips use
compact scroll arrows instead of a native scrollbar over their buttons.
Final screenshots (including the default table, last row, restored tabs,
focused/full timeline and all four themes) are beside the report.

The P0 native regression was rerun against the same final UI binary:
`target/native-row-navigation/71a5616dd94c/report.json`. Both 5,199 and 2,000,000
rows passed original-ID Go, current-result First/Last, filtered descending
navigation, excluded/invalid-row notifications, fully visible targets,
contiguous rows without duplicates/blank areas, the 8,000,000px spacer cap,
annotations/export and stale-response handling. This run operates the actual
icon buttons through their accessible Go / First / Last names.

```sh
npm ci
npm run test:ui
npm run lint
npm run lint:test
npm run build
cargo test --locked -p trivium-engine
cargo clippy --locked -p trivium-engine --all-targets -- -D warnings
bash scripts/check_rust_size.sh
cargo build --locked -p trivium-desktop --features custom-protocol

# Requires GTK/WebKitGTK, Xvfb, tauri-driver and WebKitWebDriver.
xvfb-run -a python3 tests/native/workspace_ui.py \
  --binary target/debug/trivium-desktop
xvfb-run -a python3 tests/native/row_navigation.py \
  --binary target/debug/trivium-desktop --rows 5199 2000000
```

The native scripts use isolated temporary XDG/project profiles and real native
IPC. Actual WebDriver input operates search, tabs, filters, columns, annotations,
analysis controls and themes. Race checks delay delivery of real native
responses. Exports are checked through the actual export dialog and native CSV
writer; GTK file-picker interaction itself is not automated.

Windows GUI behavior has not been verified in this Linux environment. Debug
fixture timings are functional checks, not release throughput benchmarks.
Package installation and a new GitHub Actions run have not been verified.
The Debian workflow remains configured for `develop` pushes with three-day
artifact retention; its Clippy gate now includes test targets.

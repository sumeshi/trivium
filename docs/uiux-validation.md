# UI/UX investigation workspace validation

Starting UI revision: `develop` at `fbe3417` (the P0 navigation fix).
Architecture follow-up starts from `95ee777`.
The investigation usability follow-up starts from `35f2fa3` and identifies builds as version 0.2.0.

## Implemented scope

| Request | Behavior |
| --- | --- |
| Project tabs | A thin filename tab strip; closing a tab leaves its saved project and annotations intact. Only one project view and its bounded page cache stay mounted. |
| Tab state | Search, nested conditions, sort, visible columns/widths, logical/horizontal scroll, selected original IDs and condition history are restored. Stale responses cannot update another tab. |
| Header | `Trivium / filename`, shortened SHA with full tooltip/copy, and a compact menu. Reload reads the saved project, without reimporting its source. |
| Flags | All 16 selections of Safe / Suspicious / Critical / Unflagged share query, analysis, export and bulk targets. Empty selection means all rows. Old string filters remain accepted. |
| Columns | Searchable, bounded list with Select all / Deselect all; header actions hide or freeze through any source column. Hidden columns add no frozen width. Visibility and freezing do not change the query; settings survive reloads and tab changes. |
| Table controls | Original-row Go / result First and Last, history icons, concise Flagged count. The original 32px rows, 11.5px table text and three flag buttons remain; annotation actions use Note / Add note labels. |
| Analysis tools | One nonmodal side drawer overlays the table. Inactive contents are unmounted; the visible table remains interactive. Column stats, nested Groups, time analysis and timeline share the Analysis category. |
| Timeline | Initial focus on a dense period, exact outside counts, Show all, zoom/pan/drag, and four flag counts per bucket. Current annotations and query filters are respected. |
| Themes | Menu order: Trivium, Light, Dark, Toki, Sakura, Kohaku, Hisui, Nagi, Sora, Yukari, Sumire, Yozora, Shion, Kasumi. Light is the first-run default. Only current names and version 2 preference records are accepted; retired aliases and legacy theme formats are removed. |

No cross-project timeline, import-time period restriction or automated risk
classification was added. The architecture follow-up stores case metadata,
annotations and view settings transactionally in each case's `project.sqlite`.
Existing registry/annotation/settings files remain migration inputs and are
preserved; public annotation and saved-view field semantics remain compatible.
Multiselect flags use the existing string field (`safe|critical|none`); legacy
`all`, `none` and `priority` remain compatible.
Earlier theme-migration results below are historical evidence for their named
revisions. The current theme contract is documented in the latest follow-up;
removing legacy theme preferences does not change case or schema compatibility.

## Correctness and resource bounds

- Each mounted investigation has its own controller and Svelte context. State,
  bounded pages, query generations, original-ID navigation, conditions/history
  and view persistence have separate modules. There is no module-global active
  case store or document-wide sort event.
- Inactive tabs retain control/selection/history snapshots, not loaded rows or
  initial-page responses. Metadata-only case loading avoids transferring an
  unused initial page. View and column writes are ordered per project. Case
  loads also await pending annotation writes from disposed instances; writes
  are ordered within their case and another case remains independent.
- One controller disposal cancels owned read requests, drains pending page
  waiters and releases rows, metadata, selected IDs and DOM references. Read
  owners use a unique top-level IPC request ID for each operation, including
  requests cancelled before native admission. Responses also check their case
  and query generation. Closing an analysis tool cancels only its reads;
  annotation mutations and exports may finish normally.
- Go resolves the original one-based `#` to a result-relative position before
  scrolling. First/Last cancel a superseded Go request without cancelling
  independent page reads. Changes to live filters also cancel the old lookup.
- Annotation observers establish their saved revision baseline before activation.
  Reopening a previously edited case therefore cannot fabricate a bulk refresh
  that cancels its first page or discards a pending saved scroll position.
  The saved viewport remains authoritative through the DOM update.
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
| Complete engine suite | 350 successful executions including the migration subprocess; 0 failures |
| UI unit suite | 138 passed |
| Svelte and test TypeScript checks | Passed; 0 errors / 0 Svelte warnings |
| Frontend and embedded-assets native builds | Passed |
| Engine Clippy, all targets, `-D warnings` | Passed |
| Desktop Clippy with embedded assets, `-D warnings` | Passed on Linux |
| Rust 500-line guard and diff whitespace check | Passed |

The frontend suite includes actual controller tests for independent case
stores, delayed annotations after disposal, exact read cancellation, stale
queued pages, out-of-order success/failure, disposed queues, failed queries,
original-ID navigation with 5,199/2,000,000 rows, cancelled Go requests, stable
save snapshots, read-only/newer view protection, late restores and annotation
writes continuing across tab closure/reopening. Fake backend promises gate
concurrency deterministically; native acceptance below checks
actual Tauri transport and rendered behavior.

## Native application acceptance

The verified architecture workspace run passed on the actual embedded-assets
Tauri/WebKitGTK application:
`target/native-architecture-workspace-verified/20bea628f229/report.json`.
It exercised five native app sessions and 227 real IPC calls, with zero
JavaScript errors, native errors, superseded-query cancellation responses or
transport retries. It includes the final annotation-baseline/scroll guard and
the before-native-commit annotation reopening regression.

| Scenario | Verified result |
| --- | --- |
| Flags and bulk/export | All 16 UI subsets agree with counts, CSV output and bulk preview; real tag apply/undo and a bulk write completing after drawer closure update the table. |
| 27-column table | Select all / Deselect all, search, preserved query, source-name scoped search, SHA copy, saved reload after moving the source, native header sort clicks, horizontal scrolling and keyboard access to column 27 pass. |
| Analysis | Opening/closing tools preserves table height; closed tools start zero heavy requests. Dense focus shows 510/512 events and Outside 2; Show all restores 512. Every bucket's four category counts add up to its total; each category totals 128 in this fixture. |
| Timeline interaction | Ten trusted wheel events coalesce into one histogram request. Actual zoom, pan and pointer drag produce exact nanosecond condition bounds. Changing Safe to Critical while the panel is open refreshes distribution even when Flagged is unchanged. |
| Tabs | CSV/JSON switching preserves search, nested AND/OR, descending sort, 26 visible columns, vertical/horizontal position and selected original IDs 242/243. A successful old-tab query is held and released after switching; it cannot overwrite the new tab. Closing/reopening keeps projects and annotations. |
| Pending annotation reopening | An actual Safe edit is held before native dispatch/commit. Closing that tab still allows a different case to load. Reopening the original case waits before issuing its owned metadata-only load; after release its row is Safe and Flagged is 1. |
| Edge controls | Last-row flag changes and Add memo are clickable with 27 columns; a 320px overflowing tab strip supports actual pointer and Arrow-key navigation and reveals its selected tab. |
| Themes | Light, Dark, Paper and Midnight each persist through an actual app close/relaunch; the saved theme file is checked. |

The default 1280×827 window with 27 columns has a 590px table viewport and
32px rows. Its horizontal scrollbar occupies a separate 14px footer, keeping
WebKitGTK's native hit region outside row buttons. Overfull tab strips use
compact scroll arrows instead of a native scrollbar over their buttons.
Final screenshots (including the default table, last row, restored tabs,
focused/full timeline and all four themes) are beside the report.

The architecture P0 native regression passed:
`target/native-architecture-navigation/a6c056ce42ce/report.json`. Both 5,199 and
2,000,000 rows passed original-ID Go, current-result First/Last, filtered descending
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
Package installation has not been verified. The Debian workflow builds on
`develop` pushes with three-day artifact retention; its Clippy gate includes
test targets. Current CI results are recorded below.


## Investigation usability follow-up (0.2.0)

The actual embedded Tauri/WebKit app passed all eight stages in
`target/native-feedback-workspace/fee2c14a7ca9/report.json`: seven native sessions,
all six themes restored after process restart, and no unexpected JavaScript or
native errors. One exact invalid-timestamp query was deliberately rejected and
verified; this negative case is distinguished from unexpected failures.

- Filename and SHA occupy one 38px header line; recent cases expose the full SHA
  in a tooltip. Workspace options shows version and source commit.
- Search syntax and column scope are explicit, independent of visible columns.
  Three tool categories mount only the active subpanel. The footer describes
  applied conditions and clears filters in one operation while retaining sort,
  column visibility, annotations and saved evidence.
- Column menus support search scope and an initially inactive blank rule.
  Existing OR expressions retain their meaning. Header tab navigation still
  reveals all 27 columns; Down or Shift+F10 opens column actions.
- The real Rust parser validates invalid dates, nanoseconds and offsets. Equal
  instants with different offsets produce identical decimal nanoseconds. Opening
  the UTC helper preserves existing text; only explicit insertion changes it.
- The applied-filter footer remains visible and clickable while Rules is open. A noninteractive notification cannot intercept its
  clear action.

The fixed-column defect was reproduced in the old native app:
`target/native-sticky-before/7a794d0781f2/`: all five pinned cells had transparent
backgrounds, allowing moving text to paint through. The fixed native app in
`target/native-sticky-after/7de8b93e5227/` painted each pinned cell opaquely.
The full feedback regression uses trusted horizontal wheel input at 700px and
checks opacity plus hit testing in Trivium, Light and Mauve for normal,
alternating, custom-coloured, hovered and loading rows. Screenshots and JSON
paint evidence accompany the full report. Row density and text sizes remain
unchanged.

Original-row navigation also passed with the final UI on 5,199 and 2,000,000
rows: `target/native-feedback-navigation/f49c2e687ac9/report.json`. First,
middle, last, filtered descending results, excluded-row notices, annotations,
export and delayed lookup responses remain covered.

### Published CI evidence

The `cfc0cc5` source revision passed the complete
[Debian build](https://github.com/sumeshi/trivium/actions/runs/43148962659),
including quality gates, the GUI `.deb` package and optional automation CLI.
Use its **trivium-deb** artifact for the graphical application. Its Linux
[native navigation job](https://github.com/sumeshi/trivium/actions/runs/43148962620/job/130953355499)
also passed against the embedded Tauri app with 5,199 rows.

The [Windows native job](https://github.com/sumeshi/trivium/actions/runs/43148962620/job/130953355763)
still fails before WebDriver attachment. Fixed startup markers confirm
`state-ready`, a real app window and six owned WebView2 child processes, with
no logged panic or HRESULT. Both localhost debugging endpoints remained
unreachable during the 45-second wait. This establishes successful application
initialization, but does **not** establish Windows GUI operation or identify
why the debugging endpoint is unavailable. The public startup diagnostic is
bounded; the owned application log and report remain in the native artifact.
Local startup/diagnostic contract tests (14) and namespace guard tests (3) pass.

The GTK native file-picker dialog and package installation remain outside
automated coverage. See [native CI](native-ci.md) and
[artifact identification](artifacts.md).

## Pastel themes and nested grouping follow-up

Feature source `5c5ca56` passed the full real Tauri/WebKitGTK workspace run:
`target/native-refinement-workspace-final/e16e61186f69/report.json`.
All nine stages passed across 19 actual app sessions and 356 IPC calls, with
zero JavaScript errors or transport retries. The one deliberate invalid-date
query remains the only expected native error.

- The format list uses slashes, CSV settings appear only for CSV, and the
  source notice says read-only. Real tab-delimited CSV and JSON imports retain
  their original bytes and SHA. Only the OS picker selected-path response is
  substituted; project creation and data processing use real native IPC.
- Recent keeps its shortened hash without a visible SHA prefix. The current
  filename exposes the original full path, with a copy action in its menu.
- Empty and whitespace-only search-column edits issue zero row, lookup or
  heavy-analysis reads, preserving selected IDs and both scroll positions.
  The next text query uses the selected columns; changing a live query still
  refreshes its results.
- Bookmark rows have an opaque gold tint and marker, including custom-coloured
  and frozen cells. Light, Dark and Yukari paint/hit checks pass after actual
  horizontal body-wheel input. Empty memo text is quieter than written text.
  Real checked, mixed and keyboard-focused checkboxes are verified.
- The header menu opens Analysis / Groups. Three-level nesting, counts,
  expand/collapse, reordering, removal and replacement pass. Group drill-down
  retains an existing OR tree, complete parent path, whitespace, empty strings,
  null/missing groups and integer text beyond JavaScript's safe integer range.
  Grouping rejects results exceeding 8 levels, 10,000 nodes or 32 MiB of labels;
  no partial counts are silently returned. At most 100 expanded group rows are
  rendered per page; closed panels start no grouping jobs.
- All 16 offered themes survive actual process restarts. Old plain localStorage
  and native-file Light preferences reopen as Yukari; newly saved Light stays
  the neutral palette. The ten pastel names are provisional: Yukari, Sakura,
  Sora, Kohaku, Hisui, Fuji, Wakaba, Momiji, Toki and Nagi. All palettes pass
  foreground, muted-text, focus-boundary and semantic-flag contrast checks.

The same binary passed original-row navigation with both 5,199 and 2,000,000
rows: `target/native-refinement-navigation/9828ac1274a9/report.json`.
All 349 engine executions, 116 UI tests, strict engine Clippy, generated IPC
contracts, Svelte/TypeScript and the embedded-assets build pass. Windows GUI
automation retains the previously documented debugging-endpoint limitation.

## Column controls, readable details and supplied branding

The final embedded-assets app at `ac59025` passed all 11 stages in
`target/native-polish-branded-final/ddd26d9b55aa/report.json`: 19 real native
sessions, 463 IPC calls, zero JavaScript errors and zero transport retries.
One deliberately invalid datetime query is still the only expected native
error. The complete 350-execution engine suite, 138 UI tests, strict engine
Clippy, generated contracts, Svelte/TypeScript and native build pass.

- Header menus hide a column or freeze the visible source prefix through any
  chosen column. Hidden predecessors and a hidden boundary add no width.
  View reloads, tab switches and old presets preserve the boundary; old view
  files retain first-visible-column freezing. Unfreeze is available even when
  every data column is hidden. These operations start no row queries.
- Oversized frozen prefixes pan beneath the opaque, fixed annotation columns.
  A native 4 × 450px fixture verifies trusted wheel and keyboard access to every
  frozen header and the next scrolling column. Ordinary fitting prefixes stay
  fixed. Newly frozen headers/cells do not animate from transparency.
- Full-row details have bordered fields and per-field Copy / Filter in / Filter
  out. Exact long text, empty strings and `18446744073709551615` pass native
  copy/filter checks, including Enter on Copy. Null/missing fields are distinct.
  The old Around UI starts no `surrounding_rows` calls; this removes the full
  datetime scan/sort previously performed on every detail open. Only focused
  or truncated/missing DTOs need full-cell IO, with at most two reads at once.
- Timeline count bars retain their original proportions. A separate presence
  marker makes one flagged event visible beside a 200-event unflagged peak.
  Intermediate UTC ticks, exact visible/selected periods and all four bucket
  counts remain readable without a hover tooltip. Inspection and keyboard
  movement issue zero additional histogram reads; wheel/pan/drag still pass.
- Auxiliary category headings are noninteractive `h3` elements in a separate
  band above the tabs. Bookmark backgrounds use a subtle 5% gold tint and a
  narrow marker, while all pinned layers remain opaque.
- White-led retained pastels and the four additional dark themes survive real
  restarts. Retired Fuji/Wakaba/Momiji/Paper settings resolve to current themes.
  The startup HTML uses the same versioned theme parser as the application.
- The supplied square icon and README banner are stored unchanged in
  `assets/branding/`. Tauri's official converter produces the 512px application
  PNG and multi-resolution ICO; the browser favicon uses that ICO. Original
  image hashes match the provided ZIP, and repeat conversion is byte-identical.

Original-row navigation also passed with 5,199 and 2,000,000 rows on the feature
build: `target/native-polish-navigation-verified/7ee6c6b9ea82/report.json`.
The native wheel helper uses a fresh WebDriver wheel source per impulse:
reusing a source made WebKitWebDriver deliver the difference between successive
impulses. Trusted DOM event probes diagnose that driver behaviour; no script
assigns scroll positions to satisfy gesture checks.

The native import picker selected-path response is the only substituted import
interaction. Actual OS import/export pickers and package installation remain
unverified; Windows automation has the previously recorded connection blocker.

## Compact controls, complete row scrolling and stable row paint

The next follow-up keeps the 32px table rows and existing table text sizes.
The flag, column, history and original-row navigation controls use compact
icons with accessible names and tooltips. Redundant navigation captions and
duplicate totals were removed; current result counts remain above the table.
The visible annotation label is now **Note** / **Add note**. Stored `memo`
fields, annotation APIs, saved cases and export data remain compatible.

The detail inspector now uses a compact three-column field layout: name,
unmodified value, and icon actions for Copy / Filter in. Short fields occupy
about 30px rather than a separate large card; long values wrap without truncating
their underlying text. The field region has an explicit flex scroll boundary
with `min-height: 0`, so it can shrink within the modal rather than pushing
later fields outside an unscrollable box. The region exposes wheel and
Home, PageUp/PageDown and End controls. The dialog traps Tab focus and restores
the originating cell on close. Per-field copy still uses exact complete values, and creating
a filter does not substitute a displayed preview.

The defect was reproduced before the fix on the actual embedded app:
`target/native-detail-before/dd3d550fbcd0/report.json`. With 67 fields,
64 short values and a 10,044-character long value, the apparent scroll region
reported both `scrollHeight` and `clientHeight` as 538px while the final field
was below 8,100px. A trusted wheel impulse left `scrollTop` at zero. Short
fields were about 80.6px high. This baseline records the inaccessible fields,
rather than inferring the bug from a mocked layout.

Themes are consolidated to 14 offered choices in this order: Trivium, Light,
Dark, Toki, Sakura, Kohaku, Hisui, Nagi, Sora, Yukari, Sumire, Yozora, Shion,
Kasumi. Trivium uses near-black navy and blue-black panels; Yozora and Shion
use lighter night-blue and purple surfaces. Shion retains the former Tsukimi
palette. Sumire retains the silver-grey surfaces and near-black accents of the
former Gin palette. Kasumi uses warm grey with a blue-green accent.

Theme preference compatibility was explicitly removed at the user's request.
Selection accepts only current names; persistence accepts a version 2 record
`{ "version": 2, "theme": "<current name>" }`, either as an object or its JSON
string encoding. Plain theme-name preferences, unversioned/version 1 records
and retired names such as Gin, Tsukimi or Tasogare are rejected. Invalid or
older preferences fall back to Light rather than being migrated or aliased.
This change concerns theme settings only; existing cases, schemas, annotation
data and saved-view semantics remain compatible. Thirteen theme tests cover
the current order, preference round trips, rejected formats and palette contrast.
Light and Dark use clearer selected-flag borders and bookmark emphasis;
the retained pastels keep white main surfaces and coloured accents. The
Safe / Suspicious / Critical meanings remain green / amber / red.

### Controlled scrolling comparison

The old zebra selector combined `.data-row:nth-child(2n)` with `.alt-row`.
When the virtual window removed its first DOM row, child indexes changed for
all retained rows. Their original result positions did not change, so the
extra selector repainted normal rows as alternate rows on every other window
shift. The fix removes only the DOM-child selector; `.alt-row` continues to
follow the actual result position. No row IDs, query, paging, frozen-column
geometry or cache limits changed.

`target/scroll-profile-paired/summary.json` records a controlled A/B/A comparison
within one Tauri/WebKitGTK process. Each phase ran twice using the same warmed
5,199-row, 27-column fixture at 1280 × 827. Each run advanced the native table
viewport through 160 animation-frame-driven 32px scroll steps. This measures
real rendering and native scroll events; it is not a trusted-wheel input test.
Only the zebra selector was changed through the page's CSSOM between phases.

| Phase | Median frame interval, two runs | 95th percentile, two runs | Frames above 50ms |
| --- | --- | --- | --- |
| Original selector | 68 / 69ms | 77 / 79ms | 144 / 144 |
| Result-position selector only | 33 / 33ms | 38 / 39ms | 2 / 0 |
| Original selector restored | 68 / 67ms | 84 / 77ms | 145 / 145 |

All six runs recorded 162 frames, 160 actual scroll events, 20,056 DOM mutations,
zero row-query IPCs, zero explicit bounding-rectangle reads and no JavaScript
errors. Returning to the original selector restored the slowdown in the same
process, controlling for glyph caches and startup effects. These are debug
WebKitGTK/Xvfb measurements, not a cross-platform frame-rate guarantee. The
Long Tasks API is unavailable in this WebKit build, so no long-task result is
claimed. The separate native regression moves the virtual window from original
row 1 to row 2 and verifies that retained rows 19 and 20 keep their distinct,
unchanged backgrounds and opaque fixed cells.

### Current verification

The UI unit suite passes 141 tests. Actual original-row navigation passes for
5,199 and 2,000,000 rows in
`target/native-density-navigation/60ffa8a22ccf/report.json`, including filtered
descending order, excluded-row notifications, first/middle/last geometry,
annotations, export and delayed responses.

Source `8fd58f6` passed the final 13-stage Linux native workspace run:
`target/native-density-workspace-verified/3d3b521f22b9/report.json`. All 15 actual
app sessions and 485 IPC calls completed with zero JavaScript errors, unexpected
native errors, superseded-query cancellations or transport retries. The single
deliberate invalid-timestamp rejection is recorded as an expected negative case.
All 14 themes applied and survived actual app restarts in the listed menu order.
The 14 native Python tests, build-identity check, Svelte and test TypeScript
checks also pass.

The 67-field inspector passes at both 827px and 620px window heights. Its 64
short test fields have a median height of 30px. Trusted wheel input and keyboard
End each expose the final field; real copy preserves all 10,044 characters, and
clicking the final field's Filter in applies its complete exact value. These
checks include real pointer hit testing and successful actions, not only changed
DOM scroll offsets. No surrounding-timestamp scan is started.

A separate controlled native comparison isolated GTK's overlay-scrollbar hit
region: `target/native-webkit-gutter-confirmation/63b48a3fd342/report.json`.
The last Filter in button's centre was 15px from the scroll region's right edge
and hit the scroll container instead of the button. Reserving a 16px end gutter
moved that centre to 31px and restored the actual button hit after both wheel
and keyboard End. The fix leaves field contents and values unchanged.

Right-clicking a partly visible table cell also exposed a focus/scroll race:
default focus could pan its column, and the queued scroll dismissed the newly
opened context menu. Suppressing default focus on right-button mouse-down only
keeps its original horizontal position; normal clicks and keyboard column
reveal remain available. The final native run verifies opening that menu and
applying its exact full-value Filter out action.

The tested WebKitWebDriver build released a right-button action as a left click.
Linux context-menu checks therefore use real XTest OS pointer input through
Python's standard `ctypes`; CI installs `libxtst6`. No DOM context-menu event is
substituted. The Windows input path has not been exercised. Native OS import
and export pickers, package installation and Windows GUI operation retain the
previously documented verification limits.

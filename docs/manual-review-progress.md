# Manual review rework — progress

Plan: `GOAL.md` (2026-11-11). One card per commit; verification recorded per card.
Historical wording in TASKS.md (e.g. "no tests", "Tauri 1") is not evidence for the
current tree.

## T00 — current state and minimal fixtures

Recorded: 2026-11-11.

- Branch: `develop`
- HEAD: `3739575` (`fix: D2 use bare command identifiers in generate_handler`)
- Working tree: clean except the untracked `GOAL.md` plan itself.
- Investigated SHA in GOAL.md: `3739575c…` — matches HEAD; no path re-mapping needed.

### Baseline gates (run on this HEAD)

| Gate | Command | Result |
|---|---|---|
| Engine tests | `cargo test -p trivium-engine` | 23 suites, 0 failures |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |

Native (Tauri) build is not verifiable in this environment; CI (`debian.yml`)
compiles `src-tauri` on ubuntu-24.04.

### Fixtures for later cards

Kept small and committed under `crates/engine/tests/fixtures/manual/`
(`*.csv` is git-ignored at the repo root, so test fixtures live here).

- `t00_source.csv` — 3 rows with header `flag,memo,__rowid,trivium-memo,zip,comma,quote,multiline,bigint,spaced`:
  - input-derived reserved-looking columns: `flag`, `memo`, `__rowid`, `trivium-memo`
  - leading zeros (`00123`, `00000`), comma-in-cell, doubled quotes,
    embedded newline (quoted), long integers `9007199254740993` (2^53+1),
    `9223372036854775807` (i64::MAX), `18446744073709551615` (u64::MAX),
    padded whitespace (`"  padded  "`), empty cells.
- `t00_source.jsonl` — 3 records covering: i64::MAX, u64::MAX, -i64::MAX-1,
  2^53+1, `null`, empty string, array value, literal `"a.b"` key vs nested
  `{"a": {"b": …}}`, empty object `{}`, and a key that appears only in the
  second record (`only_later`).
- `t00_source.json` — same shape as a single top-level object array (3 records).

Performance fixtures stay out of the repo: `scripts/gen_fixture.py` (seed 42)
regenerates 100k / 400k / 2M rows into git-ignored `fixtures/`.

### Known expectation notes (used by later cards)

- `9007199254740993`, `9223372036854775807`, `18446744073709551615` must
  round-trip through import → display/IPC → export unchanged.
- `a.b` (literal key) and `a` → `b` (nested) must become distinct columns.
- `null`, missing key, and empty string are three different values in JSON.
- The fixture's `flag`/`memo`/`__rowid`/`trivium-memo` columns are input data,
  not annotation metadata.

App behaviour was not changed by this card.

## T01 — CSV export keeps input `flag`/`memo` columns

Recorded: 2026-11-11.

- `crates/engine/src/export/csv.rs`: removed the unconditional `flag` and
  `memo` drops. Only the internal `__rowid` column is removed. Input columns
  named `flag`/`memo` are ordinary data; annotation metadata comes from
  `flags.json` and is exported as the `trivium-*` columns.
- `crates/engine/tests/export_options_test.rs`: added
  `input_flag_and_memo_columns_survive_export_with_annotations`. It imports a
  `flag,memo,msg` fixture, annotates row 0 (`safe`, memo, tag), exports without
  a column list, and asserts the header contains both `flag,memo,msg` and
  `trivium-*`, with the original values and `trivium-memo` intact.

### Verification

| Gate | Command | Result |
|---|---|---|
| Targeted test | `cargo test -p trivium-engine --test export_options_test` | 5 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |

### Unverified / deferred

- GUI export was not exercised; only the engine test above.
- An input column named `__rowid` is still overwritten at import time, so its
  original values are lost before export. Import-side handling is out of scope
  here and remains for T08b.

Next card: T02.

## T02 — single and bulk annotation updates are partial updates

Recorded: 2026-11-11.

- `crates/engine/src/annotation.rs` (new): `AnnotationPatch` and the pure
  `apply_annotation_patch(existing, patch)` function. A missing field keeps the
  stored value; `Some("")` (flag), `Some(None)`/blank (memo), `Some(vec![])`
  (tags), and `Some(false)` (bookmark) clear that field. The entry is deleted
  only when the flag is blank, the memo is blank/absent, tags are empty, and
  the bookmark is false. `deserialize_double_option` separates a missing memo
  key (`None`) from an explicit `null` (`Some(None)`).
- `crates/engine/src/ops/flags.rs`: `UpdateFlagPayload.flag` is now
  `Option<String>` and `memo` is `Option<Option<String>>`; the payload builds
  an `AnnotationPatch` and `update_flag` persists/deletes through the shared
  function. The returned `ProjectRow` is derived from the stored entry (its
  shape is unchanged; T13 narrows it).
- `crates/engine/src/ops/bulk.rs`: `BulkAnnotatePayload` carries the same patch
  fields (`flag`, `memo`, optional `tags`, optional `bookmarked`). The manual
  tags/bookmark carry-over was removed; omitted fields are kept by the patch.
- `src/lib/backend.ts`: TS `AnnotationPatch` with optional fields;
  `UpdateFlagArgs` and `BulkAnnotateArgs` extend it. `annotationFields` omits
  undefined keys from the IPC payload so Rust sees "missing", not null.
- UI: the flag buttons send `{ flag }` only; the memo dialog sends
  `{ memo, tags, bookmarked }` (no stale flag); bulk controls still send the
  selected flag and an explicit memo clear when the memo box is empty.

### Verification

| Gate | Command | Result |
|---|---|---|
| New regression suite | `cargo test -p trivium-engine --test annotation_patch_test` | 5 passed, 0 failed |
| Patch unit tests | `cargo test -p trivium-engine --lib annotation::` | 6 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |

Regression coverage: single flag clear keeps memo/tags/bookmark; memo save
keeps flag/tags/bookmark and leaves other rows untouched; omitted vs explicit
clear; bulk flag/memo clear keeps tags/bookmarks and tag-only entries; payload
serde distinguishes missing, null, and values (`annotation_patch_test.rs`).

### Unverified / deferred

- GUI behaviour was not exercised; only engine tests and the TypeScript
  checker/build cover the UI call sites.
- `cargo check -p trivium-desktop` could not run here because the native
  GTK/GDK development libraries (`gdk-3.0.pc`) are missing. `src-tauri` command
  signatures are unchanged; CI's Linux job remains the native compile check.
- Return-value shrinking and avoiding the full Parquet read on a single flag
  update remain for T13.

Next card: T03.

## T03 — QuerySpec search scope and count meanings

Recorded: 2026-11-11.

- `crates/engine/src/ops/query/mod.rs`: `QueryRowsPayload` now carries
  `search_columns` (`searchColumns` in the IPC JSON) instead of `columns`.
  `None` or an empty list means every data column, matching the old
  `prepare_search(None)` behaviour. Unknown search columns still follow
  `prepare_search`: they are ignored, not treated as a new error.
- `QueryRowsResponse` now returns four distinct counts:
  `total_rows` (whole project), `total_filtered_rows` (current result),
  `annotated_total` (annotated rows in the whole project, including IOC-derived
  final flags), and `annotated_filtered` (annotated rows in the result).
  `total_flagged` was removed. `page::filtered_indices` still computes the
  result-internal count; its local variable is now `annotated_filtered`.
- `crates/engine/src/ops/cross.rs` maps its existing `columns` argument to the
  renamed `search_columns` field.
- `src/lib/types.ts`: added the shared `QuerySpec` type and moved
  `FlagFilterValue` there. `src/lib/backend.ts`: `QueryProjectRowsArgs` extends
  `QuerySpec` plus `projectId`/`offset`/`limit`, and sends `searchColumns`.
  The response type follows the four Rust count fields.
- `src/lib/components/project_view/state.ts`: added `searchColumns` (initial
  `[]` = all columns) and `filteredFlaggedCount`; `flaggedCount` remains the
  whole-project annotation count. `AppliedFilters.columns` became
  `searchColumns`. Flag toggles update both counters.
- `ProjectView.svelte`: page requests send `searchColumns` from the new store,
  not visible columns. Hiding a column now only updates visibility and widths;
  it no longer re-runs the query. Page responses set `flaggedCount` from
  `annotated_total` and `filteredFlaggedCount` from `annotated_filtered`, so
  `emitSummary` keeps reporting the whole-project count.
- `FilterControls.svelte`: small scope label under the search input shows
  `All columns` when the scope is empty, otherwise the scoped column names.
  There is no control to choose search columns yet. `DataTable.svelte` shows
  `flagged in results` from `filteredFlaggedCount`.

### Verification

| Gate | Command | Result |
|---|---|---|
| New regression suite | `cargo test -p trivium-engine --test query_counts_test` | 4 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |

Regression coverage (`query_counts_test.rs`): 10 rows with 3 annotated and a
search that matches 1 row returns `total_rows=10`,
`total_filtered_rows=1`, `annotated_total=3`, `annotated_filtered=1`. Default
scope, an explicit all-column list, and an empty list return the same result;
restricting the scope to another column removes the match. The camel-case
`searchColumns` IPC field parses.

### Unverified / deferred

- GUI behaviour was not exercised: the scope label, column hiding, and the
  result-count display were checked only by `npm run lint` and `npm run build`.
- A search-column selection control is not part of this card; the store stays
  at `[]` (all columns) unless code sets it.
- Output columns, output/query row agreement, and request-generation handling
  remain for T04 onward.

Next card: T04.

## T04 — page, export, and bulk resolve the same target rows

Recorded: 2026-11-11.

- `crates/engine/src/ops/query/target.rs` (new): `QuerySpec`,
  `ResolvedTarget`, `data_columns`, and `resolve_target`. `resolve_target`
  runs `prepare_search` (`searchColumns` scope and `condition`), then
  `ordered_row_indices`, then `filtered_indices` (flag filter, bookmark-only,
  search mask), so the page query, filtered export, and bulk-filtered target
  share one implementation and one order. It also returns
  `annotated_filtered` and `annotated_total`.
- `crates/engine/src/ops/query/mod.rs`: `QueryRowsPayload` is now
  `projectId` + flattened `QuerySpec` + `offset`/`limit`. The IPC keys are
  unchanged (`search`, `searchColumns`, `flagFilter`, `bookmarkedOnly`,
  `sortKey`, `sortDirection`, `condition`). `query_project_rows` uses
  `resolve_target`; the response meaning is unchanged.
- `crates/engine/src/export/csv.rs`: `ExportProjectPayload` is
  `projectId` + `destination` + `scope` + flattened `QuerySpec` + output
  `columns`. Search/condition/sort keys are no longer export-only fields.
  Row selection and order are resolved on the full frame before output
  columns are restricted, so `columns` no longer acts as the search scope.
  `scope=filtered` applies search, condition, flag filter, bookmark-only, and
  sort; `scope=all` (default) exports every row in the requested sort order.
  The `trivium-*` columns stay first, and input `flag`/`memo` columns are
  still preserved (T01).
- `crates/engine/src/ops/bulk.rs`: `BulkAnnotatePayload` is `projectId` +
  `target` (`selection`/`filtered`/`all`) + `rowIndices` + flattened
  `QuerySpec` + flattened `AnnotationPatch` + `previewToken`. The old `filter`
  target is now `filtered`. `bulk_preview` returns `count` and
  `previewToken = sha256(spec JSON | row ids | annotated_filtered)`.
  `bulk_update` recomputes that token from the current frame, flags, and IOC
  outcomes and rejects a missing or different token with
  `Target changed; preview again.` Selection targets go through the same
  check. The patch application path is still `apply_annotation_patch`.
- `src/lib/backend.ts` / `src/lib/types.ts`: export and bulk args carry
  `searchColumns`, `flagFilter`, `bookmarkedOnly`, `sortKey`,
  `sortDirection`, `condition`, and `previewToken`; `BulkCount` returns
  `previewToken`. `BulkControls.svelte` sends the applied query state and
  passes the preview token to update. No new output-scope or row-selection UI
  was added (T23/T22).

### Verification

| Gate | Command | Result |
|---|---|---|
| New regression suite | `cargo test -p trivium-engine --test target_parity_test` | 4 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |

Regression coverage (`target_parity_test.rs`, 20 rows): a search term in a
column excluded from output, a condition on another column, a flag filter,
bookmark-only, and descending sort are set together. The page query returns
row ids `[6, 0]`; filtered export returns the same rows in that order both
when `id` is included and when only `msg` is exported. Bulk preview reports
the same count, update without the token or with a stale token fails, and the
matching token updates only rows 0 and 6. Clearing a flag inside a target
invalidates the token even when the row set is unchanged. `scope=all` with a
sort key exports all 20 rows from `r19` to `r00` regardless of the search
text.

### Unverified / deferred

- GUI bulk preview/apply and export were not exercised; only engine tests,
  svelte-check, and the production build cover the call sites.
- The preview token binds the spec, the original row-id sequence, and the
  annotated count in the target. It does not detect an external change to
  `data.parquet` that leaves the same row ids, or a memo-only edit that does
  not change whether a row counts as annotated. Input-change detection is
  T12; request generations and cache limits are T05/T19.
- Output scope selection, selected-row export, and the empty-selection
  behaviour remain for T23/T22. Undo is unchanged (T14).

Next card: T05.

## T05 — stale project and search responses are discarded

Recorded: 2026-11-11.

- `src/lib/stores/projects.ts`: added a monotonic `detailGeneration`.
  `loadProjectDetail`, `selectProject`, and `refreshSelected` start a new
  generation before requesting. A detail response (success or failure) only
  updates `projectDetail`, `selectedProjectId`, `isLoadingDetail`, or
  `projectCache` when its generation is current and its project id still equals
  `selectedProjectId`. A stale failure is ignored instead of clearing the
  loading state of a newer request.
- `src/lib/components/project_view/state.ts`: added `viewGeneration`,
  `invalidateViewRequests()`, and `pendingPageKey()`. `pendingPages` changed
  from `Set<number>` to `Set<string>` keyed by
  `viewGeneration:filterRequestId:page`, so a stale request can only remove its
  own entry. `DataTable.svelte` only needed the type change.
- `ProjectView.svelte`: `requestPage` captures `projectId`, `viewGeneration`,
  `filterRequestId`, and the page. The success path applies rows, counts, and
  loaded pages only when all three generations/ids still match. Pending cleanup
  runs in `finally` for the captured key. Applying a project detail bumps the
  view generation (and `filterRequestId` is now incremented rather than reset
  to 0); `onDestroy` also bumps it. Project switches clear `rowsCache`,
  `pendingPages`, `loadedPages`, totals, the position map, and the
  `memoEditor` / `expandedCell` / IOC manager dialog state; the old
  `filtered` total is reset while the new page loads.
- `src/lib/stores/viewHistory.ts`: `historyRestore` entries now carry the
  project id captured via `setHistoryProject()`. `ProjectView` ignores a
  restore whose project id differs from `currentProjectId` and consumes its
  nonce; switching projects also drops a queued restore. Offset restoration
  still uses the per-project saved view.

### Verification

| Gate | Command | Result |
|---|---|---|
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |

There is no frontend test runner in this repository, so no unit test was added
for the generation logic; the TypeScript checker and production build cover
the call sites.

### Unverified / deferred

- GUI behaviour was not exercised. The completion scenario (A slow, B fast,
  A→B leaves B's columns/rows/counts/pending unchanged; an old search failure
  leaves the new search pending intact) has not been reproduced in the app or
  in a browser mock. It is scheduled for real-app confirmation in T26.
- Cache limits and request cancellation at the engine boundary remain for
  T15/T19/T20b.

Next card: T06.

## T06 — annotation save result matches the displayed state

Recorded: 2026-11-11.

- `src/lib/components/project_view/state.ts`: `memoEditor` now holds
  `{ projectId, rowIndex, row }`, where `row` is the row snapshot taken when
  editing started. `editMemo(row, projectId)` captures the project id and the
  original row id instead of relying on the page cache.
- Added a per-row monotonic request sequence (`nextAnnotationRequest` /
  `isLatestAnnotationRequest`) shared by `setFlag` and memo saves. A failed
  request only rolls back while it is still the newest request for that row.
- `setFlag` reads the current optimistic row from `rowsCache` so rapid clicks
  toggle from what the table shows, updates the cache only when the row exists,
  and rolls back only the flag and the affected counts. It no longer restores
  the whole click-time row snapshot, so an older failure cannot erase a newer
  success or touch memo/tags/bookmark. Successful responses apply only the
  returned flag. Responses for a project that is no longer current are ignored.
- `src/lib/components/project_view/MemoEditorDialog.svelte`: memo save uses the
  captured `projectId` / `rowIndex` and the edit-start annotations as its
  rollback baseline. The row cache is updated optimistically when present, but
  saving no longer depends on it: an evicted row still sends the patch for the
  original row id. Failures revert only memo/tags/bookmark for the latest
  request, set `memoError`, keep the draft and the dialog open, and allow a
  retry. Success applies the returned memo/tags/bookmark and closes only the
  same editor target.
- Every save path now clears `memoSaving`; the missing-row early return and the
  never-unsubscribed `memoEditor` subscription are gone. The subscription is
  unsubscribed in `onDestroy`, and the `await tick()` focus step is skipped when
  the dialog was closed or retargeted. Escape no longer lets the window-level
  handler close the dialog while a save is in flight.
- `src/lib/components/project_view/DataTable.svelte`: passes
  `$projectDetail.project.meta.id` to `editMemo`.
- `src/lib/backend.ts` needed no change: since T02, `updateFlag` already accepts
  `AnnotationPatch` and omits fields the caller did not send.

### Verification

| Gate | Command | Result |
|---|---|---|
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| Engine tests | `cargo test -p trivium-engine` | 118 passed, 0 failed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |

There is no frontend test runner in this repository, so the request-ordering
and cache-eviction paths have no automated UI test.

### Unverified / deferred

- GUI behaviour was not exercised. The completion scenarios (a save failure is
  visible and can be retried; Safe then Critical with only the first request
  failing leaves Critical; a row evicted while editing still saves to the
  original row id) have not been reproduced in the app or a browser mock. They
  are scheduled for real-app confirmation in T26.
- Bookmark-only count parity and virtual-scroll count/position behaviour remain
  for T07.

Next card: T07.

## T07 — bookmark-only and virtual scroll counts match

Recorded: 2026-11-11.

- `src/lib/components/project_view/DataTable.svelte`: removed the local
  `flagFilter` / `search` / condition predicate. `effectiveTotalRows` is now
  `$totalFilteredRows`, the backend count for the applied QuerySpec, so
  bookmark-only and every other filter use the same number. `totalRows` remains
  only in the `{filtered} / {total} rows` meta label.
- `src/lib/components/ProjectView.svelte`: removed the duplicated predicate.
  `effectiveTotalRows` is also `$totalFilteredRows`, so prefetch ranges, spacer
  height and table rendering all use the applied result count instead of
  falling back to the whole project.
- `src/lib/components/project_view/state.ts`: documented that
  `totalFilteredRows` is the applied QuerySpec count and the sole source for the
  virtual row count.
- Scroll clamp: when the total height or viewport height changes and
  `$scrollTop` exceeds `max(0, totalHeight - viewportHeight)`, both the
  `scrollTop` store and `$bodyScrollEl.scrollTop` are set to that maximum. The
  clamp waits for `$pendingPages` to settle so the saved offset restored during
  project load (when the count is briefly 0) is not reset. Setting the store
  and the element to the same value keeps subsequent reactive runs stable.
- Zero results already render the existing `.empty-rows` message
  ("No rows match your filters.") once pending requests finish. The spacer is
  now derived from the same zero count, so no full-project blank area remains.
- Selection, sorting and paging requests were not changed.

### Verification

| Gate | Command | Result |
|---|---|---|
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| Engine tests | `cargo test -p trivium-engine` | 118 passed, 0 failed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |

There is no frontend test runner in this repository, so the count and scroll
clamp have no automated UI test.

### Unverified / deferred

- GUI behaviour was not exercised. The completion scenarios (100 rows with one
  bookmark shows one row; zero results shows the empty state; narrowing near the
  end leaves no long blank table) have not been reproduced in the app or a
  browser mock. They are scheduled for real-app confirmation in T26.

Next card: T08a.

## T08a — column ids and the original record storage format

Recorded: 2026-11-11.

- `crates/engine/src/models.rs`: added `ColumnMeta { id, label, source, order }`
  and `ProjectMeta { storage_version, columns }`. Both new metadata fields
  default for existing `projects.json`, so old projects deserialize unchanged.
  `LoadProjectResponse` now returns `columns_meta` alongside the compatibility
  `columns` list.
- `crates/engine/src/project_io.rs`: added `INTERNAL_ROW_ID` / `INTERNAL_ORIGINAL`
  and `is_internal_column` as the single internal-column exclusion list. Added
  `ProjectColumns` (id → label/source/order lookup; legacy projects synthesize
  ids, labels and sources from the frame column names) and
  `read_original_records`, which parses the new `__original` JSON column per
  row.
- `crates/engine/src/ops/projects.rs`: `load_project` builds columns from
  metadata. Legacy projects keep the original names as ids, labels and sources;
  no new id is guessed for them. New projects return internal ids in `columns`,
  labels/sources/order in `columns_meta`, and keys in `column_kinds` /
  `column_max_chars` by id. Internal columns are excluded.
- Query, condition, sort, export, stats, histogram, time, group, dupes, report,
  flags, surround, IOC and search paths now exclude `__rowid` and `__original`
  through the shared helper. Column references keep using ids (legacy ids are
  the old names), so existing conditions, presets and search scopes keep
  working.
- `crates/engine/src/export/csv.rs`: output columns are resolved by id and
  exclude internal columns. CSV headers are display labels; because Polars
  rejects duplicate DataFrame names, the header row is written separately with
  the `csv` crate and Polars writes only the body. Duplicate labels are emitted
  as-is until T23 adds the id correspondence metadata.
- Frontend: `ColumnMeta` and `columns_meta` were added to `types.ts`. A derived
  `columnLabels` map in `project_view/state.ts` resolves id → label for display.
  Data table headers, condition/column pickers, search-scope text, stats,
  grouping, time, timeline, color-rule and row-detail labels use that map.
  Values, sort keys, conditions, search scopes, hidden columns and output
  columns still use ids, so legacy projects display and behave as before.
- `crates/engine/tests/storage_v2_test.rs`: a hand-built storage-version-2
  fixture covers metadata loading, internal-column exclusion, two columns with
  the same label addressed by different ids, a data column whose label equals
  `__rowid`, original JSON records with long integers / explicit null / missing
  keys / nested and dotted keys, label-based export headers, and the legacy
  project path.
- The CSV/JSONL/JSON importers were not changed. `create_project` still writes
  the legacy typed frame and records `storage_version = 0` until T08b.

### Verification

| Gate | Command | Result |
|---|---|---|
| Engine tests | `cargo test -p trivium-engine` | 123 passed, 0 failed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |

### Unverified / deferred

- GUI behaviour was not exercised. The label display and id-based value access
  have no automated UI test in this repository; the real-app confirmation is
  scheduled for T26.
- The new storage format is covered by the hand-built fixture only. No real
  import writes `__original` or `c000001` ids until T08b, and no legacy project
  was converted or overwritten.
- `read_original_records` is exercised by the fixture; no UI path reads it yet.

Next card: T08b.

## T08b — import CSV/JSONL with column ids

Changes:
- CSV import assigns positional ids `c000001…`, so duplicate/empty headers and
  input columns named `__rowid` / `trivium-*` no longer collide. Every row also
  stores `__original` as the raw cell-string array.
- JSONL import flattens objects with JSON Pointers (`/a/b` vs `/a.b` stay
  distinct); `__original` holds the source object, arrays stay in one cell.
- Annotation restore is explicit: `CreateProjectPayload.restore_annotations`
  (default false). `create_project` writes storage version 2 with column
  metadata for new projects.
- Internal columns (`__rowid`, `__original`) are excluded from search, display,
  analysis, and export by the shared `is_internal_column` check.

Verification:
- `cargo test -p trivium-engine`: all suites pass (28 test binaries green).
- `cargo test -p trivium-engine --test import_ids_test`: 4 passed.
- `bash scripts/check_rust_size.sh`: exit 0.
- `npm run lint`: 0 errors / 0 warnings. `npm run build`: success.

Unverified: GUI import paths are not exercised (T26); value/type preservation
for long integers and CSV raw strings is refined in T09.

Next card: T09.

## T09 — exact CSV/JSON values

Changes:
- CSV import stores every cell as the original text (`Utf8`); kinds are derived
  metadata, so `00123`, padding, datetimes, and long integers are not rewritten.
- JSONL scalars are stored as exact text: numbers keep their source spelling
  (`serde_json` now uses `arbitrary_precision`), bools become `true`/`false`,
  `null` and missing keys stay null, and `""` stays an empty string. Arrays stay
  in one cell; an empty object still counts as one record.
- Condition numeric comparisons use i128 only and return an explicit error for
  decimal spellings or values outside i128 (`eval` now returns `Result`).
  Equality/text operators still work on the original strings.
- Sorting uses i128 keys; values outside i128 fall back to text order instead of
  being rounded through f64.
- Analysis (histogram/time/stats) keeps working by parsing the text values.

Verification:
- `cargo test -p trivium-engine`: 29 test binaries green, including the new
  `value_preservation_test` (4 tests) and updated `condition_eval` /
  `precision_test` / `smoke` / `jsonl_test` expectations.
- `bash scripts/check_rust_size.sh` exit 0; `npm run lint` 0 errors;
  `npm run build` success.

Unverified: IPC value DTOs and UI display/copy of long integers are T10;
GUI paths remain unverified until T26.

Next card: T10.

## T10 — exact cell text and kind across the IPC boundary

Recorded: 2026-11-11. Code commit `7c23031`.

Changes:
- `crates/engine/src/models.rs`: added `Cell { text: Option<String>, kind }`
  and `ProjectRow.cells` (`#[serde(default)]`). The compatibility `data` field
  is unchanged.
- `crates/engine/src/value_utils.rs`: `CellColumns` infers the kind once per
  column from the frame (never per cell) and maps it to `int` / `float` /
  `string`. `build_cell` keeps the exact cell text from
  `anyvalue_to_search_string`; integers are decimal strings regardless of
  digit count. A null cell is `missing` when the row's `__original` object has
  no key for the column's JSON Pointer, and `null` when the key exists with a
  JSON null. CSV originals are arrays and legacy projects have no `__original`,
  so those cells are never `missing`.
- Page materialization (`ops/query/page.rs`, `ops/util.rs`) builds `cells` for
  query pages, initial rows, surrounding rows, duplicate results, and the row
  returned by a flag update. Column metadata comes from `ProjectColumns`, and
  internal columns (`__rowid`, `__original`) are not included.
- `src/lib/types.ts`: added `Cell` / `CellKind` and `ProjectRow.cells`.
- `src/lib/ipcValues.ts` (new): pure `displayText`, `copyText`, and
  `buildCellCaches`. Display keeps empty string blank, shows an explicit null
  as `null`, and a missing key as `—`. Copy uses the source representation:
  `null` copies as `null`, missing copies as an empty string; numbers are never
  parsed.
- `project_view/state.ts`: `normalizeRow` builds `displayCache` and a new
  `copyCache` from `cells`; `CachedRow` carries both. `formatCell` remains but
  is no longer used on the cell display path.
- `DataTable.svelte`: renders `displayCache` without the old `|| '—'`
  fallback, so `""` and missing stay distinct; `openCell` /
  `handleCellKeydown` receive `copyCache` (the exact text).
- `ExpandedCellDialog.svelte`: the field list and surrounding-row lines read
  the cell display text, and Copy uses the exact value passed from the table
  (`copyCache`).
- `src/lib/ipcValues.test.ts` (new): reads IPC-shaped JSON for
  `9007199254740993`, u64::MAX, null, missing, empty string, and bool cells,
  and asserts display/copy stay exact (`Number("9007199254740993")` would be
  `9007199254740992`). It also asserts table/detail (`displayText`) and
  copy/filter (`copyText`) share the same exact string.
- `package.json`: added `test:ui` (`node --test --experimental-strip-types`).
  `tsconfig.json` excludes `src/**/*.test.ts`, because the Node test imports
  the `.ts` module with an explicit extension.

### Verification

| Gate | Command | Result |
|---|---|---|
| IPC cell test | `cargo test -p trivium-engine --test ipc_cells_test` | 1 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

The Rust test inspects `serde_json::to_value(&response)`, i.e. the same JSON
shape the frontend receives, and checks `rows[0].cells` plus the
`load_project` initial rows.

### Unverified / deferred

- GUI behaviour was not exercised. The real table, detail dialog, and
  clipboard were not operated; the display/copy distinction is covered only by
  the pure `ipcValues` test. Real-app confirmation remains for T26.
- There is no cell-to-filter control in the current UI (it is scheduled for
  T21), so the filter path is asserted through `copyText` rather than an actual
  filter action.
- Native Tauri compilation was not run here (GTK/GDK development libraries are
  unavailable); `src-tauri` command signatures are unchanged.

Next card: T11.

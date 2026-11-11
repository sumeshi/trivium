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

## T11 — open JSON arrays and JSONL from the GUI

Recorded: 2026-11-11. Code commit `263403c`.

Changes:
- `crates/engine/src/import/json.rs` (new): reads a UTF-8 JSON file (BOM
  ignored). A single object becomes one record; an array becomes one record
  per element. A non-object array element fails with its 1-based position
  (`Record 2 in the JSON array is not an object.`), and any other top-level
  value is rejected. Parse failures use `Failed to parse JSON: …`.
- `crates/engine/src/import/jsonl.rs`: the record → columns/DataFrame part is
  now `pub(crate) fn build_import(records: &[(usize, Value)], source_format:
  &str)`. `json.rs` and `jsonl.rs` both call it, so the column union,
  first-appearance order, JSON Pointer paths, exact cell text, null/absent
  handling, and `__original` payload are unchanged from T08b/T09. JSONL keeps
  the physical line number (empty lines included) for parse and non-object
  errors; blank lines are still skipped.
- `crates/engine/src/ops/projects.rs`: `create_project` dispatches on the
  lowercased extension: `.json` → `json::load`, `.jsonl` / `.ndjson` →
  `jsonl::load`, everything else → CSV. There is no CSV fallback for a
  malformed JSON/JSONL file.
- `src/App.svelte`: the Tauri open dialog accepts `csv`, `json`, `jsonl`,
  `ndjson`; the empty-state text and import failure message mention CSV or
  JSON instead of CSV only.
- `src/lib/components/app/Sidebar.svelte`: button and empty-project text
  mention CSV / JSON. No layout change and no delimiter UI change (the
  delimiter still applies only to CSV).
- `src/lib/backend.ts` needed no change; `createProject` already forwards the
  selected path and the backend decides by extension.
- `crates/engine/tests/json_import_test.rs` (new): 8 tests covering a 3-record
  array (3 rows, Pointer columns, later-only key), a single object, a numeric
  array element, broken JSON without CSV fallback and without a registered
  project, uppercase `.JSON` / `.NDJSON`, CSV/JSON/JSONL equivalence for the
  same 3 records, JSONL physical line numbers after a blank line, and project
  reload after an array import.

### Verification

| Gate | Command | Result |
|---|---|---|
| New JSON tests | `cargo test -p trivium-engine --test json_import_test` | 8 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- The real GUI was not operated: the native Tauri app was not built here
  (`pkg-config` reports GTK 3 unavailable), so selecting a `.json` / `.jsonl`
  file through the dialog and reading the table is deferred to T26. The dialog
  filter and the engine path are covered only by the source change and engine
  tests.
- Large-file streaming (T17) and temporary-directory atomic creation (T12)
  were not started; JSON arrays are still read into memory as before.
- `.csv` files still use the existing CSV path, including its delimiter
  option; no new JSON-specific UI settings were added.

Next card: T12.

## T12 — staged project creation survives failure and interruption

Recorded: 2026-11-11. Code commit `ad4156e`.

Changes:
- `crates/engine/src/ops/projects.rs`: `create_project` copies the source into
  a staging directory while computing SHA-256 with a fixed 64 KiB buffer. The
  old `source_sha256` path (`File::open` + `read_to_end` + a second read by the
  importer) is gone. The importer now parses the copied bytes, so the recorded
  hash and the imported content are the same snapshot.
- After the copy, the source path's size and modification time are checked
  against the values from the opened file. A mismatch fails with
  `Source file changed during import. Try again.` The source is only opened
  read-only; it is never written, renamed, or moved.
- `StagingDir` is an RAII guard that removes its directory on every exit path
  (including a panic) and is disarmed only after the rename. `data.parquet` and
  `flags.json` are written under `{store_root}/.staging/{uuid}`, the copied
  source is removed, and the complete directory is renamed to
  `projects/{uuid}`. `store.insert` runs only after the rename; if it fails,
  the final directory is removed again.
- `crates/engine/src/store.rs`: added `staging_root()` and `staging_dir(id)`
  (`{store_root}/.staging/{uuid}`). Completed projects still live under the
  unchanged `project_dir(id)`. `insert` now rolls its in-memory entry back
  when `projects.json` cannot be persisted, so a failed registration is not
  listed as a project before the directory is removed.
- `crates/engine/src/models.rs`: `ProjectMeta` gained `#[serde(default)]`
  `source_file_name`, `source_format` (`csv` / `json` / `jsonl` / `ndjson`),
  and `source_size`. Legacy `projects.json` files deserialize with `None`.
  Source record numbers stay in the existing zero-based `__rowid` column;
  `total_records` remains the record count.
- `src/lib/types.ts`: the corresponding optional `ProjectMeta` fields were
  added for the IPC shape.
- `crates/engine/tests/storage_v2_test.rs`: the two hand-built `ProjectMeta`
  literals set the new fields to `None`.

### Verification

| Gate | Command | Result |
|---|---|---|
| New regression suite | `cargo test -p trivium-engine --test project_create_test` | 4 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

Regression coverage (`project_create_test.rs`):
- Broken JSON and a CSV row with the wrong field count fail; `store.all()` stays
  empty and neither `projects/` nor `.staging/` has an entry.
- A successful CSV import records `source_file_name=events.csv`,
  `source_format=csv`, the byte length as `source_size`, and a SHA-256 that
  equals the digest of the source bytes. The source bytes are unchanged and the
  staging directory is empty afterwards.
- `__rowid` values are `0, 1, 2` for a 3-record import.
- Importing the same JSONL input twice creates two projects with the same
  hash and removes both staging directories.

### Unverified / deferred

- Killing the process in the middle of an import is not tested. The RAII guard
  covers errors and panics in-process, but a hard process kill can leave a
  `.staging/{uuid}` directory until cleanup exists in T16/T15. It is not a
  completed project and is not listed in `store.all()`.
- Detecting a source that changes during the copy is not tested; forcing the
  timing would be flaky. The test fixes the contract that the metadata hash
  equals the imported bytes, and the size/mtime re-check is covered by code
  review only.
- The GUI import path was not exercised (the native app was not built here).
  `project_io.rs` needed no change: the read/write helpers already operate on
  explicit paths.
- `source_size` is the number of copied bytes; it is not re-read from the
  source after import.

Next card: T13.

## T13 — a single annotation update does not read project data

Recorded: 2026-11-11. Code commit `390c0a1`.

Changes:
- `crates/engine/src/ops/flags.rs`: `update_flag` now returns
  `UpdateFlagResponse { entry, revision, flagged_records }` instead of a
  `ProjectRow`. It validates `row_index` against `meta.total_records`, reads
  only the target row's annotation, applies the patch, and writes the entry.
  The Parquet read, row materialization, cell building, and color evaluation
  from the update path are gone.
- The whole-project count is adjusted by the difference for the updated row
  (flag empty → non-empty adds one, non-empty → empty removes one, anything
  else keeps it). `count_flagged` is no longer called by `update_flag`; it
  remains for full-count callers such as bulk updates.
- `crates/engine/src/store.rs`: `ProjectsStore` keeps a process-local
  `AtomicU64` per project. `next_revision` returns a monotonic revision for
  successful annotation updates; it is not persisted, as the card allows.
- `crates/engine/src/storage.rs`: annotation databases are cached per path in
  `FLAGS_DBS` and shared through cloned `sled::Db` handles. Added
  `load_flag(path, row_index)` for a direct `get`. `upsert_flag`,
  `remove_flag`, `count_flagged`, and full `load_flags` use the cached handle;
  `write_flags_to_db` (import staging and legacy migration) closes any cached
  handle first so the database is not moved while open. `close_flags_db`
  drops a project's handle, and `delete_project` calls it before removing the
  directory. The sled key format and stored JSON are unchanged.
- `crates/engine/src/project_io.rs`: `PROJECT_DATA_READS` increments in
  `read_project_dataframe`. `project_data_reads()` and
  `reset_project_data_reads()` are test instrumentation.
- `src/lib/backend.ts`, `src/lib/types.ts`: `updateFlag` resolves to
  `UpdateFlagResult` (`entry`, `revision`, `flaggedRecords`), with a new
  `FlagEntry` type.
- `src/lib/components/project_view/state.ts` and `MemoEditorDialog.svelte`:
  successful updates apply the returned entry to the existing cached row
  (flag, memo, tags, bookmark), leaving row data and cell caches intact.
  `flaggedCount` takes the authoritative returned count; `filteredFlaggedCount`
  keeps its existing local delta. T06's per-row request ordering still guards
  stale successes and rollbacks.
- Existing tests read `result.entry`. The color test now updates a row and
  verifies the same color through the query path, because update no longer
  returns row data.

Regression coverage (`crates/engine/tests/annotation_update_test.rs`, 5 tests):
- With a 20-row project, flag, memo, and bookmark updates each leave
  `project_data_reads()` at 0 after a reset.
- Two rows are annotated; updating one row's memo leaves the other row's
  entry unchanged when read directly with `load_flag`.
- In a 10-row project, three annotations report 3; adding a fourth reports 4,
  clearing it reports 3, and adding only a memo keeps 3. Revisions increase on
  every successful update.
- `row_index == total_records` is rejected with `Row index out of range.` and
  writes no entry.
- After closing the flags handle and reopening `ProjectsStore` on the same
  root, the entries and `flagged_records` remain.

### Verification

| Gate | Command | Result |
|---|---|---|
| New regression suite | `cargo test -p trivium-engine --test annotation_update_test` | 5 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- The GUI was not operated: the native Tauri app was not built in this
  environment. The updated TypeScript stores compile and lint, but clicking a
  flag or saving a memo in the real window is deferred to T26.
- `delete_project` closes the annotation handle. Process shutdown does not
  explicitly close all remaining handles yet; that lifetime and its
  measurement belong to T20/T27, not this card.
- Bulk annotation and undo still use full `load_flags`/`count_flagged`
  scans and per-row opens/flushes. Atomic batches, undo conflict handling,
  and worker movement were not started (T14 and later).
- The revision is process-local by design; it resets when the process or
  store is recreated.

Next card: T14.

## T14 — bulk annotation and undo are saved as one operation

Recorded: 2026-11-11. Code commit `947bdff`.

Changes:
- `crates/engine/src/storage.rs`: added
  `apply_flag_batch(path, changes)`. `None` removes a row's entry and `Some`
  inserts the serialized entry, both queued in one `sled::Batch`. The batch is
  committed with a single `apply_batch` and one flush instead of opening and
  flushing per row. `upsert_flag` and `remove_flag` remain for single-row
  callers.
- `crates/engine/src/ops/bulk.rs`: `bulk_update` still resolves and fixes the
  target before writing, then computes every row's next entry in memory and
  writes the whole target with `apply_flag_batch`. The target is not
  re-resolved while writing, so a flag-filtered bulk cannot grow or shrink
  mid-operation. The preview token check is unchanged.
- The flagged count is no longer a full `count_flagged` scan. The previous and
  applied entries give the per-row flag transitions, which are applied to the
  project's existing count. The same difference calculation is used by undo.
- Undo state is now `Mutex<HashMap<Uuid, BulkUndo>>`, one level per project.
  `BulkUndo` keeps both `previous` and `applied` entries plus the revision
  issued by the operation. A conflict only affects the project it belongs to.
- `bulk_undo` validates every target row with `load_flag` against `applied`
  before writing. On a mismatch it returns
  `Annotation changed since the bulk operation; undo would overwrite it.`,
  writes nothing, and keeps the undo entry for a later retry. Only after a
  successful `apply_flag_batch(previous)` is the entry removed, and it is
  removed only when its revision still matches (another bulk may have replaced
  it).
- `crates/engine/src/store.rs`: no new API is needed; the revision issued by
  `next_revision` is stored inside `BulkUndo` so a restored entry is removed
  only if it is still the latest one for that project.
- Annotation writes no longer fail because the project-list count could not be
  persisted. After a successful batch, a failed `update_flagged_records` logs
  `[bulk] annotations were saved but the flagged count ... could not be
  persisted` and the operation still succeeds; the annotation DB is the source
  of truth and the count is recomputable.

Regression coverage (`crates/engine/tests/bulk_test.rs`, 5 new tests):
- Clearing the flag on a project with memo, tags, and bookmark entries, then
  undoing, restores all four fields exactly and returns the count to 2.
- A bulk on project A and a bulk on project B keep separate undo entries;
  undoing A leaves B annotated, and B's undo still succeeds afterwards.
- A single-row update after a bulk makes undo report the conflict without
  writing; a second undo reports the same conflict, proving the entry remains.
- A filtered bulk changes the flagged count by the same amount as a full
  recalculation from the annotation DB, and undo returns both to the
  pre-bulk value.
- A stale preview token fails before the batch and leaves every annotation
  unchanged.

### Verification

| Gate | Command | Result |
|---|---|---|
| Bulk suite | `cargo test -p trivium-engine --test bulk_test` | 10 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- The GUI was not operated and the native Tauri app was not built in this
  environment; real clicks on bulk annotate/undo are deferred to T26.
- Mid-batch I/O failure was not injected. The single `apply_batch` makes a
  half-written target unlikely by design, and the test covers the pre-batch
  failure (stale token), but sled's own atomicity was not fault-injected
  because doing so reliably would require an artificial failure hook.
- Undo is one level per project in process memory and does not survive a
  restart. This matches the card's per-project requirement; it is not a
  missing persistence feature.

Next card: T15.

## T15 — background jobs and cancellation plumbing

Recorded: 2026-11-11.

Changes:
- `crates/engine/src/jobs.rs` (new): a Tauri-independent job registry.
  `try_begin_job(kind, project_id)` returns a `JobGuard` that removes its entry
  on drop; `request_cancel(id)` returns `false` for an unknown or finished ID;
  `is_cancelled`, `active_jobs`, and `cancelled_error` ("Cancelled.") complete
  the API. Heavy jobs are capped at `MAX_CONCURRENT_JOBS = 2`; a third request
  fails with `Too many long-running operations. Wait for one to finish.`
  Job IDs are monotonic and act as the generation, so a late cancel of an old
  ID cannot select a newer job.
- The same module has `JobInfo { id, kind, projectId }`, `JobProgress { id,
  kind, done, total, message }`, `JobContext`, and `ThrottledProgress`.
  `JobContext::none()` keeps the previous engine API behaviour;
  `JobContext::with_job` checks cancellation and forwards progress.
  `ThrottledProgress` emits the first value and then at most one per
  `PROGRESS_INTERVAL = 200 ms`; its clock is injectable for tests.
- `crates/engine/src/ops/projects.rs`,
  `crates/engine/src/ops/query/mod.rs`, and
  `crates/engine/src/export/csv.rs`: added `create_project_with_job`,
  `query_project_rows_with_job`, and `export_project_with_job`. The existing
  functions delegate with `JobContext::none()`, so direct engine callers and
  all existing tests keep their behaviour.
- Cancellation checkpoints are stage boundaries only: before and after the
  source import; around the data read and target resolution for queries;
  before the CSV write and after it for exports. The large CSV/JSON parsers,
  search evaluation, and CSV writer still run as single library calls. They
  are not interruptible yet; T16/T17 add import batch cancellation, T18 adds
  search batch cancellation, and T23 adds destination-preserving output
  cancellation.
- `crates/engine/src/store.rs`: `ProjectsStore` now keeps its metadata and
  revision maps in `Arc`s and derives `Clone`, so a background worker owns a
  cheap handle instead of the Tauri state guard.
- `src-tauri/src/commands/job.rs` (new): `run_job` creates the guard, emits
  `trivium:job-started`, moves a `JobContext` with an event-forwarding
  `ProgressFn` into `tauri::async_runtime::spawn_blocking`, and returns the
  engine result. `cancel_job(jobId)` and `active_jobs()` are new commands.
  `create_project`, `query_project_rows`, and `export_project` are now async
  commands that clone the store and run through `run_job`; no long operation
  holds `State`. `commands/mod.rs` re-exports the new commands and `main.rs`
  registers them with bare identifiers.
- Completion, failure, and cancellation are distinguished at the command
  boundary: `Ok` is completion, `Err` with the text `Cancelled.` is a cancel,
  and any other `Err` is a failure. `trivium:job-progress` is throttled by the
  engine helper before it reaches the event emitter.
- `src/lib/types.ts`: added `JobKind`, `JobInfo`, and `JobProgress`.
  `src/lib/backend.ts`: added `cancelJob(jobId)`, `activeJobs()`, and
  `listenToJobEvents` (subscribes to both job events and returns one unlisten
  function). No UI subscribes yet; that stays in T18/T23.

Regression coverage (`crates/engine/tests/jobs_test.rs`, 7 tests):
- Two jobs run, a third is rejected, releasing one slot admits one more.
- A cancel reaches only the requested ID; an unknown ID is rejected.
- Cancelling a finished ID returns `false` and does not touch a new job.
- `ThrottledProgress` emits at 0, 200, and 400 ms on an injected clock and
  suppresses the reports at 100, 199, and 399 ms.
- `JobContext::none()` never cancels; a real context reports progress, then
  returns `Cancelled.` after `request_cancel`.
- A cancelled query and a cancelled import fail before reading project data or
  the selected path.

### Verification

| Gate | Command | Result |
|---|---|---|
| New job tests | `cargo test -p trivium-engine --test jobs_test` | 7 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | 161 passed, 0 failed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- `src-tauri` was not compiled in this environment. `cargo check -p
  trivium-desktop` stops while building `gdk-sys` because `gdk-3.0.pc` is
  missing (GTK/WebKit development libraries are unavailable). The new command
  signatures, `Emitter`/`spawn_blocking` usage, re-exports, and
  `generate_handler!` entries were checked by source inspection only. The
  Linux CI job remains the native compile check.
- Window responsiveness and real cancellation were not exercised in the
  native app. The event delivery, `spawn_blocking` hand-off, and command return
  values are covered by code review, not by a running Tauri window.
- Existing full-library calls are not immediately interruptible. A cancel is
  observed at the next stage boundary; import batch interruption is T16/T17,
  search interruption is T18, and output preservation is T23.
- Import jobs start with `projectId = null` because the engine creates the
  project ID during the import.
- If a cancel is requested during the final CSV write, the export observes it
  after the write and returns `Cancelled.` while the destination may already
  exist. Safe temporary-file output is deferred to T23.
- Progress is reported only at the stage boundaries above, so a single long
  library call may produce no intermediate progress even though the throttle
  is 200 ms. The throttle itself is unit-tested with an injected clock.

Next card: T16.

## T16 — CSV import uses fixed-size batches

Recorded: 2026-11-11.

Changes:
- `crates/engine/src/import/csv.rs`: removed the `fs::read` path and the
  whole-file UTF-8 conversion. `CsvImporter` opens the staged file, lets the
  `csv` crate resolve quotes, embedded newlines, and escaping, and returns one
  bounded `DataFrame` per `next_batch` call. The data columns and `__original`
  are the only accumulated representation; the input bytes and a decoded copy
  of the whole file no longer coexist with the finished frame.
- The same file computes the leading-sample width estimate while reading and
  accumulates `trivium-*` annotation columns without a second pass over all
  records. `csv::load` remains as a convenience wrapper for direct callers and
  tests; production import uses the stream and writes each batch.
- `crates/engine/src/import/utf16.rs` (new): streaming UTF-16LE/BE to UTF-8
  adapter. It reads 16-bit units incrementally, so odd bytes, surrogate pairs,
  and CSV records may cross read-buffer boundaries. A lone surrogate or a
  trailing odd byte is an error, not a replacement character.
- Batch boundaries use both limits: 50,000 rows (`DEFAULT_BATCH_ROWS`) and an
  approximate 8 MiB of cell text plus `__original` JSON
  (`DEFAULT_BATCH_BYTES`). The byte count is explicitly an estimate. A single
  record larger than the limit still forms one batch, so memory is not
  constant for a single huge record.
- `crates/engine/src/project_io.rs`: added `BatchDataWriter`, built on polars
  0.40's `ParquetWriter::batched(schema)` and `BatchedWriter::write_batch`.
  Every `write_batch` call writes its own row group and `finish` writes the
  footer. The schema is fixed from the CSV header (data columns as Utf8, then
  `__original`, then `__rowid`). The existing Parquet + metadata save format is
  unchanged.
- `crates/engine/src/ops/projects.rs`: the CSV branch streams importer batches
  into `BatchDataWriter` and writes `column_max_chars.json` into staging from
  the sampled estimate, so `load_project` does not rescan the frame. JSON and
  JSONL still use the existing load-then-write path (T17).
- `crates/engine/src/storage.rs`: `compute_column_max_chars` now scans at most
  `COLUMN_WIDTH_SAMPLE_ROWS = 1_000` leading rows. This is the legacy-project
  fallback as well; the initial table layout only needs the sample. The saved
  file format and the meaning of `load_project` are unchanged.
- Cancellation is checked at every CSV batch boundary. A cancel requested from
  the previous batch's progress callback is observed before the next batch is
  read, and the T12 staging guard removes the partial directory.
- `crates/engine/src/jobs.rs`: comments now distinguish the CSV batch
  checkpoints from the JSON/search/export calls still deferred to T17/T18/T23.

Regression coverage (`crates/engine/tests/import_streaming_test.rs`, 7 tests):
- Quoted commas, doubled quotes, and cell-internal newlines with a 4-byte read
  buffer and one-row batches keep the exact values and row count.
- UTF-16LE and UTF-16BE with a 3-byte read buffer match the ASCII import for
  columns, row count, and representative values, including a surrogate-pair
  emoji and an embedded newline.
- A generated 100,000-row CSV imports through the staged path, `load_project`
  reports 100,000 rows, and the Parquet file has at least two row groups.
- Cancelling between batches returns `Cancelled.`.
- A cancelled `create_project_with_job` leaves no registered project and no
  staging directory.
- The import width estimate adopts the sample maximum (42) and ignores a
  500-character value after the sample.
- The legacy `compute_column_max_chars` fallback also ignores values after the
  sample.

### Verification

| Gate | Command | Result |
|---|---|---|
| New streaming tests | `cargo test -p trivium-engine --test import_streaming_test` | 7 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | 168 passed, 0 failed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- Peak RSS was not measured in this card. T27 records it. The whole-file byte
  and UTF-8 buffers are gone by construction, but a single oversized record
  still grows its batch and the Parquet encoder.
- The native GUI was not exercised. Import through the Tauri command, progress
  delivery, and cancel from the UI remain code-review checks.
- JSON and JSONL still load all records before writing (T17). Search batches
  (T18) and destination-preserving output cancellation (T23) are unchanged.

Next card: T17.

## T17 — JSON import keeps no record list

Recorded: 2026-11-11.

Changes:
- `crates/engine/src/import/json_source.rs` (new): shared record streaming for
  the two import passes. JSONL is read line by line with the physical line
  number for errors. A JSON array goes through `serde_json::Deserializer` and a
  custom `Visitor`/`deserialize_seq`, so each element is dropped before the next
  one is parsed. A single JSON object is one record. Non-object array elements
  keep the T11 message with the element number.
- `crates/engine/src/import/jsonl.rs`: `JsonImporter` now runs pass 1 to collect
  the JSON Pointer column union in first-appearance order and count records,
  then pass 2 to re-read the same file and write bounded batches. Neither pass
  keeps `Vec<Value>` records. Pass 1 reports progress every 1,024 records and
  checks cancellation; pass 2 checks cancellation per record, so a request from
  the previous batch's progress callback stops at the next boundary.
- Pass 2 maps each flattened pointer to a column index through a
  `HashMap<String, usize>` and fills `Vec<Option<String>>` per column. It does
  not search the column list for each cell. Batch limits are 50,000 rows and an
  approximate 8 MiB of cell text plus `__original`, matching T16. A batch is
  written through the existing `BatchDataWriter` (one Parquet row group per
  batch). File size and record count are rechecked between passes.
- Values follow T08–T10: numbers keep their source text, booleans become
  `true`/`false`, null and absent keys stay null, an empty string stays `""`,
  arrays/objects stay JSON text in one cell, and `__original` keeps the source
  object. An empty object is still one record with no data columns.
- The leading-sample width estimate is computed during pass 2 and written to
  `column_max_chars.json` in staging, so `load_project` does not rescan the
  frame.
- `crates/engine/src/import/json.rs`: `json::load` delegates to the shared
  streaming loader. `jsonl::load` and `json::load` remain for direct callers and
  tests; production import uses the two-pass importer.
- `crates/engine/src/ops/projects.rs`: JSON and JSONL now use the same batch
  path as CSV. The load-then-write branches were removed.
- `crates/engine/src/jobs.rs`: the module comment now lists CSV/JSON import
  batch checkpoints as implemented.

Regression coverage (`crates/engine/tests/json_streaming_test.rs`, 9 tests):
- A key that first appears only in the last record stays a column and keeps its
  value; absent keys stay null.
- A generated 50,000-element JSON array imports every record; the first and last
  rows have the expected `id`/`msg` values.
- JSON and JSONL with the same records produce the same Pointer columns, row
  count, and cell text.
- `u64::MAX`, `2^53 + 1`, an i128-sized number, null, missing, empty string, and
  an empty-object record keep their values; `__original` restores the types and
  missing keys.
- Cancellation during pass 1 and at a pass-2 batch boundary returns
  `Cancelled.`; a cancelled staged import leaves no project or staging
  directory.
- Broken JSON, a non-object array element, a broken JSONL line after a blank
  line, and a non-object JSONL line report the element/line number.
- A streamed JSON array can be reopened with `load_project`.

### Verification

| Gate | Command | Result |
|---|---|---|
| New streaming tests | `cargo test -p trivium-engine --test json_streaming_test` | 9 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | 177 passed, 0 failed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- Peak RSS was not measured in this card. T27 records it. The per-record
  `Value` and the record list are gone by construction, but a single oversized
  record still grows its parsed value and formatted `__original` string.
- The native GUI was not exercised. Import through the Tauri command, progress
  delivery, and cancel from the UI remain code-review checks.
- Search batches (T18) and destination-preserving output cancellation (T23)
  are unchanged.

Next card: T18.

## T18 — search is prepared once and evaluates only required columns

Recorded: 2026-11-11.

Changes:
- `crates/engine/src/ops/query/search.rs`: added `PreparedQuery`. It tokenizes
  the search string once, converts those tokens into the condition tree,
  compiles the tree once (`PreparedNode`), and resolves the explicit condition
  and column-scope names to data columns before any row is evaluated. Unknown
  condition columns, unknown scoped search columns, and unknown
  `searchColumns` fail with `Unknown column: <name>` before evaluation.
- Row evaluation now runs in fixed 1,024-row batches and checks
  `JobContext::check_cancelled()` at each boundary. Each row builds a map with
  only the columns its condition tree references. The joined, lowercased search
  scope text is built per row only when an unscoped term exists.
- `crates/engine/src/condition/eval.rs`: added `PreparedNode` with precompiled
  `regex::Regex` values. The compatibility `eval(node, row)` entry point
  prepares on each call, while query evaluation reuses the prepared tree.
  Invalid regex patterns now return an error instead of silently evaluating
  false; the existing `condition_eval` test was updated for this specified
  behaviour change.
- `crates/engine/src/search/text.rs`: extracted `push_searchable_value` so the
  batched per-row join keeps exactly the `build_searchable_text` normalization
  (lowercase, one space, skip null/empty).
- Searchable-text cache: `prepare_search` no longer loads, clones, builds, or
  saves the sled `SearchableCacheEntry`. The storage helpers remain for other
  callers, but an unconditional display does not touch them.
- `resolve_target` now receives `&JobContext`; the page query passes its job,
  export passes its job, and bulk/report/group/histogram/time use
  `JobContext::none()` so their behaviour is unchanged.
- Test instrumentation only: `value_utils` records per-column stringification,
  `condition::eval` counts regex compilations, and `search::text` counts full
  searchable-text builds. These counters are documented as test hooks.

Regression coverage (`crates/engine/tests/search_prepare_test.rs`, 5 tests):
- 100 rows with one regex pattern compile the regex once.
- A condition on `small` stringifies `small` 100 times and a 1 MiB `big` column
  zero times.
- An unconditional query builds no full searchable-text vector.
- Unknown condition/search columns, unknown scoped terms, and an invalid regex
  fail before evaluation.
- A pre-cancelled query job returns `Cancelled.` over a 5,000-row fixture.

### Verification

| Gate | Command | Result |
|---|---|---|
| New search tests | `cargo test -p trivium-engine --test search_prepare_test` | 5 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | 182 passed, 0 failed, exit 0 |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- The native GUI was not exercised. Search input, cancel from the UI, and
  progress delivery remain code-review checks.
- Cancellation is checked before each batch. A single very large cell is still
  stringified after the boundary check for its row; the response time depends
  on that cell.
- `sort` and `ordered_row_indices` remain whole-frame calls. Reusing results
  across page moves and sort/page work is T19, not this card.
- The searchable-text cache helpers are still present in `storage.rs` but the
  query path no longer reads or writes them.

Next card: T19.

## T19 — repeated page moves reuse the resolved row ids

Recorded: 2026-11-11.

Changes:
- `crates/engine/src/ops/query/result_cache.rs` (new): process-local `ResultCache`
  holding only original row ids (`Arc<Vec<usize>>`) and the two annotation
  counts. It is an LRU with 2 entries and a 64 MiB total budget by default.
  `estimated_bytes = ids.len() * size_of::<usize>() + 64`. A result whose own
  estimate exceeds the budget is not stored, so a later request cannot report
  a hit for it. `result_cache_stats()` returns entries, total/max bytes,
  hits, misses, and evictions; `clear_result_cache()` is a test hook.
- Cache key: project id + normalized `QuerySpec` JSON + `data.parquet`
  length/mtime + an annotation token. Offset, limit, and output columns are not
  part of the key. The normalized spec treats empty `searchColumns` as omitted,
  blank search as absent, `flagFilter: "all"` as omitted,
  `bookmarkedOnly: false` as omitted, an empty sort key as absent, and
  `sortDirection: "asc"` as the default. These spellings already had the same
  query meaning.
- Annotation token: only flag-filter and bookmark-only specs include it. It is
  the store's current annotation revision plus the IOC definition signature,
  because a flag filter also reads IOC flags. Memo/tag changes therefore do not
  invalidate a data-only result. Data revision comes from file metadata; when
  it cannot be read, the result is not cached.
- `crates/engine/src/ops/query/mod.rs`: a hit skips `resolve_target` (search and
  sort), slices the cached ids for the requested offset/limit, reads only those
  rows with `load_flag`, and materializes the page. A miss keeps the existing
  full load + resolve path, then stores the ids. Project deletion removes that
  project's entries (`ops::projects::delete_project`).
- `crates/engine/src/ops/query/page.rs`: `materialize_page` now derives each
  row's final flag from the page's own `FlagEntry` map (user flag wins, else IOC
  flag) instead of being given a project-wide final-flag vector. Other callers
  already pass the full map. This removes the all-row flag vector from the
  query path.
- `crates/engine/src/store.rs`: `current_revision` reads the annotation revision
  without advancing it. `crates/engine/src/ops/query/target.rs`:
  `resolve_count`/`reset_resolve_count` test hooks count `resolve_target` calls.
- `crates/engine/src/ops/rows.rs`: re-exports the stats, clear, and counter
  hooks so integration tests use the public engine crate.

### Annotated count decision on a hit

`annotated_total` is recomputed from `ProjectMeta::flagged_records` when the
project has no IOC definitions; that count is already maintained by flag and
bulk updates, so a flag change is reflected without re-resolving ids. When the
result covers every row, `annotated_filtered` is set to that same total.

For a filtered data-only result, `annotated_filtered` keeps the value from the
miss. Recomputing it exactly would require loading every annotation, which is
what this card removes from the hit path. `annotated_total`/`annotated_filtered`
for projects with IOC definitions also keep the miss value. Consequence: after
a flag or IOC change, a filtered data-only result can show a stale result-set
annotated count until the spec or project changes. Ids and page rows remain
correct because page annotations are re-read on every hit. This trades a stale
secondary counter for not scanning the whole annotation DB on page moves.

Regression coverage (`crates/engine/tests/query_cache_test.rs`, 6 tests; the
test binary serializes on a mutex because the cache is process-local):
- 60 rows, same sorted spec, pages at offset 0/20/40 with limit 20: one
  `resolve_target` call, ids 0..59 with no gap or duplicate, 2 hits.
- Changing the search returns the new row and does not mix the old id.
- `flagFilter: "priority"` and `bookmarkedOnly` specs miss after `update_flag`;
  the new matching row appears.
- A data-only spec still hits after memo and flag updates; the hit reads the
  page memo/flag and reports the updated full-result annotation count.
- Three specs leave 2 entries with 1 eviction; the evicted spec is a miss.
- Deleting a project drops its entries and bytes.
- `result_cache.rs` unit tests inject small budgets to verify the oversized
  result is not retained and the oldest entry is evicted at the total limit.

### Verification

| Gate | Command | Result |
|---|---|---|
| New cache tests | `cargo test -p trivium-engine --test query_cache_test` | 6 passed, 0 failed |
| Cache unit tests | `cargo test -p trivium-engine --lib ops::query::result_cache` | 5 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, 0 failed |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- No wall-clock benchmark was run for a cache hit. The T27 page-move target
  (p95 150 ms, no repeated search/sort) remains unmeasured; this card verifies
  the call count and cache accounting, not latency.
- The native GUI was not exercised.
- A single cached entry still stores one `usize` per matching row. The 64 MiB
  budget bounds it, but a 2M-row result is about 16 MiB and two such results
  are within the configured budget; larger projects can bypass the cache.
- The page query still reads the full `data.parquet` for the page and still
  computes IOC outcomes for the full frame. Reading only the page's data is T20a.
- T20a's page projection, frontend page cache, and cell previews were not
  started.

Next card: T20a.

## T20a — read only the requested page and visible columns

Recorded: 2026-11-11.

Changes:
- `crates/engine/src/project_io.rs`: added `PROJECT_FULL_READS` and
  `PROJECT_PAGE_READS` (test instrumentation only) plus projection recorders.
  `read_project_dataframe` increments the full-read counter;
  `read_page_dataframe` increments the page-read counter. Added footer-only
  helpers (`read_project_schema_columns`, `read_project_row_count`),
  `read_project_columns` for resolver projections, `read_page_dataframe`, and
  `read_page_rows_dataframe` for arbitrary original row ids. Added
  `ProjectColumns::from_meta_and_names` so column metadata can be built from
  the parquet schema without reading rows.
- `crates/engine/src/ops/query/mod.rs`: an unconditional, unsorted page no
  longer calls `resolve_target` or `read_project_dataframe`. It takes row ids
  from the project row count, reads visible columns plus `__original` and
  `__rowid`, loads flags for the page only, and uses `ProjectMeta` for the
  no-IOC annotated count. With IOCs, the persisted IOC cache supplies the
  project count and page outcomes; only a missing/stale cache falls back to a
  projected all-data-column read.
- A filtered or sorted miss reads a resolver projection (search scope,
  condition columns, sort key, `__rowid`) for every row, resolves ids, then
  reads the page projection separately. Display columns are not read for
  resolution, and the resolver frame is not reused for materialization.
- A cache hit (T19) reads only the selected page rows and page annotations.
  Page rows are materialized in display order from an already page-sized
  frame (`materialize_page_rows`).
- The page projection is `visible = data_columns - hidden_columns` plus
  `__original` when the file has it and `__rowid`. Columns referenced by color
  rules are read only when rules exist, so hidden-column colors keep working;
  they are used for color evaluation and are not returned as row data or cells.
- `crates/engine/src/ioc/evaluate.rs`: added cache-only outcome helpers so a
  page does not evaluate IOC definitions against a full frame when the
  persisted IOC cache matches the current definitions and row count.
- `crates/engine/tests/page_read_test.rs` (new, 4 tests): 100 rows,
  unconditional pages 1/2/3 with `PROJECT_FULL_READS == 0`,
  `PROJECT_PAGE_READS >= 3`, correct row ids and values; hidden column absent
  from both the page projection and row cells; scoped search resolves with only
  the search column (display-only column absent from the resolver projection);
  descending sort keeps page order.
- `crates/engine/tests/query_cache_test.rs`: the two T19 regressions that used
  `QuerySpec::default()` now use a sorted data-only spec. T20a's unconditional
  path intentionally does not resolve ids or create a result-cache entry, so
  the T19 assertions needed a spec that still exercises the cache.

### Parquet read decision

- The premise that polars-io 0.40 depends on `parquet2` does not match this
  `Cargo.lock`: polars 0.40 depends on `polars-parquet` 0.40, which depends on
  `parquet-format-safe` 0.2.4. There is no `parquet2` package in the lock file,
  so no duplicate `parquet2` dependency was added.
- polars 0.40's `ParquetReader` exposes `with_columns` and `with_n_rows` but no
  row offset and no row-group selection. Reading a chosen row-group range
  through `polars-parquet` directly would require its deserializer API and a
  direct dependency; that was not adopted in this card.
- Adopted fallback for contiguous pages: `with_columns(projection)` +
  `with_n_rows(offset + limit)` + in-memory `slice(offset, limit)`. For
  arbitrary sorted/filtered ids: read the projected range from the smallest to
  the largest selected id, then `take` the requested order.
- Consequence: a page at a high offset still decodes the projected row groups
  before it, and a random sort that spans the file can read the projected range
  covering its page. No external sort, new database, or full all-column
  `finish()` is used for a page. Latency was not measured here (T27).

### Verification

| Gate | Command | Result |
|---|---|---|
| New page tests | `cargo test -p trivium-engine --test page_read_test` | 4 passed, 0 failed |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, 0 failed |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |
| Frontend lint | `npm run lint` | 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| UI value test | `npm run test:ui` | 4 passed, 0 failed |

### Unverified / deferred

- No wall-clock measurement of a random-sort page or a high-offset page was
  taken. The T27 measurement remains the place for latency and memory results;
  no engine switch was started.
- The native GUI was not exercised.
- Hidden columns are no longer present in page `ProjectRow.data`/`cells`.
  `ExpandedCellDialog` still lists hidden columns and reads `row.cells`, so
  those values will be blank until a dedicated detail/full-cell fetch (T20c).
  This is a UI-visible consequence to verify in the app.
- An unscoped full-text search still needs every data column, and an IOC cache
  miss evaluates against every data column. Both remain column projections of
  all data columns, not the all-column full frame.
- Frontend page-cache limits, long-cell previews, and giant-row navigation were
  not started.

Next card: T20b.

## T20b — bound the frontend page cache

Recorded: 2026-11-11.

Changes:
- `src/lib/pageCache.ts` (new, Svelte-independent): `estimatePageBytes(rows)`
  sums the rendered `displayCache`/`copyCache` text lengths plus a fixed
  per-row overhead, and `selectEvictions(pages, current, maxPages, maxBytes)`
  returns the pages to drop. The current page is protected from count/byte
  eviction; a page whose own estimate exceeds the byte bound is always evicted
  (even when current), which is how oversized pages are detected.
- `src/lib/components/project_view/state.ts`: `PAGE_CACHE_MAX_PAGES = 5` and
  `PAGE_CACHE_MAX_BYTES = 16 * 1024 * 1024` (not exported), a page access
  clock, and `largePages`. `registerLoadedPage` stores a response, updates the
  access order and evicts in one place; `dropPage` removes the page's rows,
  its `positionToRowIndex` entries and its `loadedPages` entry together.
  `touchPage` refreshes the access order for the visible/prefetch pages.
  Page size stays 250 rows. `resetPageCache` is called with the existing
  pagination reset so filter/project changes clear the bookkeeping.
- `src/lib/components/ProjectView.svelte`: the response path calls
  `registerLoadedPage` instead of updating the three collections separately.
  `ensureRangeLoaded` touches the pages in range and does not automatically
  retry a page recorded in `largePages`.
- `src/lib/components/project_view/DataTable.svelte` and
  `project-view.css`: a missing row whose page is in `largePages` shows
  "Page too large to cache…" instead of "Loading…". Other missing rows keep the
  existing loading state and are refetched by `ensureRangeLoaded` because their
  page was removed from `loadedPages`.
- Annotation editing remains independent of the page cache: `memoEditor`
  keeps the original `rowIndex` and its row snapshot, and `saveMemo` uses the
  backend with those values. A comment records that the future T22 selection
  must be a separate store. Eviction never closes the memo dialog or fails a
  save; if the row was evicted, the optimistic cache update is simply skipped.

### Verification

| Gate | Command | Result |
|---|---|---|
| UI tests | `npm run test:ui` | 9 passed, 0 failed (5 new page-cache tests) |
| Frontend lint | `npm run lint` | svelte-check: 0 errors / 0 warnings |
| Frontend build | `npm run build` | success |
| Engine tests | `cargo test -p trivium-engine` | all suites passed, 0 failed |
| Rust size guard | `bash scripts/check_rust_size.sh` | exit 0 |

`package.json` now runs `node --test --experimental-strip-types
src/lib/*.test.ts`, so the existing `ipcValues.test.ts` and the new
`pageCache.test.ts` both run.

### Unverified / deferred

- The native GUI was not exercised, so large scrolling, returning to an evicted
  position, and the "Page too large to cache" notice are only covered by the
  pure-function tests and code inspection. GUI verification is T26.
- The 16MiB bound is an estimate from retained cell strings, not a measured JS
  heap. Actual memory use remains for T27.
- No long-cell preview/full-cell fetch, giant-row navigation, or selection UI
  was added; those are T20c/T20d/T22.

Next card: T20c.

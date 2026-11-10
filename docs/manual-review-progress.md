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

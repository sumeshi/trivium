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

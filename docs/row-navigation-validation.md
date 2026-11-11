# P0: original-row navigation and virtual scrolling

Base: `develop` at `1dc950b2d3d6cf97a4eae64f107516a65b9f16f5`.

## Navigation contract

- Go accepts the original one-based `#`, not a position in the current result.
  It resolves that ID using the same QuerySpec and cached ordering as paging.
- An excluded row produces a notification and leaves the query and scroll
  position intact. Invalid, fractional and out-of-range inputs are rejected.
- First and Last move to the endpoints of the current filtered/sorted result.
- Rendering and page fetching share one virtual-window calculation. Rows remain
  32 pixels high; the physical spacer stays capped at 8,000,000 pixels.
- Lookup reuses cached IDs instead of retaining another whole-result index.
  Source-order lookup does not read source values or materialize pages.

## Reproducing the checks

```sh
npm ci
npm run test:ui
npm run lint
npm run lint:test
npm run build
cargo test --locked -p trivium-engine
cargo clippy --locked -p trivium-engine --lib --test locate_row_test -- -D warnings
bash scripts/check_rust_size.sh

# Requires the usual Tauri Linux development libraries, Xvfb,
# WebKitWebDriver and tauri-driver. The GUI uses embedded frontend assets.
cargo build --locked -p trivium-desktop --features custom-protocol
xvfb-run -a python3 tests/native/row_navigation.py \
  --binary target/debug/trivium-desktop --rows 5199 2000000
```

The native check creates its own temporary input files, project store and XDG
profile. It operates real Go / First / Last buttons and the search/sort controls,
checks DOM geometry and original IDs, and writes evidence under
`target/native-row-navigation`. Import, annotation setup and export use real
Tauri IPC. The response-race check delays delivery of a real native lookup.

## Validation results (2026-11-11 UTC)

Environment: Debian 13 container, GTK/WebKitGTK, Xvfb, Rust 1.99.1,
Node 24.22.1; embedded-assets debug build of the actual Tauri application.

| Check | Result |
| --- | --- |
| Complete engine test suite | 248 passed, 0 failed, 0 ignored |
| UI unit tests, including virtual geometry at 5,199 and 2M rows | 44 passed |
| Svelte and test TypeScript checks | Passed; 0 errors / 0 warnings |
| Frontend build and native Tauri build | Passed |
| Engine library and new lookup test, Clippy `-D warnings` | Passed |
| Rust size guard and `git diff --check` | Passed |

Both real Tauri/WebKit fixtures passed the complete navigation scenario:

| CSV rows | Source-order Go targets (`#`) | Filtered descending result |
| --- | --- | --- |
| 5,199 | 1 / 2,600 / 5,199 | 2,599 rows; Go #3,898, First #5,198, Last #2 |
| 2,000,000 | 1 / 1,000,000 / 2,000,000 | 1,000,000 rows; Go #1,500,000, First #2,000,000, Last #2 |

First and Last were also checked before filtering. Each displayed target was
fully inside the viewport; loaded rows had matching original source values,
unique contiguous IDs in result order, and contiguous 32-pixel bounds covering
the viewport. The 2M spacer remained capped at 8,000,000 pixels. Safe,
Suspicious, Critical and Add memo remained available on the loaded rows.
The 5,199-row scenario also passed Enter, invalid/fractional input, annotation
persistence/CSV export and the held-response/new-query race check. Both sizes
passed excluded-row notifications without changing filters or position.

Final run: `target/native-row-navigation/392f8f35f9fd/report.json` and the
adjacent 2M middle/end screenshots. It recorded 69 real native IPC calls,
zero IPC/JavaScript errors and zero transport retries. Fixtures contain three
columns (108,096 and 46,888,920 input bytes); these are functional checks,
not throughput benchmarks. The harness starts the session via tauri-driver
and then uses that same session's owned WebKitWebDriver port to avoid the
tauri-driver 2.1 HTTP proxy's stale pooled-connection resets.

Windows GUI behavior and a packaged `.deb` installation have not been checked.

## Existing issues found during verification

These are present in the base revision and are outside the P0 navigation patch:

- A scoped text query using a modern CSV's display label (for example
  `cohort:keep`) is not translated to its internal column ID and returns
  `Unknown column: cohort`. The native navigation check uses ordinary `keep`
  text search to select the same even-numbered rows.
- A failed query requested from a nonzero page can leave the loading state
  active. This was exposed by the invalid scoped query above.
- With Rust 1.99.1, all-target Clippy with `-D warnings` stops at the existing
  redundant field and unnecessary struct update in
  `crates/engine/tests/tags_test.rs`.
  The engine library and new lookup test pass the same strict Clippy check.

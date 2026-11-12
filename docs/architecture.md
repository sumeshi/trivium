# Engine and desktop boundaries

Trivium keeps Rust, Polars, Parquet, Tauri and Svelte. Polars provides typed column operations and filtering; the application controls which columns and row groups reach it. SQLite stores small mutable case state. No additional analytical engine, Python runtime or UI framework is embedded.

## Read paths

Import writes bounded Parquet row groups and computes column kinds and sampled widths once. A signature-checked SQLite profile lets desktop opening read only metadata. A legacy case without this profile gets one bounded projected pass; later opens reuse the result. The older engine load API still returns its first 250 rows for existing callers.

Page reads map result positions to original row IDs and decode only intersecting row groups. Sparse reads preserve order and duplicate IDs. Decoded batches are capped at 8,192 rows; this bounds batch row count, not the byte size of an unusually large cell or Parquet row group. Deep-page tests inspect actual compressed bytes and decoded groups rather than counting API calls.

The query resolver shares result IDs between table navigation, analyses, export and bulk preview. Cache entries are bounded, same-key misses share one worker, and an operation captures annotation values and revision together. Annotation changes that affect membership invalidate the corresponding key. Borrowed predicates bind columns once; exact string equality/membership uses Polars kernels, with parity-tested evaluation for the remaining operations. Values retain their original null/missing and timestamp semantics.

Statistics project the selected column and stream the selected rows. Exact distinct counts still require memory proportional to the number of distinct values. Time analysis and histogram point arrays scale with the relevant selected column; duplicate/diff/group results scale with distinct keys or requested output. These costs are explicit, rather than a promise of constant memory for every analysis.

CSV export scans projected rows once. Source-order output streams directly. Arbitrary result order uses bounded CSV runs and a 32-way external merge. Rank mapping has a memory budget and an external fallback. Temporary files are removed on success, cancellation or error, and the destination is finalized only after flush/sync. Export refuses original sources and managed case paths, including filesystem aliases.

## Work and state ownership

All heavy desktop commands enter one async admission queue: two running jobs and at most 32 waiting. Interactive bursts are bounded so background work progresses. Queued jobs remain visible and cancelable. Cooperative checkpoints cover batch reads and manual loops; an individual Polars kernel is not interruptible mid-call.

Each native read has a unique transport request ID. Closing/replacing a panel or workspace cancels its own reads without canceling writes or unrelated windows. Request ownership stays outside query/cache identity. Explicit job cancellation is separate, with transaction rollback before publication.

The active workspace has its own Svelte state, paging/query generation, navigation, persistence, and request owners. Inactive tabs retain compact view/session state rather than data frames or full page caches. Disposal and generation checks prevent late responses from changing another case.

## Persistence and contracts

Each case directory contains immutable evidence and one scoped SQLite database. Annotation patches, counts and durable revision change together. Bulk undo is durable, conflict-checked and streamed through a transaction. See [storage.md](storage.md) for migration guarantees, limits and retained legacy files.

Public wire DTOs generate TypeScript through `export_contracts`; CI checks for drift. UI types refine generated enums and optional legacy fields. Values used as evidence are strings; JSON counts and offsets remain numbers. The JSONL automation adapter calls the same Rust operations rather than duplicating search or annotation logic.

## Verification limits

Linux native tests exercise the embedded Tauri/WebKit application through WebDriver, including workspace races and original-row navigation on 5,199 and 2,000,000 rows. Windows native smoke runs are configured separately and must be assessed from their CI results; a Linux build is not evidence of Windows execution. Benchmarks record the actual host, fixture, sample count and cache conditions in [performance.md](performance.md).

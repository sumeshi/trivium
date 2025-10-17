# Trivium Desktop

Trivium is now a lightning-fast desktop investigator built with **Tauri**, **Svelte**, and **Polars**. Import large CSV logs, tag the records you care about, annotate with memos, and export a clean audit trail — all without leaving your machine or running a background web service.

## Feature Snapshot

- **Native desktop shell** powered by Tauri (Rust) with minimal memory overhead.
- **Polars data core** for CSV → Parquet ingestion, rapid filtering, and high-volume exports.
- **Projects workspace** that stores imported copies, descriptions, hidden-column preferences, and row-level flags.
- **Flag triage** with ◯ / ? / ✗ shortcuts, memo editing, and instant filtering.
- **Column visibility** per project so noisy fields stay hidden until needed.
- **One-click export** to CSV with flags and memos appended for re-import or sharing.

![Trivium Screenshot](screenshot.png)

## Getting Started

```bash
git clone https://github.com/yourusername/trivium.git
cd trivium
npm install
npm run tauri dev   # launches the desktop app with hot reload
```

The Tauri CLI downloads Rust dependencies on first run. Ensure you have a recent Rust toolchain (`rustup` recommended).

### Building a Release Bundle

```bash
npm run tauri build
```

Artifacts will land under `src-tauri/target/` for your platform.

## Project Layout

- `package.json`, `src/` – Svelte + Vite single-page UI.
- `src-tauri/` – Rust commands, Polars data pipeline, and persistence helpers.
  - `src/main.rs` – Project CRUD, CSV ingestion, flag/memo storage, and export routines.
  - `tauri.conf.json` – Window + bundler configuration.

Imported datasets live in the OS-specific app data directory under a `trivium/projects/` folder. Each project keeps a Parquet copy alongside a `flags.json` map; the repository stays clean of user data.

## Commands Overview

| Command | Description |
| --- | --- |
| `npm run dev` | Launch Vite for UI-only development. |
| `npm run tauri dev` | Start the full desktop app with Svelte hot reload + Rust commands. |
| `npm run build` | Produce the static UI bundle consumed by Tauri. |
| `npm run tauri build` | Bundle a distributable desktop binary. |
| `npm run lint` | Type-check Svelte components. |

## Contributing

- Use Conventional Commit prefixes (`feat:`, `fix:`, `chore:`) as in the existing history.
- Run `npm run lint` before opening a PR.
- Screenshots are appreciated when you tweak UI/UX; drop them into the PR body.
- Keep Polars operations in `src-tauri/src/main.rs` fast and immutable where possible; update serialization helpers if you add new data types.

MIT License – see [LICENSE](LICENSE).

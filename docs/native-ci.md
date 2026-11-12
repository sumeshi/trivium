# Reproducible toolchains and native checks

Node is pinned in `.node-version`; Rust is pinned in `rust-toolchain.toml`.
All build/check workflows read these files. CI checks strict engine Clippy
(all targets), engine tests, frontend checks and generated IPC contracts.
Changed Rust files were formatted with the pinned toolchain. A global format
gate is deferred because 37 untouched historical files need formatting; this
change avoids mixing that unrelated cleanup into functional changes.
The develop Debian workflow retains `.deb` and standalone Linux CLI artifacts
for three days. Windows releases include the actual `trivium-desktop.exe`
(renamed for release) and a separate `trivium-cli.exe`.

`Native smoke` builds an embedded-assets Tauri application on Linux and Windows
and drives the real application through `tauri-driver` 2.1.0. The 5,199-row
scenario checks original-number Go, First/Last, filtered descending order,
excluded-row notifications, row geometry, annotation export and stale lookup
responses. Logs, reports and screenshots are retained for three days.

Linux uses Xvfb and WebKitWebDriver. Windows uses msedgedriver selected against
the installed WebView2 runtime; the first three version components must match.
An unavailable compatible driver fails the job explicitly. Both harnesses use
isolated data/config/cache/WebView profiles and clean up their own process tree.
Windows starts the real application explicitly with a test-only localhost remote
debugging port, waits for a genuine WebView2 page, then attaches msedgedriver
using `debuggerAddress`. This is Microsoft's documented WebView2 attach mode;
it avoids implicit `DevToolsActivePort` profile discovery. The selected runtime
folder is pinned to the same version used to download the driver. App startup
output is retained as `application.log`, and an early process exit fails the
test before session creation. Linux retains the existing WebKit launch path.
IPC instrumentation recognizes `ipc://localhost` and `http://ipc.localhost`.

Native CI compiles the dedicated `com.trivium.native-tests` identifier using
`TAURI_CONFIG`. Windows known-folder APIs ignore `APPDATA`/`LOCALAPPDATA` path
overrides, so a separate compiled identifier protects normal case and preference
storage. The debug Windows app rejects automation under the production identifier
before opening its case store; the Python harness also rejects ordinary release
binary paths. A local Windows run must use a dedicated Cargo debug test build:

```powershell
$env:TAURI_CONFIG = '{"identifier":"com.trivium.native-tests"}'
cargo build --locked -p trivium-desktop --features custom-protocol
python tests/native/row_navigation.py --binary target/debug/trivium-desktop.exe --rows 5199
```

Normal builds retain `com.trivium.app` and existing cases. Linux test processes
additionally isolate their data directory with XDG variables. On Windows, test
case/preferences files live under the separate native-test app-data namespace;
the ephemeral GitHub runner discards them after the job.

The first Windows CI run failed before session creation with a missing
`DevToolsActivePort` file. The updated attach mode is covered by local contract
tests, but Windows execution cannot be verified in this Linux cloud environment.
A successful new CI run is required before claiming Windows GUI verification.
YAML parsing and Python compilation validate harness setup only.
Local native Linux results are recorded separately in the current validation
report; these scripts do not replace packaged installation checks.

## Artifact identity

Application packages and the optional automation CLI share one checked version.
Workspace options display the version and source commit embedded by Vite. Each
download artifact includes `BUILD-INFO.json`, per-file `SHA256SUMS` and a short
README identifying GUI versus CLI use. The GitHub Actions artifact ZIP has a
separate digest; compare an extracted `.deb` or `.exe` against the included
`SHA256SUMS`, not against the ZIP digest. See [artifacts.md](artifacts.md).

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
`DevToolsActivePort` file. Explicit attach mode avoids that discovery step, but
has **not** resolved Windows automation: at source revision `cfc0cc5`,
[the Windows job](https://github.com/sumeshi/trivium/actions/runs/43148962620/job/130953355763)
timed out waiting for the localhost debugging endpoint. Its final startup
marker was `state-ready`, with a real app window, six owned WebView2 children
and no logged panic or HRESULT. The 90-byte log was not truncated. App startup
completed; the cause of the unavailable debug endpoint remains unverified.

Timeout diagnostics expose only fixed startup phases, bounded error codes and
owned process names/IDs. Complete application output stays in the native
artifact. Fourteen local Python contract tests and three namespace guard tests
pass, but do not establish Windows GUI operation. A successful Windows native
job is required before claiming that verification. No GPU/runtime workaround
has been added based solely on a timeout.
Local native Linux results are recorded separately in the current validation
report; these scripts do not replace packaged installation checks.

## Artifact identity

Application packages and the optional automation CLI share one checked version.
Workspace options display the version and source commit embedded by Vite. Each
download artifact includes `BUILD-INFO.json`, per-file `SHA256SUMS` and a short
README identifying GUI versus CLI use. The GitHub Actions artifact ZIP has a
separate digest; compare an extracted `.deb` or `.exe` against the included
`SHA256SUMS`, not against the ZIP digest. See [artifacts.md](artifacts.md).

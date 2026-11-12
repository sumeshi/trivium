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
IPC instrumentation recognizes `ipc://localhost` and `http://ipc.localhost`.

The Windows job is configured; it has not been executed in this Linux cloud
environment. A successful CI run is required before claiming Windows GUI
verification. YAML parsing and Python compilation validate harness setup only.
Local native Linux results are recorded separately in the current validation
report; these scripts do not replace packaged installation checks.

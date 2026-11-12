#!/usr/bin/env bash
# Replay the same benchmark against the fixed baseline without editing its
# production engine source. Pass an existing detached 95ee777 worktree.
set -euo pipefail
baseline_dir=${1:?usage: prepare_baseline.sh BASELINE_WORKTREE}
repo_dir=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
expected=$(git -C "$baseline_dir" rev-parse 95ee777^{commit})
actual=$(git -C "$baseline_dir" rev-parse HEAD)
if [ "$actual" != "$expected" ]; then
    printf 'Expected baseline 95ee777, found %s\n' "$actual" >&2
    exit 1
fi
mkdir -p "$baseline_dir/crates/engine/src/bin/architecture_bench"
cp "$repo_dir/crates/engine/src/bin/architecture_bench.rs" "$baseline_dir/crates/engine/src/bin/architecture_bench.rs"
cp "$repo_dir/scripts/bench/baseline_instrumentation.rs" "$baseline_dir/crates/engine/src/bin/architecture_bench/instrumentation.rs"
printf 'Prepared benchmark-only sources in %s\n' "$baseline_dir"

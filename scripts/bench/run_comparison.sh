#!/usr/bin/env bash
# Run only on a quiet machine. Builds finish before timed child processes start.
set -euo pipefail
baseline_dir=${1:?usage: run_comparison.sh BASELINE_WORKTREE OUTPUT_DIR}
output_dir=${2:?usage: run_comparison.sh BASELINE_WORKTREE OUTPUT_DIR}
repo_dir=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
fixture_dir="$output_dir/fixtures"
mkdir -p "$fixture_dir"
for rows in 100000 2000000; do
    python3 "$repo_dir/scripts/gen_fixture.py" --rows "$rows" --seed 42 --out "$fixture_dir/$rows.csv"
done
python3 "$repo_dir/scripts/gen_fixture.py" --rows 100000 --seed 42 --variant long --out "$fixture_dir/long-100k.csv"
"$repo_dir/scripts/bench/prepare_baseline.sh" "$baseline_dir"
bench_target="$output_dir/target"
(
    cd "$baseline_dir"
    CARGO_TARGET_DIR="$bench_target" CARGO_PROFILE_RELEASE_OPT_LEVEL=s cargo build --release --locked -p trivium-engine --bin architecture_bench -j 3
)
cp "$bench_target/release/architecture_bench" "$output_dir/baseline-opt-s"
(
    cd "$repo_dir"
    CARGO_TARGET_DIR="$bench_target" CARGO_PROFILE_RELEASE_OPT_LEVEL=s cargo build --release --locked -p trivium-engine --bin architecture_bench -j 3
)
cp "$bench_target/release/architecture_bench" "$output_dir/current-opt-s"
(
    cd "$repo_dir"
    CARGO_TARGET_DIR="$bench_target" CARGO_PROFILE_RELEASE_OPT_LEVEL=3 cargo build --release --locked -p trivium-engine --bin architecture_bench -j 3
)
cp "$bench_target/release/architecture_bench" "$output_dir/current-opt-3"
# Each fixture is an isolated child, so VmHWM does not carry between fixtures.
for build in baseline-opt-s current-opt-s current-opt-3; do
    TRIVIUM_BENCH_LABEL="$build" "$output_dir/$build" "$fixture_dir/100000.csv" "$fixture_dir/2000000.csv" "$fixture_dir/long-100k.csv" > "$output_dir/$build.log"
done
sha256sum "$fixture_dir/"*.csv "$output_dir/"*opt-* > "$output_dir/sha256.txt"
printf 'Results: %s\n' "$output_dir"

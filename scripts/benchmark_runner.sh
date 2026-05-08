#!/usr/bin/env bash
set -euo pipefail

OXIDE_BIN="${OXIDE_BIN:-./target/release/oxide}"
BENCHMARKER="${BENCHMARKER:-scripts/benchmarker.py}"

# One named telemetry root for this benchmark batch.
# Override with: TELEMETRY_ROOT=/path/to/folder ./run_benches.sh
TELEMETRY_ROOT="${TELEMETRY_ROOT:-benchmark_telemetry/named_benchmark_batch_$(date -u +%Y%m%dT%H%M%SZ)}"

COMMON_ARGS=(
  --oxide-bin "$OXIDE_BIN"
  --worker-modes 16
  --threads 16
  --raw-oxide-presets
  --sync-after-extract
)

mkdir -p "$TELEMETRY_ROOT"

run_bench() {
  local source_dir="$1"
  local passes="$2"
  local name="$3"
  local out_dir="$TELEMETRY_ROOT/$name"

  echo
  echo "============================================================"
  echo "Running benchmark: $name"
  echo "Source: $source_dir"
  echo "Passes: $passes"
  echo "Telemetry: $out_dir"
  echo "============================================================"

  mkdir -p "$out_dir"

  uv run "$BENCHMARKER" \
    --source "$source_dir" \
    --passes "$passes" \
    --telemetry-dir "$out_dir" \
    "${COMMON_ARGS[@]}"
}

run_bench "/datasets/200mb"     30 "dataset_200mb_30_passes"
run_bench "/datasets/6gb/linux"  5 "dataset_6gb_linux_5_passes"
run_bench "/datasets/6gb"        3 "dataset_6gb_3_passes"

echo
echo "All benchmarks finished."
echo "Telemetry saved under: $TELEMETRY_ROOT"
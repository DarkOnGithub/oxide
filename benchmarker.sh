#!/bin/bash

set -euo pipefail

SOURCE="./silesia_corpus"
OXIDE="./target/release/oxide-cli"
OXIDE_OUTPUT="silesia_corpus.oxz"
SQUASHFS_OUTPUT="archive.sqfs"
BENCHMARK_THREADS="${BENCHMARK_THREADS:-16}"
SOURCE_BYTES="$(du -sb "$SOURCE" | cut -f1)"

MODE_COMPRESSION=""
MODE_LEVEL=""
OXIDE_BLOCK_SIZE=""
SQUASHFS_BLOCK_SIZE="1048576"

RESULT_ROWS=()

build_oxide() {
    echo "--- Building Oxide ---"
    cargo build --release -p oxide-cli
}

drop_caches() {
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
}

set_mode_config() {
    local mode=$1

    case $mode in
        "fast")
            MODE_COMPRESSION="lz4"
            MODE_LEVEL=""
            OXIDE_BLOCK_SIZE="1M"
            ;;
        "balanced")
            MODE_COMPRESSION="zstd"
            MODE_LEVEL="6"
            OXIDE_BLOCK_SIZE="1M"
            ;;
        "ultra")
            MODE_COMPRESSION="zstd"
            MODE_LEVEL="19"
            OXIDE_BLOCK_SIZE="1M"
            ;;
        *)
            echo "Unknown mode: $mode" >&2
            exit 1
            ;;
    esac
}

format_bytes() {
    local bytes=$1
    awk -v bytes="$bytes" 'BEGIN {
        split("B KiB MiB GiB TiB", units, " ")
        value = bytes + 0
        unit = 1
        while (value >= 1024 && unit < 5) {
            value /= 1024
            unit++
        }
        if (unit == 1) {
            printf "%d %s", bytes, units[unit]
        } else {
            printf "%.2f %s", value, units[unit]
        }
    }'
}

run_and_record() {
    local tool=$1
    local mode=$2
    local output_path=$3
    shift 3

    local start_ns end_ns elapsed_ns elapsed_s output_bytes throughput

    start_ns=$(date +%s%N)
    "$@"
    end_ns=$(date +%s%N)

    elapsed_ns=$((end_ns - start_ns))
    elapsed_s=$(awk -v ns="$elapsed_ns" 'BEGIN { printf "%.3f", ns / 1000000000 }')
    output_bytes=$(stat -c%s "$output_path")
    throughput=$(awk -v bytes="$SOURCE_BYTES" -v ns="$elapsed_ns" 'BEGIN {
        secs = ns / 1000000000
        if (secs <= 0) {
            printf "0.00"
        } else {
            printf "%.2f", (bytes / 1024 / 1024) / secs
        }
    }')

    RESULT_ROWS+=("$tool|$mode|$elapsed_s|$throughput|$output_bytes")

    rm -f "$output_path"
}

print_results_table() {
    local row tool mode elapsed throughput output_bytes ratio

    echo "=============================================================="
    echo " PERFORMANCE SUMMARY"
    echo "=============================================================="
    printf "%-12s %-10s %10s %14s %14s %8s\n" "tool" "mode" "seconds" "MiB/s" "output" "ratio"

    for row in "${RESULT_ROWS[@]}"; do
        IFS='|' read -r tool mode elapsed throughput output_bytes <<< "$row"
        ratio=$(awk -v out="$output_bytes" -v inb="$SOURCE_BYTES" 'BEGIN {
            if (inb <= 0) {
                printf "0.000"
            } else {
                printf "%.3f", out / inb
            }
        }')

        printf "%-12s %-10s %10s %14s %14s %8s\n" \
            "$tool" \
            "$mode" \
            "$elapsed" \
            "$throughput" \
            "$(format_bytes "$output_bytes")" \
            "$ratio"
    done
}

run_oxide() {
    local mode=$1

    set_mode_config "$mode"

    run_and_record "oxide" "$mode" "$OXIDE_OUTPUT" \
        "$OXIDE" archive "$SOURCE" \
        --output "$OXIDE_OUTPUT" \
        --preset "$mode" \
        --block-size "$OXIDE_BLOCK_SIZE" \
        --workers "$BENCHMARK_THREADS"
}

run_mksquashfs() {
    local mode=$1

    set_mode_config "$mode"

    if [[ -n "$MODE_LEVEL" ]]; then
        run_and_record "mksquashfs" "$mode" "$SQUASHFS_OUTPUT" \
            mksquashfs "$SOURCE" "$SQUASHFS_OUTPUT" \
            -comp "$MODE_COMPRESSION" \
            -Xcompression-level "$MODE_LEVEL" \
            -b "$SQUASHFS_BLOCK_SIZE" \
            -processors "$BENCHMARK_THREADS"
    else
        run_and_record "mksquashfs" "$mode" "$SQUASHFS_OUTPUT" \
            mksquashfs "$SOURCE" "$SQUASHFS_OUTPUT" \
            -comp "$MODE_COMPRESSION" \
            -b "$SQUASHFS_BLOCK_SIZE" \
            -processors "$BENCHMARK_THREADS"
    fi
}

run_gensquashfs() {
    local mode=$1

    set_mode_config "$mode"

    if [[ -n "$MODE_LEVEL" ]]; then
        run_and_record "gensquashfs" "$mode" "$SQUASHFS_OUTPUT" \
            gensquashfs "$SQUASHFS_OUTPUT" \
            -j "$BENCHMARK_THREADS" \
            -q \
            -c "$MODE_COMPRESSION" \
            -X "level=$MODE_LEVEL" \
            -b "$SQUASHFS_BLOCK_SIZE" \
            -D "$SOURCE" \
            -f
    else
        run_and_record "gensquashfs" "$mode" "$SQUASHFS_OUTPUT" \
            gensquashfs "$SQUASHFS_OUTPUT" \
            -j "$BENCHMARK_THREADS" \
            -q \
            -c "$MODE_COMPRESSION" \
            -b "$SQUASHFS_BLOCK_SIZE" \
            -D "$SOURCE" \
            -f
    fi
}

run_bench() {
    local mode=$1
    echo "======================================================"
    echo " TESTING MODE: $mode "
    echo "======================================================"

    drop_caches

    echo "--- Oxide ($mode) ---"
    run_oxide "$mode"
    echo ""

    drop_caches

    echo "--- mksquashfs ($mode equivalent) ---"
    run_mksquashfs "$mode"
    echo ""

    drop_caches

    echo "--- gensquashfs ($mode equivalent) ---"
    run_gensquashfs "$mode"
    echo ""
}

build_oxide

for mode in "fast" "balanced" "ultra"; do
    run_bench "$mode"
done

print_results_table

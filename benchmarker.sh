#!/bin/bash

set -euo pipefail

SOURCE="./silesia_corpus"
OXIDE="./target/release/oxide-cli"
OXIDE_OUTPUT="silesia_corpus.oxz"
SQUASHFS_OUTPUT="archive.sqfs"
MKSQFS_PROCESSORS="$(nproc)"

build_oxide() {
    echo "--- Building Oxide ---"
    cargo build --release -p oxide-cli
}

drop_caches() {
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
}

run_oxide() {
    local mode=$1
    time "$OXIDE" archive "$SOURCE" --preset "$mode"
    rm -f "$OXIDE_OUTPUT"
}

run_mksquashfs() {
    local mode=$1

    case $mode in
        "fast")
            time mksquashfs "$SOURCE" "$SQUASHFS_OUTPUT" \
                -comp lz4 \
                -b 1048576 \
                -processors "$MKSQFS_PROCESSORS"
            ;;
        "balanced")
            time mksquashfs "$SOURCE" "$SQUASHFS_OUTPUT" \
                -comp zstd \
                -Xcompression-level 6 \
                -b 1048576 \
                -processors "$MKSQFS_PROCESSORS"
            ;;
        "ultra")
            time mksquashfs "$SOURCE" "$SQUASHFS_OUTPUT" \
                -comp zstd \
                -Xcompression-level 19 \
                -b 1048576 \
                -processors "$MKSQFS_PROCESSORS"
            ;;
    esac

    rm -f "$SQUASHFS_OUTPUT"
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
}

build_oxide

for mode in "fast" "balanced" "ultra"; do
    run_bench "$mode"
done

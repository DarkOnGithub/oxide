#!/bin/bash

SOURCE="./datasets"
OXIDE="./target/release/oxide-cli"

run_bench() {
    echo "--- Testing $1 ---"
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    
    if [ "$1" == "oxide" ]; then
        time $OXIDE archive $SOURCE --preset fast
        rm datasets.oxz
    else
        time mksquashfs $SOURCE archive.sqfs -comp lz4 -processors $(nproc)
        rm archive.sqfs
    fi
    echo ""
}

run_bench "oxide"
run_bench "mksquashfs"
#!/usr/bin/env python3
"""Count duplicate blocks in an Oxide OXZ archive.

Two modes are supported:
- payload: exact duplicate stored block bytes (default)
- raw: duplicate raw blocks using (raw_len, checksum) from the chunk table
"""

from __future__ import annotations

import argparse
import hashlib
import struct
from collections import Counter
from pathlib import Path


FOOTER_SIZE = 40
CHUNK_DESCRIPTOR_SIZE = 14
GLOBAL_HEADER_SIZE = 8


def read_footer(f) -> tuple[int, int, int]:
    f.seek(0, 2)
    file_size = f.tell()
    if file_size < FOOTER_SIZE + GLOBAL_HEADER_SIZE:
        raise ValueError("file too small to be a valid OXZ archive")

    f.seek(file_size - FOOTER_SIZE)
    footer = f.read(FOOTER_SIZE)
    (
        magic,
        version,
        flags,
        block_count,
        _entry_off,
        _entry_len,
        chunk_off,
        chunk_len,
        _crc,
    ) = struct.unpack("<4sHHIQIQII", footer)

    if magic != b"END\0":
        raise ValueError("invalid OXZ footer magic")
    if version != 2:
        raise ValueError(f"unsupported OXZ footer version: {version}")
    if flags != 0:
        raise ValueError(f"unsupported OXZ footer flags: {flags}")

    return block_count, chunk_off, chunk_len


def parse_descriptors(table: bytes, block_count: int):
    if len(table) != block_count * CHUNK_DESCRIPTOR_SIZE:
        raise ValueError("chunk table length does not match block count")

    for i in range(block_count):
        off = i * CHUNK_DESCRIPTOR_SIZE
        encoded_len, raw_len, checksum, _flags, _dict_id = struct.unpack_from(
            "<III2B", table, off
        )
        yield encoded_len, raw_len, checksum


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("archive", type=Path, help="path to an .oxz archive")
    ap.add_argument(
        "--mode",
        choices=("payload", "raw"),
        default="payload",
        help="payload = exact stored bytes, raw = (raw_len, checksum) fingerprint",
    )
    args = ap.parse_args()

    with args.archive.open("rb") as f:
        block_count, chunk_off, chunk_len = read_footer(f)

        f.seek(chunk_off)
        table = f.read(chunk_len)
        descs = list(parse_descriptors(table, block_count))

        if args.mode == "raw":
            counts = Counter(
                (raw_len, checksum) for _encoded_len, raw_len, checksum in descs
            )
        else:
            counts = Counter()
            payload_off = GLOBAL_HEADER_SIZE
            for encoded_len, _raw_len, _checksum in descs:
                f.seek(payload_off)
                payload = f.read(encoded_len)
                if len(payload) != encoded_len:
                    raise ValueError("archive ended while reading a block payload")
                counts[(encoded_len, hashlib.sha256(payload).digest())] += 1
                payload_off += encoded_len

    dup_groups = sum(1 for n in counts.values() if n > 1)
    dup_blocks = sum(n - 1 for n in counts.values() if n > 1)

    print(f"blocks: {block_count}")
    print(f"duplicate groups: {dup_groups}")
    print(f"duplicate blocks: {dup_blocks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

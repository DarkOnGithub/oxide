#!/usr/bin/env python3
"""Analyze Oxide OXZ archives.

Parses OXZ structures using only the Python standard library and reports:
- Section sizes
- Chunk-table statistics
- Manifest statistics
"""

from __future__ import annotations

import argparse
import json
import os
import struct
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Tuple


OXZ_MAGIC = b"OXZ\x00"
OXZ_END_MAGIC = b"END\x00"
SUPPORTED_OXZ_VERSIONS = {2, 3}
FOOTER_VERSION = 2
GLOBAL_HEADER_SIZE = 8
FOOTER_SIZE = 40
CHUNK_DESCRIPTOR_SIZE = 14

DEFAULT_CHUNK_DETAIL_LIMIT = 24
DEFAULT_MANIFEST_ENTRY_DETAIL_LIMIT = 12
TOP_FILE_ANALYSIS_LIMIT = 10
NEAR_RAW_RATIO_THRESHOLD = 0.98

MANIFEST_MAGIC = b"OXM2"
SUPPORTED_MANIFEST_VERSIONS = {1, 2}

COMPRESSION_FLAG_REFERENCE = 1 << 4
RAW_PASSTHROUGH_FLAG = 1 << 3

ENTRY_FLAG_DIRECTORY = 1 << 0
ENTRY_FLAG_SYMLINK = 1 << 1
ENTRY_FLAG_MODE_PRESENT = 1 << 2
ENTRY_FLAG_UID_PRESENT = 1 << 3
ENTRY_FLAG_GID_PRESENT = 1 << 4
ENTRY_FLAG_MTIME_SECONDS_PRESENT = 1 << 5
ENTRY_FLAG_MTIME_NANOS_PRESENT = 1 << 6
ENTRY_FLAG_RESERVED_MASK = 1 << 7


class FormatError(ValueError):
    """Raised when an OXZ archive is invalid."""


@dataclass(frozen=True)
class HeaderPrefix:
    magic: bytes
    version: int
    flags: int


@dataclass(frozen=True)
class Footer:
    end_magic: bytes
    version: int
    flags: int
    block_count: int
    entry_table_offset: int
    entry_table_len: int
    chunk_table_offset: int
    chunk_table_len: int
    global_crc32: int


@dataclass(frozen=True)
class ChunkDescriptor:
    encoded_len: int
    raw_len: int
    raw_offset: int
    raw_end: int
    checksum_or_target: int
    compression_flags: int
    dictionary_id: int
    is_reference: bool
    algorithm: str | None
    raw_passthrough: bool


def _decode_varint(data: bytes, cursor: int) -> Tuple[int, int]:
    shift = 0
    value = 0
    while True:
        if cursor >= len(data):
            raise FormatError("truncated manifest varint")
        if shift >= 64:
            raise FormatError("manifest varint overflow")
        byte = data[cursor]
        cursor += 1
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            return value, cursor
        shift += 7


def _varint_size(value: int) -> int:
    if value < 0:
        raise ValueError("varint value must be non-negative")
    size = 1
    while value >= 0x80:
        value >>= 7
        size += 1
    return size


def _zigzag_decode_i64(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


def _decode_compression_algo(flags: int) -> str:
    selector = flags & 0b111
    if selector == 0b001:
        return "lz4"
    if selector == 0b010:
        return "zstd"
    if selector == 0b011:
        return "lzma"
    raise FormatError(f"invalid compression flags algorithm selector: {selector:#05b}")


def _parse_header_prefix(data: bytes) -> HeaderPrefix:
    if len(data) != GLOBAL_HEADER_SIZE:
        raise FormatError("global header prefix size mismatch")
    magic = data[:4]
    version, flags = struct.unpack_from("<HH", data, 4)
    if magic != OXZ_MAGIC:
        raise FormatError("invalid OXZ magic")
    if version not in SUPPORTED_OXZ_VERSIONS:
        raise FormatError(f"unsupported OXZ version: {version}")
    if flags & ~0x001F:
        raise FormatError("unsupported OXZ header flags")
    return HeaderPrefix(magic=magic, version=version, flags=flags)


def _parse_footer(data: bytes) -> Footer:
    if len(data) != FOOTER_SIZE:
        raise FormatError("footer size mismatch")
    end_magic = data[:4]
    if end_magic != OXZ_END_MAGIC:
        raise FormatError("invalid OXZ footer magic")
    version, flags = struct.unpack_from("<HH", data, 4)
    if version != FOOTER_VERSION:
        raise FormatError(f"unsupported OXZ footer version: {version}")
    if flags != 0:
        raise FormatError("invalid OXZ footer flags")
    (
        block_count,
        entry_table_offset,
        entry_table_len,
        chunk_table_offset,
        chunk_table_len,
        global_crc32,
    ) = struct.unpack_from("<IQIQI", data, 8) + (struct.unpack_from("<I", data, 36)[0],)
    return Footer(
        end_magic=end_magic,
        version=version,
        flags=flags,
        block_count=block_count,
        entry_table_offset=entry_table_offset,
        entry_table_len=entry_table_len,
        chunk_table_offset=chunk_table_offset,
        chunk_table_len=chunk_table_len,
        global_crc32=global_crc32,
    )


def _parse_chunk_table(
    table: bytes, block_count: int, payload_offset: int
) -> Dict[str, object]:
    expected_len = block_count * CHUNK_DESCRIPTOR_SIZE
    if len(table) != expected_len:
        raise FormatError("chunk table length does not match block count")

    descriptors: List[ChunkDescriptor] = []
    parts: List[Dict[str, object]] = []
    next_payload_offset = payload_offset
    total_raw_bytes = 0
    total_encoded_bytes = 0
    reference_block_count = 0
    raw_passthrough_block_count = 0
    near_raw_block_count = 0
    algo_counts: Counter[str] = Counter()
    dictionary_id_counts: Counter[str] = Counter()
    next_raw_offset = 0

    for i in range(block_count):
        base = i * CHUNK_DESCRIPTOR_SIZE
        encoded_len, raw_len, checksum_or_target, compression_flags, dictionary_id = (
            struct.unpack_from("<III2B", table, base)
        )
        is_reference = (compression_flags & COMPRESSION_FLAG_REFERENCE) != 0
        raw_passthrough = False
        algorithm: str | None = None

        if is_reference:
            if compression_flags & ~COMPRESSION_FLAG_REFERENCE:
                raise FormatError("reference chunk uses unsupported compression flags")
            if encoded_len != 0:
                raise FormatError("reference chunk descriptor must have encoded_len=0")
            if dictionary_id != 0:
                raise FormatError(
                    "reference chunk descriptor must have dictionary_id=0"
                )
            if checksum_or_target == 0xFFFFFFFF:
                raise FormatError("reference chunk target index is invalid")
            reference_block_count += 1
        else:
            if compression_flags & 0b1111_0000:
                raise FormatError("invalid compression flags reserved bits")
            algorithm = _decode_compression_algo(compression_flags)
            raw_passthrough = (compression_flags & RAW_PASSTHROUGH_FLAG) != 0
            if raw_passthrough and dictionary_id != 0:
                raise FormatError(
                    "raw chunk descriptor must not reference dictionaries"
                )
            if algorithm != "zstd" and dictionary_id != 0:
                raise FormatError(
                    "only zstd chunk descriptors may reference dictionaries"
                )
            algo_counts[algorithm] += 1
            if dictionary_id != 0:
                dictionary_id_counts[str(dictionary_id)] += 1
            total_encoded_bytes += encoded_len
            if raw_passthrough:
                raw_passthrough_block_count += 1
            elif raw_len > 0 and (encoded_len / raw_len) >= NEAR_RAW_RATIO_THRESHOLD:
                near_raw_block_count += 1

        raw_offset = next_raw_offset
        raw_end = raw_offset + raw_len
        next_raw_offset = raw_end
        total_raw_bytes += raw_len
        next_payload_offset += encoded_len
        descriptors.append(
            ChunkDescriptor(
                encoded_len=encoded_len,
                raw_len=raw_len,
                raw_offset=raw_offset,
                raw_end=raw_end,
                checksum_or_target=checksum_or_target,
                compression_flags=compression_flags,
                dictionary_id=dictionary_id,
                is_reference=is_reference,
                algorithm=algorithm,
                raw_passthrough=raw_passthrough,
            )
        )
        parts.append(
            {
                "index": i,
                "name": f"chunk[{i}]",
                "size": CHUNK_DESCRIPTOR_SIZE,
                "offset": base,
                "payload_offset": next_payload_offset - encoded_len,
                "payload_end": next_payload_offset,
                "raw_offset": raw_offset,
                "raw_end": raw_end,
                "encoded_len": encoded_len,
                "raw_len": raw_len,
                "compression_ratio": (encoded_len / raw_len) if raw_len else None,
                "checksum_or_target": checksum_or_target,
                "compression_flags": compression_flags,
                "dictionary_id": dictionary_id,
                "is_reference": is_reference,
                "algorithm": algorithm,
                "raw_passthrough": raw_passthrough,
            }
        )

    return {
        "descriptors": descriptors,
        "parts": parts,
        "payload_end": next_payload_offset,
        "stats": {
            "block_count": block_count,
            "total_raw_bytes": total_raw_bytes,
            "total_encoded_bytes": total_encoded_bytes,
            "compression_ratio": (total_encoded_bytes / total_raw_bytes)
            if total_raw_bytes
            else None,
            "reference_block_count": reference_block_count,
            "raw_passthrough_block_count": raw_passthrough_block_count,
            "near_raw_block_count": near_raw_block_count,
            "per_algorithm_counts": dict(sorted(algo_counts.items())),
            "dictionary_id_counts": dict(
                sorted(dictionary_id_counts.items(), key=lambda x: int(x[0]))
            ),
        },
    }


def _decode_manifest(manifest_bytes: bytes) -> Dict[str, object]:
    cursor = 0
    if len(manifest_bytes) < len(MANIFEST_MAGIC) + 1:
        raise FormatError("archive manifest is truncated")
    if manifest_bytes[:4] != MANIFEST_MAGIC:
        raise FormatError("invalid archive manifest magic")

    parts: List[Dict[str, object]] = [
        {"name": "manifest_magic", "offset": 0, "size": 4},
        {
            "name": "manifest_version",
            "offset": 4,
            "size": 1,
            "value": manifest_bytes[4],
        },
    ]
    cursor = 5
    version = manifest_bytes[4]
    if version not in SUPPORTED_MANIFEST_VERSIONS:
        raise FormatError(f"unsupported archive manifest version: {version}")

    dictionary_count_start = cursor
    dictionary_count_u64, cursor = _decode_varint(manifest_bytes, cursor)
    dictionary_count = int(dictionary_count_u64)
    parts.append(
        {
            "name": "dictionary_count_varint",
            "offset": dictionary_count_start,
            "size": cursor - dictionary_count_start,
            "value": dictionary_count,
        }
    )

    dictionaries_total_bytes = 0
    dictionary_records: List[Dict[str, object]] = []

    for index in range(dictionary_count):
        record_start = cursor
        if cursor + 2 > len(manifest_bytes):
            raise FormatError("truncated archive dictionary header")
        dictionary_id = manifest_bytes[cursor]
        algo_flags = manifest_bytes[cursor + 1]
        _ = _decode_compression_algo(algo_flags)
        cursor += 2

        if cursor >= len(manifest_bytes):
            raise FormatError("truncated archive dictionary class")
        class_id = manifest_bytes[cursor]
        cursor += 1
        if class_id in (1, 2, 3):
            pass
        elif class_id == 4:
            ext_len_start = cursor
            ext_len, cursor = _decode_varint(manifest_bytes, cursor)
            ext_end = cursor + int(ext_len)
            if ext_end > len(manifest_bytes):
                raise FormatError("truncated archive dictionary extension")
            try:
                manifest_bytes[cursor:ext_end].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise FormatError("archive dictionary extension is not utf8") from exc
            cursor = ext_end
        else:
            raise FormatError("invalid archive dictionary class id")

        dictionary_len_start = cursor
        dictionary_len, cursor = _decode_varint(manifest_bytes, cursor)
        dictionary_len_varint_bytes = cursor - dictionary_len_start
        dictionary_end = cursor + int(dictionary_len)
        if dictionary_end > len(manifest_bytes):
            raise FormatError("truncated archive dictionary bytes")
        dictionaries_total_bytes += int(dictionary_len)
        cursor = dictionary_end

        record_size = cursor - record_start
        parts.append(
            {
                "name": f"dictionary[{index}]",
                "offset": record_start,
                "size": record_size,
                "dictionary_bytes": int(dictionary_len),
            }
        )
        dictionary_records.append(
            {
                "index": index,
                "offset": record_start,
                "size": record_size,
                "id": dictionary_id,
                "compression_flags": algo_flags,
                "class_id": class_id,
                "dictionary_bytes": int(dictionary_len),
                "dictionary_len_varint_bytes": dictionary_len_varint_bytes,
            }
        )

    entry_count_start = cursor
    entry_count_u64, cursor = _decode_varint(manifest_bytes, cursor)
    entry_count = int(entry_count_u64)
    parts.append(
        {
            "name": "entry_count_varint",
            "offset": entry_count_start,
            "size": cursor - entry_count_start,
            "value": entry_count,
        }
    )

    previous_path = b""
    previous_mode = 0
    previous_uid = 0
    previous_gid = 0
    previous_mtime_seconds = 0
    previous_mtime_nanoseconds = 0

    file_count = 0
    directory_count = 0
    symlink_count = 0
    total_file_payload_bytes = 0
    entry_records: List[Dict[str, object]] = []
    file_records: List[Dict[str, object]] = []
    next_content_offset = 0

    for index in range(entry_count):
        entry_start = cursor
        if cursor >= len(manifest_bytes):
            raise FormatError("truncated archive manifest entry flags")
        flags = manifest_bytes[cursor]
        cursor += 1
        if flags & ENTRY_FLAG_RESERVED_MASK:
            raise FormatError("invalid archive manifest entry flags")

        prefix_start = cursor
        prefix_len_u64, cursor = _decode_varint(manifest_bytes, cursor)
        prefix_len = int(prefix_len_u64)
        if prefix_len > len(previous_path):
            raise FormatError("manifest path prefix exceeds previous path length")
        prefix_size = cursor - prefix_start

        suffix_start = cursor
        suffix_len_u64, cursor = _decode_varint(manifest_bytes, cursor)
        suffix_len = int(suffix_len_u64)
        suffix_data_start = cursor
        suffix_end = cursor + suffix_len
        if suffix_end > len(manifest_bytes):
            raise FormatError("truncated archive manifest path data")
        suffix_size = suffix_len

        path_bytes = previous_path[:prefix_len] + manifest_bytes[cursor:suffix_end]
        cursor = suffix_end
        try:
            _path = path_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise FormatError("archive manifest path is not utf8") from exc

        kind_bits = flags & (ENTRY_FLAG_DIRECTORY | ENTRY_FLAG_SYMLINK)
        if kind_bits == 0:
            kind = "file"
        elif kind_bits == ENTRY_FLAG_DIRECTORY:
            kind = "directory"
        elif kind_bits == ENTRY_FLAG_SYMLINK:
            kind = "symlink"
        else:
            raise FormatError("invalid archive manifest entry kind flags")

        target_size = 0
        target_varint_bytes = 0
        if kind == "symlink":
            target_len_start = cursor
            target_len_u64, cursor = _decode_varint(manifest_bytes, cursor)
            target_len = int(target_len_u64)
            target_end = cursor + target_len
            if target_end > len(manifest_bytes):
                raise FormatError("truncated archive manifest target data")
            try:
                manifest_bytes[cursor:target_end].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise FormatError(
                    "archive manifest symlink target is not utf8"
                ) from exc
            cursor = target_end
            target_varint_bytes = cursor - target_len_start
            target_size = target_len

        size = 0
        size_field_bytes = 0
        if kind == "file":
            size_start = cursor
            size_u64, cursor = _decode_varint(manifest_bytes, cursor)
            size = int(size_u64)
            size_field_bytes = cursor - size_start

        mode = previous_mode
        uid = previous_uid
        gid = previous_gid
        mtime_seconds = previous_mtime_seconds
        mtime_nanoseconds = previous_mtime_nanoseconds
        metadata_parts: Dict[str, int] = {}

        if flags & ENTRY_FLAG_MODE_PRESENT:
            field_start = cursor
            mode_u64, cursor = _decode_varint(manifest_bytes, cursor)
            mode = int(mode_u64)
            if mode > 0xFFFFFFFF:
                raise FormatError("manifest mode exceeds u32 range")
            metadata_parts["mode_bytes"] = cursor - field_start
        if flags & ENTRY_FLAG_UID_PRESENT:
            field_start = cursor
            uid_u64, cursor = _decode_varint(manifest_bytes, cursor)
            uid = int(uid_u64)
            if uid > 0xFFFFFFFF:
                raise FormatError("manifest uid exceeds u32 range")
            metadata_parts["uid_bytes"] = cursor - field_start
        if flags & ENTRY_FLAG_GID_PRESENT:
            field_start = cursor
            gid_u64, cursor = _decode_varint(manifest_bytes, cursor)
            gid = int(gid_u64)
            if gid > 0xFFFFFFFF:
                raise FormatError("manifest gid exceeds u32 range")
            metadata_parts["gid_bytes"] = cursor - field_start
        if flags & ENTRY_FLAG_MTIME_SECONDS_PRESENT:
            field_start = cursor
            delta_u64, cursor = _decode_varint(manifest_bytes, cursor)
            mtime_seconds = previous_mtime_seconds + _zigzag_decode_i64(delta_u64)
            metadata_parts["mtime_seconds_bytes"] = cursor - field_start
        if flags & ENTRY_FLAG_MTIME_NANOS_PRESENT:
            field_start = cursor
            nanos_u64, cursor = _decode_varint(manifest_bytes, cursor)
            mtime_nanoseconds = int(nanos_u64)
            if mtime_nanoseconds >= 1_000_000_000:
                raise FormatError("archive manifest mtime nanoseconds out of range")
            metadata_parts["mtime_nanoseconds_bytes"] = cursor - field_start

        if kind == "directory":
            directory_count += 1
            if size != 0:
                raise FormatError("directory manifest entries must have size=0")
            content_offset = 0
            content_end = 0
        elif kind == "symlink":
            symlink_count += 1
            if size != 0:
                raise FormatError("symlink manifest entries must have size=0")
            content_offset = 0
            content_end = 0
        else:
            file_count += 1
            total_file_payload_bytes += size
            content_offset = next_content_offset
            next_content_offset += size
            content_end = next_content_offset
            file_records.append(
                {
                    "index": index,
                    "path": _path,
                    "size": size,
                    "content_offset": content_offset,
                    "content_end": content_end,
                }
            )

        entry_size = cursor - entry_start
        parts.append(
            {
                "name": f"entry[{index}]",
                "offset": entry_start,
                "size": entry_size,
                "kind": kind,
            }
        )
        entry_records.append(
            {
                "index": index,
                "offset": entry_start,
                "size": entry_size,
                "path": _path,
                "kind": kind,
                "content_offset": content_offset,
                "content_end": content_end,
                "entry_payload_size": size,
                "flags": flags,
                "prefix_varint_bytes": prefix_size,
                "suffix_varint_bytes": suffix_data_start - suffix_start,
                "suffix_bytes": suffix_size,
                "path_bytes": len(path_bytes),
                "target_bytes": target_size,
                "target_varint_bytes": target_varint_bytes,
                "size_field_bytes": size_field_bytes,
                "metadata_bytes": sum(metadata_parts.values()),
                "metadata_parts": metadata_parts,
            }
        )

        previous_path = path_bytes
        previous_mode = mode
        previous_uid = uid
        previous_gid = gid
        previous_mtime_seconds = mtime_seconds
        previous_mtime_nanoseconds = mtime_nanoseconds

    if cursor != len(manifest_bytes):
        raise FormatError("archive manifest has trailing data")

    parts.append(
        {
            "name": "manifest_trailing",
            "offset": cursor,
            "size": len(manifest_bytes) - cursor,
        }
    )

    return {
        "entry_count": entry_count,
        "file_count": file_count,
        "directory_count": directory_count,
        "symlink_count": symlink_count,
        "total_file_payload_bytes": total_file_payload_bytes,
        "dictionary_bank_count": dictionary_count,
        "total_dictionary_bytes": dictionaries_total_bytes,
        "files": file_records,
        "layout": {
            "total_bytes": len(manifest_bytes),
            "parts": parts,
            "dictionaries": dictionary_records,
            "entries": entry_records,
        },
    }


def _build_file_analysis(
    manifest_stats: Dict[str, object], chunk_result: Dict[str, object]
) -> Dict[str, object]:
    files = manifest_stats["files"]
    chunks = chunk_result["parts"]
    if not files:
        return {
            "file_count": 0,
            "largest_estimated_encoded_files": [],
            "worst_estimated_ratio_files": [],
            "raw_passthrough_dominated_files": [],
        }

    chunk_index = 0
    file_rows: List[Dict[str, object]] = []

    for file_record in files:
        start = int(file_record["content_offset"])
        end = int(file_record["content_end"])
        size = int(file_record["size"])

        while (
            chunk_index < len(chunks) and int(chunks[chunk_index]["raw_end"]) <= start
        ):
            chunk_index += 1

        estimated_encoded_bytes = 0.0
        chunk_count = 0
        raw_passthrough_overlap_bytes = 0
        reference_overlap_bytes = 0
        near_raw_overlap_bytes = 0
        algorithm_bytes: Counter[str] = Counter()

        cursor = chunk_index
        while cursor < len(chunks) and int(chunks[cursor]["raw_offset"]) < end:
            chunk = chunks[cursor]
            overlap_start = max(start, int(chunk["raw_offset"]))
            overlap_end = min(end, int(chunk["raw_end"]))
            overlap = overlap_end - overlap_start
            if overlap > 0:
                chunk_count += 1
                if chunk["is_reference"]:
                    reference_overlap_bytes += overlap
                else:
                    raw_len = int(chunk["raw_len"])
                    encoded_len = int(chunk["encoded_len"])
                    if raw_len > 0:
                        estimated_encoded_bytes += encoded_len * (overlap / raw_len)
                    if chunk["raw_passthrough"]:
                        raw_passthrough_overlap_bytes += overlap
                    elif (
                        raw_len > 0
                        and (encoded_len / raw_len) >= NEAR_RAW_RATIO_THRESHOLD
                    ):
                        near_raw_overlap_bytes += overlap
                    algorithm = chunk.get("algorithm")
                    if algorithm:
                        algorithm_bytes[str(algorithm)] += overlap
            cursor += 1

        estimated_ratio = (estimated_encoded_bytes / size) if size else None
        file_rows.append(
            {
                "path": file_record["path"],
                "size": size,
                "chunk_count": chunk_count,
                "estimated_encoded_bytes": round(estimated_encoded_bytes),
                "estimated_ratio": estimated_ratio,
                "raw_passthrough_overlap_bytes": raw_passthrough_overlap_bytes,
                "near_raw_overlap_bytes": near_raw_overlap_bytes,
                "reference_overlap_bytes": reference_overlap_bytes,
                "dominant_algorithms": dict(sorted(algorithm_bytes.items())),
            }
        )

    largest_estimated_encoded_files = sorted(
        file_rows,
        key=lambda row: (int(row["estimated_encoded_bytes"]), int(row["size"])),
        reverse=True,
    )[:TOP_FILE_ANALYSIS_LIMIT]
    worst_estimated_ratio_files = sorted(
        [row for row in file_rows if row["estimated_ratio"] is not None],
        key=lambda row: (float(row["estimated_ratio"]), int(row["size"])),
        reverse=True,
    )[:TOP_FILE_ANALYSIS_LIMIT]
    raw_passthrough_dominated_files = sorted(
        [
            row
            for row in file_rows
            if int(row["raw_passthrough_overlap_bytes"]) > 0
            or int(row["near_raw_overlap_bytes"]) > 0
        ],
        key=lambda row: (
            int(row["raw_passthrough_overlap_bytes"])
            + int(row["near_raw_overlap_bytes"]),
            int(row["size"]),
        ),
        reverse=True,
    )[:TOP_FILE_ANALYSIS_LIMIT]

    return {
        "file_count": len(file_rows),
        "largest_estimated_encoded_files": largest_estimated_encoded_files,
        "worst_estimated_ratio_files": worst_estimated_ratio_files,
        "raw_passthrough_dominated_files": raw_passthrough_dominated_files,
    }


def analyze_archive(path: str) -> Dict[str, object]:
    with open(path, "rb") as f:
        data = f.read()

    file_size = len(data)
    if file_size < GLOBAL_HEADER_SIZE + FOOTER_SIZE:
        raise FormatError("archive is too short")

    header = _parse_header_prefix(data[:GLOBAL_HEADER_SIZE])
    footer_offset = file_size - FOOTER_SIZE
    footer = _parse_footer(data[footer_offset:file_size])

    payload_offset = GLOBAL_HEADER_SIZE
    entry_table_offset = footer.entry_table_offset
    entry_table_len = footer.entry_table_len
    chunk_table_offset = footer.chunk_table_offset
    chunk_table_len = footer.chunk_table_len

    if entry_table_offset < payload_offset:
        raise FormatError("manifest offset overlaps payload prefix")
    entry_table_end = entry_table_offset + entry_table_len
    if entry_table_end != chunk_table_offset:
        raise FormatError("manifest and chunk table must be contiguous")
    chunk_table_end = chunk_table_offset + chunk_table_len
    if chunk_table_end != footer_offset:
        raise FormatError("chunk table must end at footer")
    if footer_offset + FOOTER_SIZE != file_size:
        raise FormatError("archive length does not match declared footer offset")

    payload_size = entry_table_offset - payload_offset
    if chunk_table_len != footer.block_count * CHUNK_DESCRIPTOR_SIZE:
        raise FormatError("chunk table length does not match footer block_count")

    manifest_bytes = data[entry_table_offset:entry_table_end]
    chunk_table_bytes = data[chunk_table_offset:chunk_table_end]

    manifest_stats = _decode_manifest(manifest_bytes)
    chunk_result = _parse_chunk_table(
        chunk_table_bytes, footer.block_count, payload_offset
    )
    chunk_stats = chunk_result["stats"]
    chunk_stats["parts"] = chunk_result["parts"]
    file_analysis = _build_file_analysis(manifest_stats, chunk_result)

    if chunk_result["payload_end"] != entry_table_offset:
        raise FormatError("chunk payload bytes do not align with manifest offset")

    section_sizes = {
        "global_header": GLOBAL_HEADER_SIZE,
        "payload": payload_size,
        "manifest_entry_table": entry_table_len,
        "chunk_table": chunk_table_len,
        "footer": FOOTER_SIZE,
        "total_file_size": file_size,
    }

    section_parts = [
        {"name": "global_header", "offset": 0, "size": GLOBAL_HEADER_SIZE},
        {"name": "payload", "offset": payload_offset, "size": payload_size},
        {
            "name": "manifest_entry_table",
            "offset": entry_table_offset,
            "size": entry_table_len,
        },
        {
            "name": "chunk_table",
            "offset": chunk_table_offset,
            "size": chunk_table_len,
        },
        {"name": "footer", "offset": footer_offset, "size": FOOTER_SIZE},
    ]

    return {
        "archive_path": os.path.abspath(path),
        "header": {
            "magic": header.magic.decode("ascii", errors="replace"),
            "version": header.version,
            "flags": header.flags,
            "parts": [
                {"name": "magic", "offset": 0, "size": 4},
                {"name": "version", "offset": 4, "size": 2},
                {"name": "flags", "offset": 6, "size": 2},
            ],
        },
        "footer": {
            "version": footer.version,
            "flags": footer.flags,
            "block_count": footer.block_count,
            "entry_table_offset": footer.entry_table_offset,
            "entry_table_len": footer.entry_table_len,
            "chunk_table_offset": footer.chunk_table_offset,
            "chunk_table_len": footer.chunk_table_len,
            "global_crc32": footer.global_crc32,
            "parts": [
                {"name": "end_magic", "offset": footer_offset, "size": 4},
                {"name": "version", "offset": footer_offset + 4, "size": 2},
                {"name": "flags", "offset": footer_offset + 6, "size": 2},
                {"name": "block_count", "offset": footer_offset + 8, "size": 4},
                {
                    "name": "entry_table_offset",
                    "offset": footer_offset + 12,
                    "size": 8,
                },
                {
                    "name": "entry_table_len",
                    "offset": footer_offset + 20,
                    "size": 4,
                },
                {
                    "name": "chunk_table_offset",
                    "offset": footer_offset + 24,
                    "size": 8,
                },
                {
                    "name": "chunk_table_len",
                    "offset": footer_offset + 32,
                    "size": 4,
                },
                {"name": "global_crc32", "offset": footer_offset + 36, "size": 4},
            ],
        },
        "section_sizes": section_sizes,
        "section_parts": section_parts,
        "chunk_analysis": chunk_stats,
        "manifest_analysis": manifest_stats,
        "file_analysis": file_analysis,
    }


def _format_ratio(ratio: float | None) -> str:
    if ratio is None:
        return "n/a"
    return f"{ratio:.4f}"


def _format_bytes(value: int) -> str:
    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    unit = 0
    while abs(size) >= 1024 and unit < len(units) - 1:
        size /= 1024
        unit += 1
    if unit == 0:
        return f"{int(size):,} {units[unit]}"
    return f"{size:.1f} {units[unit]} ({value:,} B)"


def _format_part(part: Dict[str, object]) -> str:
    fields = []
    for key in (
        "value",
        "kind",
        "encoded_len",
        "raw_len",
        "payload_offset",
        "payload_end",
        "dictionary_bytes",
        "dictionary_len_varint_bytes",
        "compression_flags",
        "dictionary_id",
        "prefix_varint_bytes",
        "suffix_varint_bytes",
        "suffix_bytes",
        "path_bytes",
        "target_bytes",
        "target_varint_bytes",
        "size_field_bytes",
        "metadata_bytes",
    ):
        if key in part and part[key] not in {None, ""}:
            fields.append(f"{key}={part[key]}")
    if "metadata_parts" in part and part["metadata_parts"]:
        fields.append(
            f"metadata_parts={json.dumps(part['metadata_parts'], sort_keys=True)}"
        )
    if "algorithm" in part and part["algorithm"] is not None:
        fields.append(f"algo={part['algorithm']}")
    if "flags" in part:
        fields.append(f"flags={part['flags']}")
    extras = f" [{', '.join(fields)}]" if fields else ""
    offset = part.get("offset")
    offset_text = f"@{offset}" if offset is not None else ""
    size = int(part.get("size", 0))
    return f"{part.get('name', '<part>')}{offset_text}: {_format_bytes(size)}{extras}"


def _print_part_group(
    title: str, parts: List[Dict[str, object]], indent: str = "  "
) -> None:
    print(title)
    for part in parts:
        print(f"{indent}{_format_part(part)}")


def _format_chunk_part(part: Dict[str, object]) -> str:
    name = str(part.get("name", "<chunk>"))
    payload_offset = int(part.get("payload_offset", 0))
    payload_end = int(part.get("payload_end", payload_offset))
    raw_offset = int(part.get("raw_offset", 0))
    raw_end = int(part.get("raw_end", raw_offset))
    run_length = int(part.get("run_length", 1))
    run_label = f" x{run_length}" if run_length > 1 else ""

    if part.get("is_reference"):
        return (
            f"{name}{run_label}: ref -> chunk[{part['checksum_or_target']}] "
            f"raw={_format_bytes(int(part['raw_len']))} "
            f"logical={_format_bytes(raw_offset)}..{_format_bytes(raw_end)}"
        )

    tags = [str(part.get("algorithm", "unknown"))]
    if part.get("raw_passthrough"):
        tags.append("raw")
    dictionary_id = int(part.get("dictionary_id", 0))
    if dictionary_id != 0:
        tags.append(f"dict={dictionary_id}")

    return (
        f"{name}{run_label}: {'/'.join(tags)} raw={_format_bytes(int(part['raw_len']))} "
        f"-> encoded={_format_bytes(int(part['encoded_len']))} "
        f"ratio={_format_ratio(part.get('compression_ratio'))} "
        f"payload={_format_bytes(payload_offset)}..{_format_bytes(payload_end)} "
        f"logical={_format_bytes(raw_offset)}..{_format_bytes(raw_end)}"
    )


def _summarize_chunk_runs(parts: List[Dict[str, object]]) -> List[Dict[str, object]]:
    if not parts:
        return []

    summarized: List[Dict[str, object]] = []
    index = 0
    while index < len(parts):
        part = parts[index]
        run_end = index + 1
        signature = (
            part.get("encoded_len"),
            part.get("raw_len"),
            part.get("compression_flags"),
            part.get("dictionary_id"),
            part.get("algorithm"),
            part.get("raw_passthrough"),
            part.get("is_reference"),
            part.get("checksum_or_target"),
        )
        while run_end < len(parts):
            candidate = parts[run_end]
            candidate_signature = (
                candidate.get("encoded_len"),
                candidate.get("raw_len"),
                candidate.get("compression_flags"),
                candidate.get("dictionary_id"),
                candidate.get("algorithm"),
                candidate.get("raw_passthrough"),
                candidate.get("is_reference"),
                candidate.get("checksum_or_target"),
            )
            if candidate_signature != signature:
                break
            run_end += 1

        if run_end - index == 1:
            summarized.append(part)
        else:
            summarized.append(
                {
                    **part,
                    "name": f"chunk[{index}..{run_end - 1}]",
                    "size": (run_end - index) * CHUNK_DESCRIPTOR_SIZE,
                    "payload_end": parts[run_end - 1]["payload_end"],
                    "raw_end": parts[run_end - 1]["raw_end"],
                    "run_length": run_end - index,
                }
            )
        index = run_end

    return summarized


def _print_chunk_descriptors(parts: List[Dict[str, object]], show_all: bool) -> None:
    print("Chunk descriptors")
    summarized = _summarize_chunk_runs(parts)
    if show_all or len(summarized) <= DEFAULT_CHUNK_DETAIL_LIMIT:
        head = summarized
        tail: List[Dict[str, object]] = []
        omitted = 0
    else:
        head_count = DEFAULT_CHUNK_DETAIL_LIMIT // 2
        tail_count = DEFAULT_CHUNK_DETAIL_LIMIT - head_count
        head = summarized[:head_count]
        tail = summarized[-tail_count:]
        omitted = len(summarized) - len(head) - len(tail)

    for part in head:
        print(f"  {_format_chunk_part(part)}")
    if omitted:
        print(f"  ... omitted {omitted:,} summarized chunk lines ...")
        for part in tail:
            print(f"  {_format_chunk_part(part)}")


def _print_manifest_entries(entries: List[Dict[str, object]], show_all: bool) -> None:
    print("Manifest entries")
    if show_all or len(entries) <= DEFAULT_MANIFEST_ENTRY_DETAIL_LIMIT:
        head = entries
        tail: List[Dict[str, object]] = []
        omitted = 0
    else:
        head_count = DEFAULT_MANIFEST_ENTRY_DETAIL_LIMIT // 2
        tail_count = DEFAULT_MANIFEST_ENTRY_DETAIL_LIMIT - head_count
        head = entries[:head_count]
        tail = entries[-tail_count:]
        omitted = len(entries) - len(head) - len(tail)

    for entry in head:
        description = (
            f"entry[{entry['index']}] {entry['kind']} path={json.dumps(entry['path'])} "
            f"size={_format_bytes(int(entry['entry_payload_size']))} "
            f"manifest={_format_bytes(int(entry['size']))}"
        )
        if entry["kind"] == "file":
            description += (
                f" logical={_format_bytes(int(entry['content_offset']))}"
                f"..{_format_bytes(int(entry['content_end']))}"
            )
        print(f"  {description}")
    if omitted:
        print(f"  ... omitted {omitted:,} manifest entries ...")
        for entry in tail:
            description = (
                f"entry[{entry['index']}] {entry['kind']} path={json.dumps(entry['path'])} "
                f"size={_format_bytes(int(entry['entry_payload_size']))} "
                f"manifest={_format_bytes(int(entry['size']))}"
            )
            if entry["kind"] == "file":
                description += (
                    f" logical={_format_bytes(int(entry['content_offset']))}"
                    f"..{_format_bytes(int(entry['content_end']))}"
                )
            print(f"  {description}")


def _print_manifest_layout(parts: List[Dict[str, object]]) -> None:
    print("Manifest layout")
    entry_parts = [
        part for part in parts if str(part.get("name", "")).startswith("entry[")
    ]
    other_parts = [part for part in parts if part not in entry_parts]
    for part in other_parts:
        print(f"  {_format_part(part)}")
    if entry_parts:
        first_index = entry_parts[0]["name"].split("[", 1)[1].split("]", 1)[0]
        last_index = entry_parts[-1]["name"].split("[", 1)[1].split("]", 1)[0]
        total_entry_bytes = sum(int(part["size"]) for part in entry_parts)
        print(
            "  "
            f"entries[{first_index}..{last_index}]: {_format_bytes(total_entry_bytes)} "
            f"({len(entry_parts):,} manifest entry records)"
        )


def _print_file_analysis(file_analysis: Dict[str, object]) -> None:
    print("File analysis")
    print(f"  file_count: {file_analysis['file_count']}")

    def print_rows(title: str, rows: List[Dict[str, object]]) -> None:
        if not rows:
            return
        print(f"  {title}:")
        for row in rows:
            details = (
                f"    {json.dumps(row['path'])}: raw={_format_bytes(int(row['size']))} "
                f"est_encoded={_format_bytes(int(row['estimated_encoded_bytes']))} "
                f"est_ratio={_format_ratio(row['estimated_ratio'])} chunks={row['chunk_count']}"
            )
            if int(row["raw_passthrough_overlap_bytes"]) > 0:
                details += f" raw_overlap={_format_bytes(int(row['raw_passthrough_overlap_bytes']))}"
            if int(row["near_raw_overlap_bytes"]) > 0:
                details += f" near_raw_overlap={_format_bytes(int(row['near_raw_overlap_bytes']))}"
            print(details)

    print_rows(
        "largest_estimated_encoded_files",
        file_analysis["largest_estimated_encoded_files"],
    )
    print_rows(
        "worst_estimated_ratio_files",
        file_analysis["worst_estimated_ratio_files"],
    )
    print_rows(
        "raw_passthrough_dominated_files",
        file_analysis["raw_passthrough_dominated_files"],
    )


def print_human_report(
    result: Dict[str, object],
    *,
    show_all_chunks: bool = False,
    show_all_entries: bool = False,
) -> None:
    section_sizes = result["section_sizes"]
    section_parts = result["section_parts"]
    header = result["header"]
    footer = result["footer"]
    chunk = result["chunk_analysis"]
    manifest = result["manifest_analysis"]
    file_analysis = result["file_analysis"]
    manifest_layout = manifest["layout"]

    print(f"Archive: {result['archive_path']}")
    print()
    print("Section sizes")
    for name, size in section_sizes.items():
        print(f"  {name}: {_format_bytes(int(size))}")
    print()
    _print_part_group("Top-level layout", section_parts)
    print()
    _print_part_group("Global header", header["parts"])
    print()
    _print_part_group("Footer", footer["parts"])
    print()
    print("Chunk analysis")
    print(f"  block_count: {chunk['block_count']}")
    print(f"  total_raw_bytes: {_format_bytes(int(chunk['total_raw_bytes']))}")
    print(f"  total_encoded_bytes: {_format_bytes(int(chunk['total_encoded_bytes']))}")
    print(f"  compression_ratio: {_format_ratio(chunk['compression_ratio'])}")
    print(f"  reference_block_count: {chunk['reference_block_count']}")
    print(f"  raw_passthrough_block_count: {chunk['raw_passthrough_block_count']}")
    print(f"  near_raw_block_count: {chunk['near_raw_block_count']}")
    print(
        f"  per_algorithm_counts: {json.dumps(chunk['per_algorithm_counts'], sort_keys=True)}"
    )
    print(
        f"  dictionary_id_counts: {json.dumps(chunk['dictionary_id_counts'], sort_keys=True)}"
    )
    print()
    _print_chunk_descriptors(chunk["parts"], show_all_chunks)
    print()
    print("Manifest analysis")
    print(f"  entry_count: {manifest['entry_count']}")
    print(f"  file_count: {manifest['file_count']}")
    print(f"  directory_count: {manifest['directory_count']}")
    print(f"  symlink_count: {manifest['symlink_count']}")
    print(
        f"  total_file_payload_bytes: {_format_bytes(int(manifest['total_file_payload_bytes']))}"
    )
    print(f"  dictionary_bank_count: {manifest['dictionary_bank_count']}")
    print(
        f"  total_dictionary_bytes: {_format_bytes(int(manifest['total_dictionary_bytes']))}"
    )
    print()
    _print_manifest_layout(manifest_layout["parts"])
    if manifest_layout["dictionaries"]:
        print()
        _print_part_group("Manifest dictionaries", manifest_layout["dictionaries"])
    if manifest_layout["entries"]:
        print()
        _print_manifest_entries(manifest_layout["entries"], show_all_entries)
    print()
    _print_file_analysis(file_analysis)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze an Oxide .oxz archive.")
    parser.add_argument("archive_path", help="Path to .oxz archive")
    parser.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON output"
    )
    parser.add_argument(
        "--show-all-chunks",
        action="store_true",
        help="Print every chunk descriptor instead of summarized output",
    )
    parser.add_argument(
        "--show-all-entries",
        action="store_true",
        help="Print every manifest entry instead of sampled output",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    try:
        result = analyze_archive(args.archive_path)
    except (OSError, FormatError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print_human_report(
            result,
            show_all_chunks=args.show_all_chunks,
            show_all_entries=args.show_all_entries,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

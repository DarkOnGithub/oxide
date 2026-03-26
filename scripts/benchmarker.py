#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import shlex
import random
import re
import statistics
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence
import sys

try:
    import psutil  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dependency
    psutil = None

try:
    import resource  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dependency
    resource = None

PSUTIL_SAMPLE_ERRORS = ()
if psutil is not None:  # pragma: no branch - import guard
    PSUTIL_SAMPLE_ERRORS = (
        psutil.NoSuchProcess,
        psutil.AccessDenied,
        psutil.ZombieProcess,
    )

try:
    from rich import box
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: rich. Install with `python3 -m pip install -r scripts/requirements-benchmark.txt`."
    ) from exc

BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ModeConfig:
    compression: str
    compression_level: str | None
    block_size: str = "1M"


@dataclass(frozen=True)
class Settings:
    source: Path
    oxide_bin: Path
    oxide_output: Path
    squashfs_output: Path
    oxide_extract_dir: Path
    squashfs_extract_dir: Path
    threads: int
    passes: int
    skip_extract: bool
    drop_caches: bool
    rebuild_oxide: bool
    modes: tuple[str, ...]
    squashfs_block_size: str | None
    telemetry_dir: Path
    shuffle_seed: int


@dataclass(frozen=True)
class ResultRow:
    tool: str
    phase: str
    mode: str
    workers: str
    pass_num: int
    elapsed_ns: int
    throughput_mib_s: float
    output_bytes: int
    input_bytes: int
    command: str
    output_path: str
    stdout_path: str
    stderr_path: str
    telemetry_path: str | None


@dataclass(frozen=True)
class ModeStats:
    tool: str
    phase: str
    mode: str
    workers: str
    avg_seconds: float
    avg_throughput_mib_s: float
    avg_output_bytes: int
    ratio: float


@dataclass(frozen=True)
class ModeComparison:
    mode: str
    workers: str
    archive_fastest_tool: str
    archive_delta_pct: float
    extract_fastest_tool: str | None
    extract_delta_pct: float | None
    oxide_archive_ratio: float
    baseline_archive_ratio: float
    oxide_extract_ratio: float | None
    baseline_extract_ratio: float | None


@dataclass(frozen=True)
class Step:
    tool: str
    phase: str
    workers: str
    output_path: Path
    command_builder: Callable[[Settings, str, ModeConfig, str], Sequence[str]]
    cleanup_targets: tuple[Path, ...] = ()


@dataclass(frozen=True)
class RunUnit:
    mode: str
    workers: str
    mode_config: ModeConfig
    oxide_archive: Step
    oxide_extract: Step | None
    baseline_archive: Step
    baseline_extract: Step | None


@dataclass(frozen=True)
class HostTelemetry:
    cpu_user_ns: int | None
    cpu_system_ns: int | None
    cpu_percent: float | None
    peak_rss_bytes: int | None
    final_rss_bytes: int | None
    io_read_bytes: int | None
    io_write_bytes: int | None
    io_read_count: int | None
    io_write_count: int | None
    voluntary_ctx_switches: int | None
    involuntary_ctx_switches: int | None
    minor_faults: int | None
    major_faults: int | None
    disk_usage_before: dict[str, int] | None
    disk_usage_after: dict[str, int] | None


MODE_CONFIGS: dict[str, ModeConfig] = {
    "fast": ModeConfig(compression="lz4", compression_level=None, block_size="1M"),
    "balanced": ModeConfig(
        compression="zstd",
        compression_level="6",
        block_size="1M",
    ),
    "ultra": ModeConfig(compression="lzma", compression_level="9", block_size="4M"),
    "extreme": ModeConfig(compression="lzma", compression_level="9", block_size="8M"),
}

MODE_BASELINES: dict[str, str] = {
    "fast": "mksquashfs",
    "balanced": "mksquashfs",
    "ultra": "7zz",
    "extreme": "7zz",
}


def resolve_tool(*candidates: str) -> str:
    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise SystemExit(f"Missing required tool: tried {', '.join(candidates)}")


def env_value(name: str, default: str) -> str:
    return os.environ.get(name, default)


def parse_args() -> Settings:
    parser = argparse.ArgumentParser(
        description="Benchmark oxide against squashfs tools"
    )
    parser.add_argument(
        "--source",
        default=env_value("BENCHMARK_SOURCE", str(BASE_DIR / "silesia_corpus")),
    )
    parser.add_argument(
        "--oxide-bin",
        default=env_value(
            "BENCHMARK_OXIDE_BIN", str(BASE_DIR / "target/release/oxide")
        ),
    )
    parser.add_argument(
        "--oxide-output",
        default=env_value(
            "BENCHMARK_OXIDE_OUTPUT", str(BASE_DIR / "silesia_corpus.oxz")
        ),
    )
    parser.add_argument(
        "--squashfs-output",
        default=env_value("BENCHMARK_SQUASHFS_OUTPUT", str(BASE_DIR / "archive.sqfs")),
    )
    parser.add_argument(
        "--oxide-extract-dir",
        default=env_value(
            "BENCHMARK_OXIDE_EXTRACT_DIR", str(BASE_DIR / "oxide_extract_out")
        ),
    )
    parser.add_argument(
        "--squashfs-extract-dir",
        default=env_value(
            "BENCHMARK_SQUASHFS_EXTRACT_DIR", str(BASE_DIR / "squashfs_extract_out")
        ),
    )
    parser.add_argument(
        "--threads", type=int, default=int(env_value("BENCHMARK_THREADS", "16"))
    )
    parser.add_argument(
        "--passes", type=int, default=int(env_value("BENCHMARK_PASSES", "5"))
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        default=env_value("BENCHMARK_SKIP_EXTRACT", "0") == "1",
    )
    parser.add_argument(
        "--no-drop-caches",
        dest="drop_caches",
        action="store_false",
        help="Do not clear OS caches between benchmark steps.",
    )
    parser.set_defaults(drop_caches=True)
    parser.add_argument(
        "--rebuild-oxide",
        action="store_true",
        default=True,
        help="Rebuild oxide before benchmarking instead of using the existing binary.",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        default=["fast", "balanced", "ultra", "extreme"],
    )
    parser.add_argument(
        "--squashfs-block-size",
        default=env_value("BENCHMARK_SQUASHFS_BLOCK_SIZE", ""),
        help="Optional override for mksquashfs block size; default is mode-matched.",
    )
    parser.add_argument(
        "--shuffle-seed",
        type=int,
        default=None,
        help="Random seed used to shuffle run order per pass.",
    )
    parser.add_argument(
        "--telemetry-dir",
        default=env_value(
            "BENCHMARK_TELEMETRY_DIR", str(BASE_DIR / "benchmark_telemetry")
        ),
        help="Directory where JSONL/CSV telemetry for each run is written.",
    )

    args = parser.parse_args()

    shuffle_seed = (
        args.shuffle_seed
        if args.shuffle_seed is not None
        else random.randint(0, 2**31 - 1)
    )
    squashfs_block_size = args.squashfs_block_size or None

    return Settings(
        source=Path(args.source),
        oxide_bin=Path(args.oxide_bin),
        oxide_output=Path(args.oxide_output),
        squashfs_output=Path(args.squashfs_output),
        oxide_extract_dir=Path(args.oxide_extract_dir),
        squashfs_extract_dir=Path(args.squashfs_extract_dir),
        threads=args.threads,
        passes=args.passes,
        skip_extract=args.skip_extract,
        drop_caches=args.drop_caches,
        rebuild_oxide=args.rebuild_oxide,
        modes=tuple(args.modes),
        squashfs_block_size=squashfs_block_size,
        telemetry_dir=Path(args.telemetry_dir),
        shuffle_seed=shuffle_seed,
    )


def format_bytes(num_bytes: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(num_bytes)
    unit_index = 0

    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.2f} {units[unit_index]}"


def cleanup_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def apparent_size(path: Path) -> int:
    """
    Calculates size exactly like `du -sb`.
    Uses lstat to avoid following symlinks and tracks inodes to avoid double-counting hard links.
    """
    if not path.exists():
        return 0
    if path.is_file() or path.is_symlink():
        return path.lstat().st_size

    total = 0
    seen_inodes = set()
    for root, _, files in os.walk(path):
        for name in files:
            file_path = Path(root) / name
            try:
                st = file_path.lstat()
                # Deduplicate hard links by inode
                if st.st_nlink > 1:
                    if st.st_ino in seen_inodes:
                        continue
                    seen_inodes.add(st.st_ino)
                total += st.st_size
            except FileNotFoundError:
                continue
    return total


def source_size(path: Path) -> int:
    if path.is_file() or path.is_symlink():
        return path.lstat().st_size
    return apparent_size(path)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def percent_delta(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return ((new - old) / old) * 100.0


def format_delta_pct(delta_pct: float, baseline_name: str = "baseline") -> str:
    if delta_pct == 0:
        return f"0.00% vs {baseline_name}"
    if delta_pct < 0:
        return f"{abs(delta_pct):.2f}% faster than {baseline_name}"
    return f"{delta_pct:.2f}% slower than {baseline_name}"


def phase_sort_value(phase: str) -> int:
    return {"archive": 0, "extract": 1}.get(phase, 99)


def mean_bytes(rows: Sequence[ResultRow]) -> int:
    return round(statistics.mean(row.output_bytes for row in rows))


ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def parse_duration_to_us(text: str) -> int:
    value = text.strip()
    if not value:
        return 0
    if value.endswith("us"):
        return int(float(value[:-2]))
    if value.endswith("ms"):
        return round(float(value[:-2]) * 1_000)
    if value.endswith("s"):
        return round(float(value[:-1]) * 1_000_000)
    parts = value.split(":")
    if len(parts) == 2:
        minutes = int(parts[0])
        seconds = float(parts[1])
        return round((minutes * 60 + seconds) * 1_000_000)
    if len(parts) == 3:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])
        return round(((hours * 60 + minutes) * 60 + seconds) * 1_000_000)
    return 0


def parse_bytes_text(text: str) -> int | None:
    value = text.strip()
    if not value:
        return None
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)", value, re.IGNORECASE)
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2).lower()
    multipliers = {
        "b": 1,
        "kib": 1024,
        "kb": 1000,
        "mib": 1024**2,
        "mb": 1000**2,
        "gib": 1024**3,
        "gb": 1000**3,
        "tib": 1024**4,
        "tb": 1000**4,
    }
    return round(number * multipliers.get(unit, 1))


def parse_percent(text: str) -> float | None:
    value = text.strip()
    if value.endswith("%"):
        try:
            return float(value[:-1]) / 100.0
        except ValueError:
            return None
    return None


def parse_scalar_value(label: str, value: str) -> int | float | str | None:
    normalized = normalize_key(label)
    raw = value.strip()
    if normalized in {"scheduler", "buffer_pool"}:
        return raw
    if normalized == "effective_cores":
        try:
            return float(raw)
        except ValueError:
            return raw
    if normalized in {
        "compress_busy",
        "compression_busy",
        "decode_busy",
    }:
        return parse_duration_to_us(raw)
    if normalized in {
        "max_inflight_blocks",
        "inflight_per_worker",
        "inflight_worker",
        "writer_queue",
        "reorder_limit",
        "reorder_peak",
    }:
        try:
            return int(raw)
        except ValueError:
            return raw
    if normalized in {
        "configured_inflight",
        "max_inflight_bytes",
    }:
        return parse_bytes_text(raw) or raw
    if normalized == "stages":
        return raw
    return raw


def split_table_cells(line: str) -> list[str]:
    stripped = strip_ansi(line).strip()
    if not stripped.startswith("│"):
        return []
    cells = [part.strip() for part in stripped.strip("│").split("│")]
    return cells


def extract_sections(output: str) -> dict[str, list[str]]:
    lines = strip_ansi(output).splitlines()
    sections: dict[str, list[str]] = {}
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("▣ "):
            title = line[2:].strip()
            section_lines: list[str] = []
            i += 1
            while i < len(lines):
                next_line = lines[i].strip()
                if next_line.startswith("▣ ") or next_line.startswith("╔"):
                    break
                section_lines.append(lines[i])
                i += 1
            sections[title] = section_lines
            continue
        i += 1
    return sections


def parse_stage_summary(summary: str) -> dict[str, int]:
    stages: dict[str, int] = {}
    for piece in summary.split("|"):
        item = piece.strip()
        if not item:
            continue
        if " " not in item:
            continue
        label, duration = item.rsplit(" ", 1)
        stages[label.strip()] = parse_duration_to_us(duration)
    return stages


def parse_runtime_section(section_lines: Sequence[str]) -> dict[str, object]:
    rows = [split_table_cells(line) for line in section_lines]
    rows = [row for row in rows if row]
    runtime: dict[str, object] = {
        "fields": {},
        "stages": {},
    }
    fields = runtime["fields"]  # type: ignore[assignment]
    stages = runtime["stages"]  # type: ignore[assignment]

    for row in rows:
        if len(row) < 2:
            continue
        label, value = row[0], row[1]
        if label == "Field":
            continue
        if label == "Stages":
            runtime["stage_summary"] = value
            stages.update(parse_stage_summary(value))
            continue

        parsed = parse_scalar_value(label, value)
        fields[normalize_key(label)] = parsed
        runtime[normalize_key(label)] = parsed

    return runtime


def parse_worker_section(section_lines: Sequence[str]) -> list[dict[str, object]]:
    rows = [split_table_cells(line) for line in section_lines]
    rows = [row for row in rows if row]
    workers: list[dict[str, object]] = []
    for row in rows:
        if row[0] == "ID" or len(row) < 6:
            continue
        try:
            workers.append(
                {
                    "worker_id": int(row[0]),
                    "tasks": int(row[1]),
                    "utilization": (parse_percent(row[2]) or 0.0),
                    "busy_us": parse_duration_to_us(row[3]),
                    "idle_us": parse_duration_to_us(row[4]),
                    "uptime_us": parse_duration_to_us(row[5]),
                }
            )
        except ValueError:
            continue
    return workers


def parse_oxide_telemetry(stdout: str) -> dict[str, object]:
    sections = extract_sections(stdout)
    runtime = parse_runtime_section(sections.get("Runtime", []))
    workers = parse_worker_section(sections.get("Worker table", []))
    queue_peaks = {
        key: runtime[key]
        for key in (
            "max_inflight_blocks",
            "inflight_worker",
            "configured_inflight",
            "max_inflight_bytes",
            "writer_queue",
            "reorder_limit",
            "reorder_peak",
        )
        if key in runtime
    }
    worker_utilization = {
        str(row["worker_id"]): row["utilization"]
        for row in workers
        if "worker_id" in row and "utilization" in row
    }
    return {
        "runtime": runtime,
        "workers": workers,
        "stage_timings": runtime.get("stages", {}),
        "queue_peaks": queue_peaks,
        "worker_utilization": worker_utilization,
    }


def disk_usage_snapshot(path: Path) -> dict[str, int] | None:
    try:
        usage = shutil.disk_usage(path)
    except FileNotFoundError:
        return None
    return {"total": usage.total, "used": usage.used, "free": usage.free}


def _safe_getrusage_children():
    if resource is None:
        return None
    return resource.getrusage(resource.RUSAGE_CHILDREN)


def _rusage_delta(before, after) -> dict[str, int] | None:
    if before is None or after is None:
        return None
    return {
        "cpu_user_ns": int(
            max(0.0, (after.ru_utime - before.ru_utime) * 1_000_000_000)
        ),
        "cpu_system_ns": int(
            max(0.0, (after.ru_stime - before.ru_stime) * 1_000_000_000)
        ),
        "minor_faults": max(0, after.ru_minflt - before.ru_minflt),
        "major_faults": max(0, after.ru_majflt - before.ru_majflt),
        "voluntary_ctx_switches": max(0, after.ru_nvcsw - before.ru_nvcsw),
        "involuntary_ctx_switches": max(0, after.ru_nivcsw - before.ru_nivcsw),
    }


def host_telemetry_payload(host: HostTelemetry, elapsed_ns: int) -> dict[str, object]:
    cpu_user_ns = host.cpu_user_ns or 0
    cpu_system_ns = host.cpu_system_ns or 0
    cpu_percent = (
        ((cpu_user_ns + cpu_system_ns) / elapsed_ns * 100.0) if elapsed_ns > 0 else None
    )
    payload: dict[str, object] = {
        "cpu_user_ns": host.cpu_user_ns,
        "cpu_system_ns": host.cpu_system_ns,
        "cpu_percent": cpu_percent,
        "peak_rss_bytes": host.peak_rss_bytes,
        "final_rss_bytes": host.final_rss_bytes,
        "io_read_bytes": host.io_read_bytes,
        "io_write_bytes": host.io_write_bytes,
        "io_read_count": host.io_read_count,
        "io_write_count": host.io_write_count,
        "voluntary_ctx_switches": host.voluntary_ctx_switches,
        "involuntary_ctx_switches": host.involuntary_ctx_switches,
        "minor_faults": host.minor_faults,
        "major_faults": host.major_faults,
        "disk_usage_before": host.disk_usage_before,
        "disk_usage_after": host.disk_usage_after,
    }
    return payload


def run_command_with_host_telemetry(
    command: Sequence[str],
    telemetry_path: Path,
    sample_interval_s: float = 0.05,
) -> tuple[subprocess.CompletedProcess[str], HostTelemetry]:
    before_rusage = _safe_getrusage_children()
    disk_path = (
        telemetry_path.parent
        if telemetry_path.parent.exists()
        else telemetry_path.parent.parent
    )
    disk_before = disk_usage_snapshot(disk_path)

    proc = subprocess.Popen(
        list(command),
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    ps_proc = psutil.Process(proc.pid) if psutil is not None else None
    peak_rss = 0
    final_rss = None
    io_read_bytes = None
    io_write_bytes = None
    io_read_count = None
    io_write_count = None

    def sample_process() -> None:
        nonlocal \
            peak_rss, \
            final_rss, \
            io_read_bytes, \
            io_write_bytes, \
            io_read_count, \
            io_write_count
        if ps_proc is None:
            return
        try:
            memory_info = ps_proc.memory_info()
            peak_rss = max(peak_rss, int(memory_info.rss))
            final_rss = int(memory_info.rss)
            if hasattr(ps_proc, "io_counters"):
                io = ps_proc.io_counters()
                if io is not None:
                    io_read_bytes = int(getattr(io, "read_bytes", 0))
                    io_write_bytes = int(getattr(io, "write_bytes", 0))
                    io_read_count = int(getattr(io, "read_count", 0))
                    io_write_count = int(getattr(io, "write_count", 0))
        except PSUTIL_SAMPLE_ERRORS:
            pass

    sample_process()

    while True:
        if proc.poll() is not None:
            break

        sample_process()

        time.sleep(sample_interval_s)

    stdout, stderr = proc.communicate()
    after_rusage = _safe_getrusage_children()
    disk_after = disk_usage_snapshot(disk_path)

    rusage_delta = _rusage_delta(before_rusage, after_rusage) or {}
    cpu_user_ns = rusage_delta.get("cpu_user_ns")
    cpu_system_ns = rusage_delta.get("cpu_system_ns")
    total_cpu_ns = (cpu_user_ns or 0) + (cpu_system_ns or 0)
    cpu_percent = None
    wall_ns = None
    if total_cpu_ns > 0:
        wall_ns = None

    host = HostTelemetry(
        cpu_user_ns=cpu_user_ns,
        cpu_system_ns=cpu_system_ns,
        cpu_percent=None,
        peak_rss_bytes=peak_rss or None,
        final_rss_bytes=final_rss,
        io_read_bytes=io_read_bytes,
        io_write_bytes=io_write_bytes,
        io_read_count=io_read_count,
        io_write_count=io_write_count,
        voluntary_ctx_switches=rusage_delta.get("voluntary_ctx_switches"),
        involuntary_ctx_switches=rusage_delta.get("involuntary_ctx_switches"),
        minor_faults=rusage_delta.get("minor_faults"),
        major_faults=rusage_delta.get("major_faults"),
        disk_usage_before=disk_before,
        disk_usage_after=disk_after,
    )

    result = subprocess.CompletedProcess(
        args=list(command),
        returncode=proc.returncode,
        stdout=stdout,
        stderr=stderr,
    )
    return result, host


class TelemetryWriter:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.steps_dir = run_dir / "steps"
        self.steps_jsonl = run_dir / "steps.jsonl"
        self.steps_csv = run_dir / "steps.csv"
        self.run_json = run_dir / "run.json"
        self.summary_json = run_dir / "summary.json"
        self._jsonl_fp = None
        self._csv_fp = None
        self._csv_writer = None

    def __enter__(self) -> "TelemetryWriter":
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.steps_dir.mkdir(parents=True, exist_ok=True)
        self._jsonl_fp = self.steps_jsonl.open("w", encoding="utf-8")
        self._csv_fp = self.steps_csv.open("w", encoding="utf-8", newline="")
        self._csv_writer = csv.DictWriter(
            self._csv_fp, fieldnames=self.csv_fieldnames()
        )
        self._csv_writer.writeheader()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._jsonl_fp is not None:
            self._jsonl_fp.close()
        if self._csv_fp is not None:
            self._csv_fp.close()

    @staticmethod
    def csv_fieldnames() -> list[str]:
        return [
            "event",
            "timestamp",
            "tool",
            "phase",
            "mode",
            "workers",
            "pass_num",
            "elapsed_ns",
            "elapsed_s",
            "throughput_mib_s",
            "input_bytes",
            "output_bytes",
            "ratio",
            "output_path",
            "command",
            "stdout_path",
            "stderr_path",
            "telemetry_path",
            "host_cpu_user_ns",
            "host_cpu_system_ns",
            "host_cpu_percent",
            "host_peak_rss_bytes",
            "host_final_rss_bytes",
            "host_io_read_bytes",
            "host_io_write_bytes",
            "host_io_read_count",
            "host_io_write_count",
            "host_voluntary_ctx_switches",
            "host_involuntary_ctx_switches",
            "host_minor_faults",
            "host_major_faults",
            "host_disk_total_before_bytes",
            "host_disk_used_before_bytes",
            "host_disk_free_before_bytes",
            "host_disk_total_after_bytes",
            "host_disk_used_after_bytes",
            "host_disk_free_after_bytes",
        ]

    def write_json(self, path: Path, payload: dict[str, object]) -> None:
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def write_run(self, payload: dict[str, object]) -> None:
        self.write_json(self.run_json, payload)

    def write_summary(self, payload: dict[str, object]) -> None:
        self.write_json(self.summary_json, payload)

    def step_prefix(self, row: ResultRow) -> str:
        return f"p{row.pass_num:02d}_{row.mode}_{row.workers}_{row.tool}_{row.phase}"

    def write_step_artifacts(
        self,
        row: ResultRow,
        stdout: str,
        stderr: str,
        host_payload: dict[str, object] | None,
        telemetry_payload: dict[str, object] | None,
    ) -> tuple[Path, Path, Path | None]:
        step_dir = self.steps_dir / self.step_prefix(row)
        step_dir.mkdir(parents=True, exist_ok=True)

        stdout_path = step_dir / "stdout.log"
        stderr_path = step_dir / "stderr.log"
        stdout_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")

        telemetry_path: Path | None = None
        if telemetry_payload is not None or host_payload is not None:
            telemetry_path = step_dir / "telemetry.json"
            payload: dict[str, object] = {
                "host": host_payload,
            }
            if telemetry_payload is not None:
                payload["oxide"] = telemetry_payload
                payload.update(telemetry_payload)
            self.write_json(telemetry_path, payload)

        return stdout_path, stderr_path, telemetry_path

    def write_step(
        self,
        row: ResultRow,
        stdout_path: Path,
        stderr_path: Path,
        host_payload: dict[str, object] | None,
        telemetry_path: Path | None,
    ) -> None:
        host_payload = host_payload or {}
        record = {
            "event": "step",
            "timestamp": utc_now(),
            "tool": row.tool,
            "phase": row.phase,
            "mode": row.mode,
            "workers": row.workers,
            "pass_num": row.pass_num,
            "elapsed_ns": row.elapsed_ns,
            "elapsed_s": row.elapsed_ns / 1_000_000_000,
            "throughput_mib_s": row.throughput_mib_s,
            "input_bytes": row.input_bytes,
            "output_bytes": row.output_bytes,
            "ratio": safe_ratio(row.output_bytes, row.input_bytes),
            "output_path": row.output_path,
            "command": row.command,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "telemetry_path": str(telemetry_path) if telemetry_path else None,
            "host_cpu_user_ns": host_payload.get("cpu_user_ns"),
            "host_cpu_system_ns": host_payload.get("cpu_system_ns"),
            "host_cpu_percent": host_payload.get("cpu_percent"),
            "host_peak_rss_bytes": host_payload.get("peak_rss_bytes"),
            "host_final_rss_bytes": host_payload.get("final_rss_bytes"),
            "host_io_read_bytes": host_payload.get("io_read_bytes"),
            "host_io_write_bytes": host_payload.get("io_write_bytes"),
            "host_io_read_count": host_payload.get("io_read_count"),
            "host_io_write_count": host_payload.get("io_write_count"),
            "host_voluntary_ctx_switches": host_payload.get("voluntary_ctx_switches"),
            "host_involuntary_ctx_switches": host_payload.get(
                "involuntary_ctx_switches"
            ),
            "host_minor_faults": host_payload.get("minor_faults"),
            "host_major_faults": host_payload.get("major_faults"),
            "host_disk_total_before_bytes": (
                host_payload.get("disk_usage_before") or {}
            ).get("total")
            if isinstance(host_payload.get("disk_usage_before"), dict)
            else None,
            "host_disk_used_before_bytes": (
                host_payload.get("disk_usage_before") or {}
            ).get("used")
            if isinstance(host_payload.get("disk_usage_before"), dict)
            else None,
            "host_disk_free_before_bytes": (
                host_payload.get("disk_usage_before") or {}
            ).get("free")
            if isinstance(host_payload.get("disk_usage_before"), dict)
            else None,
            "host_disk_total_after_bytes": (
                host_payload.get("disk_usage_after") or {}
            ).get("total")
            if isinstance(host_payload.get("disk_usage_after"), dict)
            else None,
            "host_disk_used_after_bytes": (
                host_payload.get("disk_usage_after") or {}
            ).get("used")
            if isinstance(host_payload.get("disk_usage_after"), dict)
            else None,
            "host_disk_free_after_bytes": (
                host_payload.get("disk_usage_after") or {}
            ).get("free")
            if isinstance(host_payload.get("disk_usage_after"), dict)
            else None,
        }
        assert self._jsonl_fp is not None and self._csv_writer is not None
        self._jsonl_fp.write(json.dumps(record, sort_keys=True) + "\n")
        self._jsonl_fp.flush()
        self._csv_writer.writerow(record)
        self._csv_fp.flush()  # type: ignore[union-attr]


def row_command(command: Sequence[str]) -> str:
    return shlex.join([str(part) for part in command])


def result_to_stats(rows: Sequence[ResultRow], source_bytes: int) -> list[ModeStats]:
    grouped: dict[tuple[str, str, str, str], list[ResultRow]] = {}
    for row in rows:
        grouped.setdefault((row.tool, row.phase, row.mode, row.workers), []).append(row)

    stats_rows: list[ModeStats] = []
    for tool, phase, mode, workers in sorted(grouped):
        group = grouped[(tool, phase, mode, workers)]
        avg_seconds = statistics.mean(row.elapsed_ns for row in group) / 1_000_000_000
        avg_throughput = statistics.mean(row.throughput_mib_s for row in group)
        avg_output = mean_bytes(group)
        stats_rows.append(
            ModeStats(
                tool=tool,
                phase=phase,
                mode=mode,
                workers=workers,
                avg_seconds=avg_seconds,
                avg_throughput_mib_s=avg_throughput,
                avg_output_bytes=avg_output,
                ratio=safe_ratio(avg_output, source_bytes),
            )
        )
    return stats_rows


def stats_lookup(
    stats_rows: Sequence[ModeStats],
) -> dict[tuple[str, str, str, str], ModeStats]:
    return {(row.tool, row.phase, row.mode, row.workers): row for row in stats_rows}


def mode_comparisons(
    stats_rows: Sequence[ModeStats], modes: Sequence[str], worker_modes: Sequence[str]
) -> list[ModeComparison]:
    lookup = stats_lookup(stats_rows)
    comparisons: list[ModeComparison] = []

    for mode in modes:
        baseline_tool = MODE_BASELINES[mode]
        for workers in worker_modes:
            oxide_archive = lookup.get(("oxide", "archive", mode, workers))
            baseline_archive = lookup.get((baseline_tool, "archive", mode, workers))
            if oxide_archive is None or baseline_archive is None:
                continue
            oxide_extract = lookup.get(("oxide", "extract", mode, workers))
            baseline_extract = lookup.get((baseline_tool, "extract", mode, workers))

            archive_fastest_tool = (
                "oxide"
                if oxide_archive.avg_seconds <= baseline_archive.avg_seconds
                else baseline_tool
            )
            if oxide_extract is not None and baseline_extract is not None:
                extract_fastest_tool: str | None = (
                    "oxide"
                    if oxide_extract.avg_seconds <= baseline_extract.avg_seconds
                    else baseline_tool
                )
                extract_delta_pct: float | None = percent_delta(
                    oxide_extract.avg_seconds, baseline_extract.avg_seconds
                )
                oxide_extract_ratio: float | None = oxide_extract.ratio
                baseline_extract_ratio: float | None = baseline_extract.ratio
            else:
                extract_fastest_tool = None
                extract_delta_pct = None
                oxide_extract_ratio = None
                baseline_extract_ratio = None

            comparisons.append(
                ModeComparison(
                    mode=mode,
                    workers=workers,
                    archive_fastest_tool=archive_fastest_tool,
                    archive_delta_pct=percent_delta(
                        oxide_archive.avg_seconds, baseline_archive.avg_seconds
                    ),
                    extract_fastest_tool=extract_fastest_tool,
                    extract_delta_pct=extract_delta_pct,
                    oxide_archive_ratio=oxide_archive.ratio,
                    baseline_archive_ratio=baseline_archive.ratio,
                    oxide_extract_ratio=oxide_extract_ratio,
                    baseline_extract_ratio=baseline_extract_ratio,
                )
            )

    return comparisons


def build_takes(comparisons: Sequence[ModeComparison]) -> list[str]:
    takes: list[str] = []
    if not comparisons:
        return takes

    for mode in sorted({item.mode for item in comparisons}):
        mode_items = [item for item in comparisons if item.mode == mode]
        best_archive = min(mode_items, key=lambda item: abs(item.archive_delta_pct))
        extract_candidates = [
            item for item in mode_items if item.extract_delta_pct is not None
        ]
        best_ratio = min(mode_items, key=lambda item: item.oxide_archive_ratio)

        takes.append(
            f"[{mode}] workers={best_archive.workers}: Closest archive match ({format_delta_pct(best_archive.archive_delta_pct)})."
        )
        if extract_candidates:
            best_extract = min(
                extract_candidates, key=lambda item: abs(item.extract_delta_pct or 0.0)
            )
            takes.append(
                f"[{mode}] workers={best_extract.workers}: Closest extract match ({format_delta_pct(best_extract.extract_delta_pct or 0.0)})."
            )
        takes.append(
            f"[{mode}] workers={best_ratio.workers}: Best Oxide archive ratio ({best_ratio.oxide_archive_ratio:.3f})."
        )
    return takes


def print_run_header(
    console: Console, settings: Settings, telemetry_run_dir: Path, source_bytes: int
) -> None:
    squashfs_policy = settings.squashfs_block_size or "mode-matched"
    panel = Panel.fit(
        Text.from_markup(
            f"[bold]Benchmark run[/bold]\n"
            f"source: [cyan]{settings.source}[/cyan]\n"
            f"source size: [cyan]{format_bytes(source_bytes)}[/cyan]\n"
            f"modes: [cyan]{', '.join(settings.modes)}[/cyan]\n"
            f"workers: [cyan]{settings.threads}[/cyan]\n"
            f"passes: [cyan]{settings.passes}[/cyan]  threads: [cyan]{settings.threads}[/cyan]\n"
            f"squashfs block size: [cyan]{squashfs_policy}[/cyan]\n"
            f"drop caches: [cyan]{'yes' if settings.drop_caches else 'no'}[/cyan]\n"
            f"shuffle seed: [cyan]{settings.shuffle_seed}[/cyan]\n"
            f"telemetry: [cyan]{telemetry_run_dir}[/cyan]"
        ),
        title="Oxide benchmark",
        border_style="blue",
    )
    console.print(panel)


def print_steps_table(
    console: Console, results: Sequence[ResultRow], source_bytes: int
) -> None:
    table = Table(title="Per-step results", box=box.SIMPLE_HEAVY)
    table.caption = (
        "Smaller source delta means better compression; lower time means faster."
    )
    table.add_column("mode", style="bold")
    table.add_column("workers", justify="right")
    table.add_column("pass", justify="right")
    table.add_column("tool", style="bold")
    table.add_column("phase")
    table.add_column("seconds", justify="right")
    table.add_column("MiB/s", justify="right")
    table.add_column("output", justify="right")
    table.add_column("source delta", justify="right")

    sorted_results = sorted(
        results,
        key=lambda row: (
            row.mode,
            row.workers,
            row.pass_num,
            phase_sort_value(row.phase),
            row.tool,
        ),
    )
    fastest = (
        min(sorted_results, key=lambda row: row.elapsed_ns) if sorted_results else None
    )
    smallest = (
        min(sorted_results, key=lambda row: row.output_bytes)
        if sorted_results
        else None
    )
    last_mode: str | None = None
    last_workers: str | None = None

    for row in sorted_results:
        style = None
        if fastest is row:
            style = "green"
        elif smallest is row:
            style = "cyan"

        if last_mode is not None and row.mode != last_mode:
            table.add_section()
        elif last_workers is not None and row.workers != last_workers:
            table.add_section()
        last_mode = row.mode
        last_workers = row.workers

        table.add_row(
            row.mode,
            row.workers,
            str(row.pass_num),
            row.tool,
            row.phase,
            f"{row.elapsed_ns / 1_000_000_000:.3f}",
            f"{row.throughput_mib_s:.2f}",
            format_bytes(row.output_bytes),
            f"{percent_delta(row.output_bytes, source_bytes):+.2f}%",
            style=style,
        )

    console.print(table)


def print_analysis(
    console: Console,
    stats_rows: Sequence[ModeStats],
    comparisons: Sequence[ModeComparison],
) -> None:
    lookup = stats_lookup(stats_rows)
    table = Table(box=box.SIMPLE_HEAVY)
    table.caption = "Delta is Oxide minus baseline: negative means Oxide is faster."
    table.add_column("mode", style="bold")
    table.add_column("workers", justify="right")
    table.add_column("baseline")
    table.add_column("comparison", style="bold")
    table.add_column("oxide avg sec", justify="right")
    table.add_column("baseline avg sec", justify="right")
    table.add_column("winner")
    table.add_column("delta", justify="right")
    table.add_column("oxide ratio", justify="right")
    table.add_column("baseline ratio", justify="right")

    for comparison in comparisons:
        baseline_tool = MODE_BASELINES.get(comparison.mode, "baseline")
        archive_oxide = lookup.get(
            ("oxide", "archive", comparison.mode, comparison.workers)
        )
        archive_base = lookup.get(
            (baseline_tool, "archive", comparison.mode, comparison.workers)
        )
        extract_oxide = lookup.get(
            ("oxide", "extract", comparison.mode, comparison.workers)
        )
        extract_base = lookup.get(
            (baseline_tool, "extract", comparison.mode, comparison.workers)
        )

        if archive_oxide is not None and archive_base is not None:
            table.add_row(
                comparison.mode,
                comparison.workers,
                baseline_tool,
                "archive",
                f"{archive_oxide.avg_seconds:.3f}",
                f"{archive_base.avg_seconds:.3f}",
                comparison.archive_fastest_tool,
                format_delta_pct(comparison.archive_delta_pct),
                f"{archive_oxide.ratio:.3f}",
                f"{archive_base.ratio:.3f}",
            )

        if extract_oxide is not None and extract_base is not None:
            table.add_row(
                comparison.mode,
                comparison.workers,
                baseline_tool,
                "extract",
                f"{extract_oxide.avg_seconds:.3f}",
                f"{extract_base.avg_seconds:.3f}",
                comparison.extract_fastest_tool or "—",
                format_delta_pct(comparison.extract_delta_pct or 0.0),
                f"{extract_oxide.ratio:.3f}",
                f"{extract_base.ratio:.3f}",
            )

    console.print(Panel(table, title="Mode comparisons", border_style="magenta"))

    takes = build_takes(comparisons)
    if takes:
        take_text = "\n".join(f"• {take}" for take in takes)
        console.print(Panel(take_text, title="Takeaways", border_style="magenta"))


def print_averages(console: Console, stats_rows: Sequence[ModeStats]) -> None:
    table = Table(title="Averages", box=box.SIMPLE_HEAVY)
    table.caption = "Averages are grouped by mode, workers, then phase, then tool."
    table.add_column("mode", style="bold")
    table.add_column("workers", justify="right")
    table.add_column("tool", style="bold")
    table.add_column("phase")
    table.add_column("avg sec", justify="right")
    table.add_column("avg MiB/s", justify="right")
    table.add_column("avg output", justify="right")
    table.add_column("ratio", justify="right")

    for row in sorted(
        stats_rows, key=lambda item: (item.mode, item.workers, item.phase, item.tool)
    ):
        table.add_row(
            row.mode,
            row.workers,
            row.tool,
            row.phase,
            f"{row.avg_seconds:.3f}",
            f"{row.avg_throughput_mib_s:.2f}",
            format_bytes(row.avg_output_bytes),
            f"{row.ratio:.3f}",
        )

    console.print(table)


def drop_caches() -> None:
    subprocess.run(["sync"], check=True)
    subprocess.run(["sudo", "sh", "-c", "echo 3 >/proc/sys/vm/drop_caches"], check=True)


def build_oxide(settings: Settings) -> None:
    if settings.oxide_bin.exists() and not settings.rebuild_oxide:
        print(f"--- Using existing Oxide binary: {settings.oxide_bin} ---")
        return

    print("--- Building Oxide ---")
    subprocess.run(
        ["cargo", "build", "--release", "-p", "oxide-cli"], check=True, cwd=BASE_DIR
    )


def step_key(step: Step, pass_num: int) -> str:
    return f"p{pass_num:02d}_{step.mode}_{step.workers}_{step.tool}_{step.phase}"


def oxide_archive_command(
    settings: Settings, mode: str, config: ModeConfig, workers: str
) -> list[str]:
    command = [
        str(settings.oxide_bin),
        "archive",
        str(settings.source),
        "--output",
        str(settings.oxide_output),
        "--preset",
        mode,
    ]
    if workers != "auto":
        command.extend(["--workers", workers])
    command.append("--telemetry-details")
    return command


def oxide_extract_command(
    settings: Settings, _: str, __: ModeConfig, workers: str
) -> list[str]:
    command = [
        str(settings.oxide_bin),
        "extract",
        str(settings.oxide_output),
        "--output",
        str(settings.oxide_extract_dir),
    ]
    if workers != "auto":
        command.extend(["--workers", workers])
    command.append("--telemetry-details")
    return command


def unsquashfs_extract_command(
    settings: Settings, _: str, __: ModeConfig, __workers: str
) -> list[str]:
    return [
        resolve_tool("unsquashfs"),
        "-q",
        "-f",
        "-no-xattrs",
        "-d",
        str(settings.squashfs_extract_dir),
        str(settings.squashfs_output),
    ]


def mksquashfs_command(
    settings: Settings, mode: str, config: ModeConfig, workers: str
) -> list[str]:
    block_size = settings.squashfs_block_size or config.block_size
    command = [
        resolve_tool("mksquashfs"),
        str(settings.source),
        str(settings.squashfs_output),
        "-comp",
        config.compression,
        "-b",
        block_size,
        "-processors",
        workers if workers != "auto" else str(settings.threads),
    ]
    if config.compression_level is not None:
        command.extend(["-Xcompression-level", config.compression_level])
    return command


def sevenzip_archive_command(
    settings: Settings, mode: str, config: ModeConfig, workers: str
) -> list[str]:
    mmt = workers if workers != "auto" else str(settings.threads)
    return [
        resolve_tool("7zz", "7z"),
        "a",
        "-t7z",
        "-mx=9",
        "-ms=on",
        "-m0=LZMA2",
        f"-md={config.block_size}",
        f"-mmt={mmt}",
        "-snl",  # Store Symbolic Links
        "-snh",  # Store Hard Links
        str(settings.squashfs_output.with_suffix(".7z")),
        str(settings.source),
    ]


def sevenzip_extract_command(
    settings: Settings, _: str, __: ModeConfig, __workers: str
) -> list[str]:
    command = [
        resolve_tool("7zz", "7z"),
        "x",
        str(settings.squashfs_output.with_suffix(".7z")),
        f"-o{settings.squashfs_extract_dir}",
        "-aoa",
        "-snl",  # Extract Symbolic Links natively
        "-snh",  # Extract Hard Links natively
    ]
    if __workers != "auto":
        command.insert(2, f"-mmt={__workers}")
    return command


def archive_path_for(tool: str, settings: Settings) -> Path:
    if tool == "oxide":
        return settings.oxide_output
    if tool == "mksquashfs":
        return settings.squashfs_output
    if tool == "7zz":
        return settings.squashfs_output.with_suffix(".7z")
    raise SystemExit(f"Unknown tool: {tool}")


def baseline_steps(
    settings: Settings, mode: str, config: ModeConfig, workers: str
) -> tuple[Step, Step]:
    tool = MODE_BASELINES[mode]
    output_path = archive_path_for(tool, settings)

    if tool == "mksquashfs":
        return (
            Step(tool, "archive", workers, output_path, mksquashfs_command),
            Step(
                "unsquashfs",
                "extract",
                workers,
                settings.squashfs_extract_dir,
                unsquashfs_extract_command,
                cleanup_targets=(output_path, settings.squashfs_extract_dir),
            ),
        )

    if tool == "7zz":
        return (
            Step(tool, "archive", workers, output_path, sevenzip_archive_command),
            Step(
                tool,
                "extract",
                workers,
                settings.squashfs_extract_dir,
                sevenzip_extract_command,
                cleanup_targets=(output_path, settings.squashfs_extract_dir),
            ),
        )

    raise SystemExit(f"Unsupported baseline tool for {mode}: {tool}")


def build_run_units(settings: Settings) -> list[RunUnit]:
    units: list[RunUnit] = []
    workers = str(settings.threads)
    for mode in settings.modes:
        mode_config = MODE_CONFIGS.get(mode)
        if mode_config is None:
            raise SystemExit(f"Unknown mode: {mode}")

        baseline_archive_step, baseline_extract_step = baseline_steps(
            settings, mode, mode_config, workers
        )
        oxide_archive_step = Step(
            "oxide",
            "archive",
            workers,
            settings.oxide_output,
            oxide_archive_command,
        )
        oxide_extract_step = Step(
            "oxide",
            "extract",
            workers,
            settings.oxide_extract_dir,
            oxide_extract_command,
            cleanup_targets=(settings.oxide_output, settings.oxide_extract_dir),
        )
        if settings.skip_extract:
            oxide_archive_step = Step(
                oxide_archive_step.tool,
                oxide_archive_step.phase,
                oxide_archive_step.workers,
                oxide_archive_step.output_path,
                oxide_archive_step.command_builder,
                cleanup_targets=(oxide_archive_step.output_path,),
            )
            baseline_archive_step = Step(
                baseline_archive_step.tool,
                baseline_archive_step.phase,
                baseline_archive_step.workers,
                baseline_archive_step.output_path,
                baseline_archive_step.command_builder,
                cleanup_targets=(baseline_archive_step.output_path,),
            )
            oxide_extract_step = None
            baseline_extract_step = None

        units.append(
            RunUnit(
                mode=mode,
                workers=workers,
                mode_config=mode_config,
                oxide_archive=oxide_archive_step,
                oxide_extract=oxide_extract_step,
                baseline_archive=baseline_archive_step,
                baseline_extract=baseline_extract_step,
            )
        )

    return units


def record_step(
    settings: Settings,
    step: Step,
    mode: str,
    mode_config: ModeConfig,
    pass_num: int,
    source_bytes: int,
    results: list[ResultRow],
    telemetry: TelemetryWriter,
) -> ResultRow:
    cleanup_path(step.output_path)

    command = step.command_builder(settings, mode, mode_config, step.workers)
    start_ns = time.perf_counter_ns()
    completed, host = run_command_with_host_telemetry(command, step.output_path)
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    elapsed_ns = time.perf_counter_ns() - start_ns

    host_payload = host_telemetry_payload(host, elapsed_ns)
    if completed.returncode != 0:
        row = ResultRow(
            tool=step.tool,
            phase=step.phase,
            mode=mode,
            workers=step.workers,
            pass_num=pass_num,
            elapsed_ns=elapsed_ns,
            throughput_mib_s=0.0,
            output_bytes=0,
            input_bytes=source_bytes,
            command=row_command(command),
            output_path=str(step.output_path),
            stdout_path="",
            stderr_path="",
            telemetry_path=None,
        )
        telemetry.write_step_artifacts(
            row,
            stdout,
            stderr,
            host_payload,
            parse_oxide_telemetry(stdout) if step.tool == "oxide" else None,
        )
        for target in step.cleanup_targets:
            cleanup_path(target)
        raise subprocess.CalledProcessError(
            completed.returncode,
            command,
            output=stdout,
            stderr=stderr,
        )

    elapsed_s = elapsed_ns / 1_000_000_000
    throughput_mib_s = (
        0.0 if elapsed_s <= 0 else (source_bytes / 1024 / 1024) / elapsed_s
    )
    output_bytes = apparent_size(step.output_path)

    parsed_telemetry = parse_oxide_telemetry(stdout) if step.tool == "oxide" else None
    row = ResultRow(
        tool=step.tool,
        phase=step.phase,
        mode=mode,
        workers=step.workers,
        pass_num=pass_num,
        elapsed_ns=elapsed_ns,
        throughput_mib_s=throughput_mib_s,
        output_bytes=output_bytes,
        input_bytes=source_bytes,
        command=row_command(command),
        output_path=str(step.output_path),
        stdout_path="",
        stderr_path="",
        telemetry_path=None,
    )

    stdout_path, stderr_path, telemetry_path = telemetry.write_step_artifacts(
        row,
        stdout,
        stderr,
        host_payload,
        parsed_telemetry,
    )

    row = ResultRow(
        tool=row.tool,
        phase=row.phase,
        mode=row.mode,
        workers=row.workers,
        pass_num=row.pass_num,
        elapsed_ns=row.elapsed_ns,
        throughput_mib_s=row.throughput_mib_s,
        output_bytes=row.output_bytes,
        input_bytes=row.input_bytes,
        command=row.command,
        output_path=row.output_path,
        stdout_path=str(stdout_path),
        stderr_path=str(stderr_path),
        telemetry_path=str(telemetry_path) if telemetry_path else None,
    )

    telemetry.write_step(row, stdout_path, stderr_path, host_payload, telemetry_path)

    results.append(row)

    for target in step.cleanup_targets:
        cleanup_path(target)

    return row


def print_results_table(results: Sequence[ResultRow], source_bytes: int) -> None:
    modes = tuple(dict.fromkeys(row.mode for row in results))
    console = Console(width=130)
    stats_rows = result_to_stats(results, source_bytes)
    worker_modes = tuple(dict.fromkeys(row.workers for row in results))
    comparisons = mode_comparisons(stats_rows, modes, worker_modes)
    print_steps_table(console, results, source_bytes)
    print_analysis(console, stats_rows, comparisons)
    print_averages(console, stats_rows)


def run_bench_with_telemetry(
    settings: Settings,
    source_bytes: int,
    results: list[ResultRow],
    telemetry: TelemetryWriter,
    console: Console,
    telemetry_run_dir: Path,
) -> None:
    run_units = build_run_units(settings)

    telemetry.write_run(
        {
            "run_id": telemetry_run_dir.name,
            "started_at": utc_now(),
            "source": str(settings.source),
            "source_bytes": source_bytes,
            "oxide_bin": str(settings.oxide_bin),
            "oxide_output": str(settings.oxide_output),
            "squashfs_output": str(settings.squashfs_output),
            "threads": settings.threads,
            "passes": settings.passes,
            "skip_extract": settings.skip_extract,
            "rebuild_oxide": settings.rebuild_oxide,
            "modes": list(settings.modes),
            "workers": settings.threads,
            "shuffle_seed": settings.shuffle_seed,
            "squashfs_block_size": settings.squashfs_block_size,
            "host_telemetry": True,
            "host_telemetry_sample_interval_s": 0.05,
            "telemetry_dir": str(telemetry_run_dir),
        }
    )

    rng = random.Random(settings.shuffle_seed)

    for pass_num in range(1, settings.passes + 1):
        console.rule(f"Pass {pass_num}/{settings.passes}")

        pass_units = list(run_units)
        rng.shuffle(pass_units)

        for unit in pass_units:
            console.rule(f"Mode {unit.mode} / workers {unit.workers}")

            tool_groups: list[tuple[str, Step, Step | None]] = [
                ("oxide", unit.oxide_archive, unit.oxide_extract),
                (
                    MODE_BASELINES[unit.mode],
                    unit.baseline_archive,
                    unit.baseline_extract,
                ),
            ]
            rng.shuffle(tool_groups)

            for _tool_name, archive_step, extract_step in tool_groups:
                if settings.drop_caches:
                    drop_caches()
                console.print(
                    f"[bold]{archive_step.tool} archive[/bold] ({unit.mode}, workers={unit.workers})"
                )
                record_step(
                    settings,
                    archive_step,
                    unit.mode,
                    unit.mode_config,
                    pass_num,
                    source_bytes,
                    results,
                    telemetry,
                )

                if extract_step is not None:
                    if settings.drop_caches:
                        drop_caches()
                    console.print(
                        f"[bold]{extract_step.tool} extract[/bold] ({unit.mode}, workers={unit.workers})"
                    )
                    record_step(
                        settings,
                        extract_step,
                        unit.mode,
                        unit.mode_config,
                        pass_num,
                        source_bytes,
                        results,
                        telemetry,
                    )

                console.print("")


def main() -> int:
    settings = parse_args()

    if not settings.source.exists():
        raise SystemExit(f"Source path does not exist: {settings.source}")

    source_bytes = source_size(settings.source)
    results: list[ResultRow] = []
    exit_code = 0
    console = Console(width=130)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    telemetry_run_dir = settings.telemetry_dir / run_id

    try:
        print_run_header(console, settings, telemetry_run_dir, source_bytes)
        build_oxide(settings)
        with TelemetryWriter(telemetry_run_dir) as telemetry:
            run_bench_with_telemetry(
                settings,
                source_bytes,
                results,
                telemetry,
                console,
                telemetry_run_dir,
            )
    except Exception as exc:
        exit_code = 1
        print(f"\nBenchmark aborted: {exc}", file=sys.stderr)
    finally:
        if results:
            print_results_table(results, source_bytes)

            if telemetry_run_dir.exists():
                stats_rows = result_to_stats(results, source_bytes)
                comparisons = mode_comparisons(
                    stats_rows,
                    tuple(dict.fromkeys(row.mode for row in results)),
                    tuple(dict.fromkeys(row.workers for row in results)),
                )
                telemetry = TelemetryWriter(telemetry_run_dir)
                telemetry.write_summary(
                    {
                        "completed_at": utc_now(),
                        "results": [
                            {
                                "tool": row.tool,
                                "phase": row.phase,
                                "mode": row.mode,
                                "workers": row.workers,
                                "avg_seconds": row.avg_seconds,
                                "avg_throughput_mib_s": row.avg_throughput_mib_s,
                                "avg_output_bytes": row.avg_output_bytes,
                                "ratio": row.ratio,
                            }
                            for row in stats_rows
                        ],
                        "comparisons": [
                            comparison.__dict__ for comparison in comparisons
                        ],
                        "shuffle_seed": settings.shuffle_seed,
                        "workers": settings.threads,
                        "host_telemetry": True,
                    }
                )

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

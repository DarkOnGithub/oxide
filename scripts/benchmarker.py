#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import shlex
import statistics
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence
import sys

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
    oxide_block_size: str = "1M"


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
    rebuild_oxide: bool
    modes: tuple[str, ...]
    squashfs_block_size: str = "1048576"
    telemetry_dir: Path = BASE_DIR / "benchmark_telemetry"


@dataclass(frozen=True)
class ResultRow:
    tool: str
    phase: str
    mode: str
    pass_num: int
    elapsed_ns: int
    throughput_mib_s: float
    output_bytes: int
    input_bytes: int
    command: str
    output_path: str


@dataclass(frozen=True)
class ModeStats:
    tool: str
    phase: str
    mode: str
    avg_seconds: float
    avg_throughput_mib_s: float
    avg_output_bytes: int
    ratio: float


@dataclass(frozen=True)
class ModeComparison:
    mode: str
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
    output_path: Path
    command_builder: Callable[[Settings, str, ModeConfig], Sequence[str]]
    cleanup_targets: tuple[Path, ...] = ()


MODE_CONFIGS: dict[str, ModeConfig] = {
    "fast": ModeConfig(
        compression="lz4", compression_level=None, oxide_block_size="1M"
    ),
    "balanced": ModeConfig(
        compression="zstd", compression_level="6", oxide_block_size="2M"
    ),
    "ultra": ModeConfig(
        compression="lzma", compression_level="9", oxide_block_size="8M"
    ),
    "extreme": ModeConfig(
        compression="lzma", compression_level="9", oxide_block_size="16M"
    ),
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
        "--passes", type=int, default=int(env_value("BENCHMARK_PASSES", "2"))
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        default=env_value("BENCHMARK_SKIP_EXTRACT", "0") == "1",
    )
    parser.add_argument(
        "--rebuild-oxide",
        action="store_true",
        default=env_value("BENCHMARK_REBUILD_OXIDE", "0") == "1",
        help="Rebuild oxide before benchmarking instead of using the existing binary.",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        default=["fast", "balanced", "ultra", "extreme"],
    )
    parser.add_argument(
        "--squashfs-block-size",
        default=env_value("BENCHMARK_SQUASHFS_BLOCK_SIZE", "1048576"),
    )
    parser.add_argument(
        "--telemetry-dir",
        default=env_value(
            "BENCHMARK_TELEMETRY_DIR", str(BASE_DIR / "benchmark_telemetry")
        ),
        help="Directory where JSONL/CSV telemetry for each run is written.",
    )

    args = parser.parse_args()

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
        rebuild_oxide=args.rebuild_oxide,
        modes=tuple(args.modes),
        squashfs_block_size=args.squashfs_block_size,
        telemetry_dir=Path(args.telemetry_dir),
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
    if not path.exists():
        return 0
    if path.is_file() or path.is_symlink():
        return path.stat().st_size

    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            file_path = Path(root) / name
            try:
                total += file_path.stat().st_size
            except FileNotFoundError:
                continue
    return total


def source_size(path: Path) -> int:
    if path.is_file() or path.is_symlink():
        return path.stat().st_size
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


def mean_bytes(rows: Sequence[ResultRow]) -> int:
    return round(statistics.mean(row.output_bytes for row in rows))


class TelemetryWriter:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.steps_jsonl = run_dir / "steps.jsonl"
        self.steps_csv = run_dir / "steps.csv"
        self.run_json = run_dir / "run.json"
        self.summary_json = run_dir / "summary.json"
        self._jsonl_fp = None
        self._csv_fp = None
        self._csv_writer = None

    def __enter__(self) -> "TelemetryWriter":
        self.run_dir.mkdir(parents=True, exist_ok=True)
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
            "pass_num",
            "elapsed_ns",
            "elapsed_s",
            "throughput_mib_s",
            "input_bytes",
            "output_bytes",
            "ratio",
            "output_path",
            "command",
        ]

    def write_json(self, path: Path, payload: dict[str, object]) -> None:
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def write_run(self, payload: dict[str, object]) -> None:
        self.write_json(self.run_json, payload)

    def write_summary(self, payload: dict[str, object]) -> None:
        self.write_json(self.summary_json, payload)

    def write_step(self, row: ResultRow) -> None:
        record = {
            "event": "step",
            "timestamp": utc_now(),
            "tool": row.tool,
            "phase": row.phase,
            "mode": row.mode,
            "pass_num": row.pass_num,
            "elapsed_ns": row.elapsed_ns,
            "elapsed_s": row.elapsed_ns / 1_000_000_000,
            "throughput_mib_s": row.throughput_mib_s,
            "input_bytes": row.input_bytes,
            "output_bytes": row.output_bytes,
            "ratio": safe_ratio(row.output_bytes, row.input_bytes),
            "output_path": row.output_path,
            "command": row.command,
        }
        assert self._jsonl_fp is not None and self._csv_writer is not None
        self._jsonl_fp.write(json.dumps(record, sort_keys=True) + "\n")
        self._jsonl_fp.flush()
        self._csv_writer.writerow(record)
        self._csv_fp.flush()  # type: ignore[union-attr]


def row_command(command: Sequence[str]) -> str:
    return shlex.join([str(part) for part in command])


def result_to_stats(rows: Sequence[ResultRow], source_bytes: int) -> list[ModeStats]:
    grouped: dict[tuple[str, str, str], list[ResultRow]] = {}
    for row in rows:
        grouped.setdefault((row.tool, row.phase, row.mode), []).append(row)

    stats_rows: list[ModeStats] = []
    for tool, phase, mode in sorted(grouped):
        group = grouped[(tool, phase, mode)]
        avg_seconds = statistics.mean(row.elapsed_ns for row in group) / 1_000_000_000
        avg_throughput = statistics.mean(row.throughput_mib_s for row in group)
        avg_output = mean_bytes(group)
        stats_rows.append(
            ModeStats(
                tool=tool,
                phase=phase,
                mode=mode,
                avg_seconds=avg_seconds,
                avg_throughput_mib_s=avg_throughput,
                avg_output_bytes=avg_output,
                ratio=safe_ratio(avg_output, source_bytes),
            )
        )
    return stats_rows


def stats_lookup(
    stats_rows: Sequence[ModeStats],
) -> dict[tuple[str, str, str], ModeStats]:
    return {(row.tool, row.phase, row.mode): row for row in stats_rows}


def mode_comparisons(
    stats_rows: Sequence[ModeStats], modes: Sequence[str]
) -> list[ModeComparison]:
    lookup = stats_lookup(stats_rows)
    comparisons: list[ModeComparison] = []

    for mode in modes:
        baseline_tool = MODE_BASELINES[mode]
        oxide_archive = lookup[("oxide", "archive", mode)]
        baseline_archive = lookup[(baseline_tool, "archive", mode)]
        oxide_extract = lookup.get(("oxide", "extract", mode))
        baseline_extract = lookup.get((baseline_tool, "extract", mode))

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

    best_archive = min(comparisons, key=lambda item: abs(item.archive_delta_pct))
    extract_candidates = [
        item for item in comparisons if item.extract_delta_pct is not None
    ]
    best_ratio = min(comparisons, key=lambda item: item.oxide_archive_ratio)

    takes.append(
        f"Archive gap is smallest in {best_archive.mode} ({best_archive.archive_delta_pct:+.2f}% vs baseline)."
    )
    if extract_candidates:
        best_extract = min(
            extract_candidates, key=lambda item: abs(item.extract_delta_pct or 0.0)
        )
        takes.append(
            f"Extraction gap is smallest in {best_extract.mode} ({best_extract.extract_delta_pct:+.2f}% vs baseline)."
        )
    takes.append(
        f"Best Oxide archive ratio appears in {best_ratio.mode} ({best_ratio.oxide_archive_ratio:.3f})."
    )
    return takes


def print_run_header(
    console: Console, settings: Settings, telemetry_run_dir: Path, source_bytes: int
) -> None:
    panel = Panel.fit(
        Text.from_markup(
            f"[bold]Benchmark run[/bold]\n"
            f"source: [cyan]{settings.source}[/cyan]\n"
            f"source size: [cyan]{format_bytes(source_bytes)}[/cyan]\n"
            f"passes: [cyan]{settings.passes}[/cyan]  threads: [cyan]{settings.threads}[/cyan]\n"
            f"telemetry: [cyan]{telemetry_run_dir}[/cyan]"
        ),
        title="Oxide benchmark",
        border_style="blue",
    )
    console.print(panel)


def print_steps_table(
    console: Console, results: Sequence[ResultRow], source_bytes: int
) -> None:
    table = Table(title="Step results", box=box.SIMPLE_HEAVY)
    table.add_column("tool", style="bold")
    table.add_column("phase")
    table.add_column("mode")
    table.add_column("pass", justify="right")
    table.add_column("seconds", justify="right")
    table.add_column("MiB/s", justify="right")
    table.add_column("output", justify="right")
    table.add_column("ratio", justify="right")
    table.add_column("vs source", justify="right")

    fastest = min(results, key=lambda row: row.elapsed_ns) if results else None
    smallest = min(results, key=lambda row: row.output_bytes) if results else None

    for row in results:
        ratio = safe_ratio(row.output_bytes, row.input_bytes)
        style = None
        if fastest is row:
            style = "green"
        elif smallest is row:
            style = "cyan"

        table.add_row(
            row.tool,
            row.phase,
            row.mode,
            str(row.pass_num),
            f"{row.elapsed_ns / 1_000_000_000:.3f}",
            f"{row.throughput_mib_s:.2f}",
            format_bytes(row.output_bytes),
            f"{ratio:.3f}",
            f"{percent_delta(row.output_bytes, source_bytes):+.2f}%",
            style=style,
        )

    console.print(table)


def print_analysis(console: Console, comparisons: Sequence[ModeComparison]) -> None:
    for comparison in comparisons:
        baseline_tool = MODE_BASELINES.get(comparison.mode, "baseline")
        table = Table(box=box.SIMPLE_HEAVY, show_header=True)
        table.add_column("phase", style="bold")
        table.add_column("winner")
        table.add_column("Δ", justify="right")
        table.add_column("oxide ratio", justify="right")
        table.add_column(f"{baseline_tool} ratio", justify="right")

        table.add_row(
            "archive",
            comparison.archive_fastest_tool,
            f"{comparison.archive_delta_pct:+.2f}%",
            f"{comparison.oxide_archive_ratio:.3f}",
            f"{comparison.baseline_archive_ratio:.3f}",
        )

        table.add_row(
            "extract",
            comparison.extract_fastest_tool or "n/a",
            (
                f"{comparison.extract_delta_pct:+.2f}%"
                if comparison.extract_delta_pct is not None
                else "n/a"
            ),
            (
                f"{comparison.oxide_extract_ratio:.3f}"
                if comparison.oxide_extract_ratio is not None
                else "n/a"
            ),
            (
                f"{comparison.baseline_extract_ratio:.3f}"
                if comparison.baseline_extract_ratio is not None
                else "n/a"
            ),
        )

        console.print(
            Panel(
                table,
                title=f"{comparison.mode} comparison ({baseline_tool})",
                border_style="magenta",
            )
        )

    takes = build_takes(comparisons)
    if takes:
        take_text = "\n".join(f"• {take}" for take in takes)
        console.print(Panel(take_text, title="Takeaways", border_style="magenta"))


def print_averages(console: Console, stats_rows: Sequence[ModeStats]) -> None:
    table = Table(title="Averages", box=box.SIMPLE_HEAVY)
    table.add_column("tool", style="bold")
    table.add_column("phase")
    table.add_column("mode")
    table.add_column("avg sec", justify="right")
    table.add_column("avg MiB/s", justify="right")
    table.add_column("avg output", justify="right")
    table.add_column("ratio", justify="right")

    for row in sorted(stats_rows, key=lambda item: (item.tool, item.phase, item.mode)):
        table.add_row(
            row.tool,
            row.phase,
            row.mode,
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


def run_command(command: Sequence[str]) -> None:
    subprocess.run(list(command), check=True, cwd=BASE_DIR)


def oxide_archive_command(
    settings: Settings, mode: str, config: ModeConfig
) -> list[str]:
    return [
        str(settings.oxide_bin),
        "archive",
        str(settings.source),
        "--output",
        str(settings.oxide_output),
        "--preset",
        mode,
        "--block-size",
        config.oxide_block_size,
        "--workers",
        str(settings.threads),
    ]


def oxide_extract_command(settings: Settings, _: str, __: ModeConfig) -> list[str]:
    return [
        str(settings.oxide_bin),
        "extract",
        str(settings.oxide_output),
        "--output",
        str(settings.oxide_extract_dir),
        "--workers",
        str(settings.threads),
    ]


def unsquashfs_extract_command(settings: Settings, _: str, __: ModeConfig) -> list[str]:
    return [
        resolve_tool("unsquashfs"),
        "-q",
        "-f",
        "-no-xattrs",
        "-d",
        str(settings.squashfs_extract_dir),
        str(settings.squashfs_output),
    ]


def mksquashfs_command(settings: Settings, _: str, config: ModeConfig) -> list[str]:
    command = [
        resolve_tool("mksquashfs"),
        str(settings.source),
        str(settings.squashfs_output),
        "-comp",
        config.compression,
        "-b",
        settings.squashfs_block_size,
        "-processors",
        str(settings.threads),
    ]
    if config.compression_level is not None:
        command.extend(["-Xcompression-level", config.compression_level])
    return command


def sevenzip_archive_command(
    settings: Settings, _: str, config: ModeConfig
) -> list[str]:
    return [
        resolve_tool("7zz", "7z"),
        "a",
        "-t7z",
        "-mx=9",
        "-ms=on",
        "-m0=LZMA2",
        f"-md={config.oxide_block_size}",
        f"-mmt={settings.threads}",
        str(settings.squashfs_output.with_suffix(".7z")),
        str(settings.source),
    ]


def sevenzip_extract_command(settings: Settings, _: str, __: ModeConfig) -> list[str]:
    return [
        resolve_tool("7zz", "7z"),
        "x",
        str(settings.squashfs_output.with_suffix(".7z")),
        f"-o{settings.squashfs_extract_dir}",
        "-aoa",
    ]


def archive_path_for(tool: str, settings: Settings) -> Path:
    if tool == "oxide":
        return settings.oxide_output
    if tool == "mksquashfs":
        return settings.squashfs_output
    if tool == "7zz":
        return settings.squashfs_output.with_suffix(".7z")
    raise SystemExit(f"Unknown tool: {tool}")


def baseline_steps(
    settings: Settings, mode: str, config: ModeConfig
) -> tuple[Step, Step]:
    tool = MODE_BASELINES[mode]
    output_path = archive_path_for(tool, settings)

    if tool == "mksquashfs":
        return (
            Step(tool, "archive", output_path, mksquashfs_command),
            Step(
                "unsquashfs",
                "extract",
                settings.squashfs_extract_dir,
                unsquashfs_extract_command,
                cleanup_targets=(output_path, settings.squashfs_extract_dir),
            ),
        )

    if tool == "7zz":
        return (
            Step(tool, "archive", output_path, sevenzip_archive_command),
            Step(
                tool,
                "extract",
                settings.squashfs_extract_dir,
                sevenzip_extract_command,
                cleanup_targets=(output_path, settings.squashfs_extract_dir),
            ),
        )

    raise SystemExit(f"Unsupported baseline tool for {mode}: {tool}")


def record_step(
    settings: Settings,
    step: Step,
    mode: str,
    mode_config: ModeConfig,
    pass_num: int,
    source_bytes: int,
    results: list[ResultRow],
) -> None:
    cleanup_path(step.output_path)

    command = step.command_builder(settings, mode, mode_config)
    start_ns = time.perf_counter_ns()
    run_command(command)
    elapsed_ns = time.perf_counter_ns() - start_ns

    elapsed_s = elapsed_ns / 1_000_000_000
    throughput_mib_s = (
        0.0 if elapsed_s <= 0 else (source_bytes / 1024 / 1024) / elapsed_s
    )
    output_bytes = apparent_size(step.output_path)

    results.append(
        ResultRow(
            tool=step.tool,
            phase=step.phase,
            mode=mode,
            pass_num=pass_num,
            elapsed_ns=elapsed_ns,
            throughput_mib_s=throughput_mib_s,
            output_bytes=output_bytes,
            input_bytes=source_bytes,
            command=row_command(command),
            output_path=str(step.output_path),
        )
    )

    for target in step.cleanup_targets:
        cleanup_path(target)


def print_results_table(results: Sequence[ResultRow], source_bytes: int) -> None:
    modes = tuple(dict.fromkeys(row.mode for row in results))
    console = Console(width=130)
    stats_rows = result_to_stats(results, source_bytes)
    comparisons = mode_comparisons(stats_rows, modes)
    print_steps_table(console, results, source_bytes)
    print_analysis(console, comparisons)
    print_averages(console, stats_rows)


def run_bench_with_telemetry(
    settings: Settings,
    source_bytes: int,
    results: list[ResultRow],
    telemetry: TelemetryWriter,
    console: Console,
    telemetry_run_dir: Path,
) -> None:
    mode_lookup = {name: MODE_CONFIGS.get(name) for name in settings.modes}

    unknown_modes = [name for name, config in mode_lookup.items() if config is None]
    if unknown_modes:
        raise SystemExit(f"Unknown mode(s): {', '.join(unknown_modes)}")

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
            "telemetry_dir": str(telemetry_run_dir),
        }
    )

    for mode in settings.modes:
        mode_config = mode_lookup[mode]
        assert mode_config is not None
        baseline_archive_step, baseline_extract_step = baseline_steps(
            settings, mode, mode_config
        )
        oxide_archive_step = Step(
            "oxide", "archive", settings.oxide_output, oxide_archive_command
        )
        oxide_extract_step = Step(
            "oxide",
            "extract",
            settings.oxide_extract_dir,
            oxide_extract_command,
            cleanup_targets=(settings.oxide_output, settings.oxide_extract_dir),
        )

        console.rule(f"Testing mode: {mode}")

        for pass_num in range(1, settings.passes + 1):
            console.print(f"[bold]Pass {pass_num}/{settings.passes}[/bold]")

            drop_caches()
            console.print(f"[blue]Oxide archive[/blue] ({mode})")
            record_step(
                settings,
                oxide_archive_step,
                mode,
                mode_config,
                pass_num,
                source_bytes,
                results,
            )
            telemetry.write_step(results[-1])

            if settings.skip_extract:
                cleanup_path(settings.oxide_output)
            else:
                drop_caches()
                console.print(f"[blue]Oxide extract[/blue] ({mode})")
                record_step(
                    settings,
                    oxide_extract_step,
                    mode,
                    mode_config,
                    pass_num,
                    source_bytes,
                    results,
                )
                telemetry.write_step(results[-1])

            console.print("")

            drop_caches()
            console.print(
                f"[yellow]{baseline_archive_step.tool} archive[/yellow] ({mode} equivalent)"
            )
            record_step(
                settings,
                baseline_archive_step,
                mode,
                mode_config,
                pass_num,
                source_bytes,
                results,
            )
            telemetry.write_step(results[-1])

            if settings.skip_extract:
                cleanup_path(baseline_archive_step.output_path)
            else:
                drop_caches()
                console.print(
                    f"[yellow]{baseline_extract_step.tool} extract[/yellow] (from {baseline_archive_step.tool}, {mode} equivalent)"
                )
                record_step(
                    settings,
                    baseline_extract_step,
                    mode,
                    mode_config,
                    pass_num,
                    source_bytes,
                    results,
                )
                telemetry.write_step(results[-1])

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
                    stats_rows, tuple(dict.fromkeys(row.mode for row in results))
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
                    }
                )

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

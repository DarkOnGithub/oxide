#!/usr/bin/env python3

from __future__ import annotations

import os
import shutil
import stat as statmod
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: rich. benchmarker.py already requires it; run via `uv run` or install rich first."
    ) from exc


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_TELEMETRY_ROOT = (
    BASE_DIR
    / "benchmark_telemetry"
    / f"named_benchmark_batch_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
)
MIB = 1024 * 1024
COMMON_ARGS: tuple[str, ...] = (
    "--worker-modes",
    "auto",
    "--raw-oxide-presets",
    "--sync-after-extract",
    "--competitors",
    "mode-defaults",
    "--quiet",
)


@dataclass(frozen=True)
class BenchmarkJob:
    source_dir: Path
    passes: int
    name: str


@dataclass(frozen=True)
class RunnerConfig:
    oxide_bin: Path
    benchmarker: Path
    telemetry_root: Path
    jobs: tuple[BenchmarkJob, ...]


@dataclass(frozen=True)
class BenchmarkResult:
    job: BenchmarkJob
    source_bytes: int
    telemetry_dir: Path
    log_path: Path
    elapsed_seconds: float
    exit_code: int
    command: tuple[str, ...]

    @property
    def seconds_per_iter(self) -> float:
        return self.elapsed_seconds / self.job.passes

    @property
    def throughput_mib_s(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return ((self.source_bytes * self.job.passes) / MIB) / self.elapsed_seconds


def default_jobs() -> tuple[BenchmarkJob, ...]:
    return (
        BenchmarkJob(BASE_DIR / "datasets/200mb", 5, "dataset_200mb"),
        BenchmarkJob(
            BASE_DIR / "datasets/6gb/linux", 4, "dataset_6gb_linux"
        ),
        BenchmarkJob(BASE_DIR / "datasets/6gb", 3, "dataset_6gb"),
    )


def env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    if not raw:
        return default
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    return (Path.cwd() / path).resolve()


def load_config() -> RunnerConfig:
    return RunnerConfig(
        oxide_bin=env_path("OXIDE_BIN", BASE_DIR / "target/release/oxide"),
        benchmarker=env_path("BENCHMARKER", BASE_DIR / "scripts/benchmarker.py"),
        telemetry_root=env_path("TELEMETRY_ROOT", DEFAULT_TELEMETRY_ROOT),
        jobs=default_jobs(),
    )


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return str(path)


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


def format_seconds(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def source_size(path: Path) -> int:
    try:
        st = path.lstat()
    except OSError as exc:
        raise SystemExit(f"Failed to stat {path}: {exc}") from exc

    if statmod.S_ISREG(st.st_mode) or statmod.S_ISLNK(st.st_mode):
        return st.st_size

    total = 0
    for root, dir_names, file_names in os.walk(path, followlinks=False):
        root_path = Path(root)

        for dir_name in list(dir_names):
            dir_path = root_path / dir_name
            try:
                dir_stat = dir_path.lstat()
            except OSError:
                continue
            if statmod.S_ISLNK(dir_stat.st_mode):
                total += dir_stat.st_size

        for file_name in file_names:
            file_path = root_path / file_name
            try:
                file_stat = file_path.lstat()
            except OSError:
                continue
            if statmod.S_ISREG(file_stat.st_mode) or statmod.S_ISLNK(file_stat.st_mode):
                total += file_stat.st_size

    return total


def launcher_command(benchmarker: Path) -> list[str]:
    uv = shutil.which("uv")
    if uv is not None:
        return [uv, "run", str(benchmarker)]
    python = shutil.which("python3") or sys.executable
    return [python, str(benchmarker)]


def benchmark_command(config: RunnerConfig, job: BenchmarkJob, out_dir: Path) -> list[str]:
    return [
        *launcher_command(config.benchmarker),
        "--source",
        str(job.source_dir),
        "--passes",
        str(job.passes),
        "--telemetry-dir",
        str(out_dir),
        "--oxide-bin",
        str(config.oxide_bin),
        *COMMON_ARGS,
    ]


def rolling_seconds_per_iter(results: Sequence[BenchmarkResult]) -> float | None:
    total_passes = sum(result.job.passes for result in results)
    if total_passes == 0:
        return None
    return sum(result.elapsed_seconds for result in results) / total_passes


def log_tail(path: Path, max_lines: int = 40) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "<unable to read log>"
    tail = lines[-max_lines:]
    return "\n".join(tail) if tail else "<log is empty>"


def build_plan_table(config: RunnerConfig, sizes: dict[str, int]) -> Table:
    table = Table(title="Benchmark plan")
    table.add_column("Benchmark")
    table.add_column("Source")
    table.add_column("Size", justify="right")
    table.add_column("Passes", justify="right")
    table.add_column("Work", justify="right")

    for job in config.jobs:
        source_bytes = sizes[job.name]
        table.add_row(
            job.name,
            display_path(job.source_dir),
            format_bytes(source_bytes),
            str(job.passes),
            format_bytes(source_bytes * job.passes),
        )
    return table


def build_results_table(results: Sequence[BenchmarkResult]) -> Table:
    table = Table(title="Completed benchmarks")
    table.add_column("Benchmark")
    table.add_column("Size", justify="right")
    table.add_column("Passes", justify="right")
    table.add_column("Elapsed", justify="right")
    table.add_column("Time/iter", justify="right")
    table.add_column("Throughput", justify="right")
    table.add_column("Telemetry")

    for result in results:
        table.add_row(
            result.job.name,
            format_bytes(result.source_bytes),
            str(result.job.passes),
            format_seconds(result.elapsed_seconds),
            f"{result.seconds_per_iter:.2f}s",
            f"{result.throughput_mib_s:.2f} MiB/s",
            display_path(result.telemetry_dir),
        )

    return table


def build_totals_table(results: Sequence[BenchmarkResult]) -> Table:
    table = Table(title="Batch totals")
    table.add_column("Metric")
    table.add_column("Value", justify="right")

    total_elapsed = sum(result.elapsed_seconds for result in results)
    total_passes = sum(result.job.passes for result in results)
    total_work = sum(result.source_bytes * result.job.passes for result in results)
    avg_seconds_per_iter = total_elapsed / total_passes if total_passes else 0.0
    throughput = (total_work / MIB) / total_elapsed if total_elapsed > 0 else 0.0

    table.add_row("Benchmarks", str(len(results)))
    table.add_row("Total passes", str(total_passes))
    table.add_row("Total elapsed", format_seconds(total_elapsed))
    table.add_row("Avg time/iter", f"{avg_seconds_per_iter:.2f}s")
    table.add_row("Total work", format_bytes(total_work))
    table.add_row("Effective throughput", f"{throughput:.2f} MiB/s")
    return table


def validate_config(config: RunnerConfig) -> None:
    if not config.benchmarker.is_file():
        raise SystemExit(f"Benchmarker not found: {config.benchmarker}")
    missing_sources = [job.source_dir for job in config.jobs if not job.source_dir.exists()]
    if missing_sources:
        joined = ", ".join(str(path) for path in missing_sources)
        raise SystemExit(f"Missing benchmark source(s): {joined}")


def run_job(
    console: Console,
    progress: Progress,
    overall_task: int,
    current_task: int,
    config: RunnerConfig,
    job: BenchmarkJob,
    source_bytes: int,
    completed_results: list[BenchmarkResult],
    completed_passes: int,
    total_passes: int,
) -> BenchmarkResult:
    out_dir = config.telemetry_root / job.name
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "runner.log"
    command = benchmark_command(config, job, out_dir)

    console.print(
        Panel.fit(
            "\n".join(
                [
                    f"[bold]Running[/bold] {job.name}",
                    f"Source: {display_path(job.source_dir)}",
                    f"Size: {format_bytes(source_bytes)}",
                    f"Passes: {job.passes}",
                    f"Telemetry: {display_path(out_dir)}",
                    f"Log: {display_path(log_path)}",
                ]
            )
        )
    )

    started = time.monotonic()
    average_seconds_per_iter = rolling_seconds_per_iter(completed_results)
    progress.update(
        current_task,
        total=None,
        completed=0,
        description=(
            f"{job.name} | {job.passes} iters | elapsed 00:00 | time/iter --"
        ),
    )

    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write("$ " + " ".join(command) + "\n\n")
        log_file.flush()

        process = subprocess.Popen(
            command,
            cwd=BASE_DIR,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )

        while True:
            exit_code = process.poll()
            elapsed = time.monotonic() - started
            current_seconds_per_iter = elapsed / job.passes if job.passes else 0.0

            partial_passes = 0.0
            if average_seconds_per_iter and average_seconds_per_iter > 0:
                partial_passes = min(job.passes * 0.98, elapsed / average_seconds_per_iter)

            progress.update(
                overall_task,
                completed=completed_passes + partial_passes,
                description=(
                    f"batch progress | passes {completed_passes}/{total_passes}"
                    + (
                        f" | avg {rolling_seconds_per_iter(completed_results):.2f}s/iter"
                        if completed_results
                        else ""
                    )
                ),
            )
            progress.update(
                current_task,
                description=(
                    f"{job.name} | {job.passes} iters | elapsed {format_seconds(elapsed)}"
                    f" | time/iter {current_seconds_per_iter:.2f}s"
                ),
            )

            if exit_code is not None:
                elapsed = time.monotonic() - started
                progress.update(overall_task, completed=completed_passes + job.passes)
                progress.update(
                    current_task,
                    description=(
                        f"{job.name} | done in {format_seconds(elapsed)}"
                        f" | time/iter {elapsed / job.passes:.2f}s"
                    ),
                )
                return BenchmarkResult(
                    job=job,
                    source_bytes=source_bytes,
                    telemetry_dir=out_dir,
                    log_path=log_path,
                    elapsed_seconds=elapsed,
                    exit_code=exit_code,
                    command=tuple(command),
                )

            time.sleep(0.5)


def main() -> int:
    console = Console(width=140)
    config = load_config()
    validate_config(config)
    config.telemetry_root.mkdir(parents=True, exist_ok=True)

    source_sizes = {job.name: source_size(job.source_dir) for job in config.jobs}
    total_passes = sum(job.passes for job in config.jobs)
    results: list[BenchmarkResult] = []

    console.print(
        Panel.fit(
            "\n".join(
                [
                    f"Oxide bin: {display_path(config.oxide_bin)}",
                    f"Benchmarker: {display_path(config.benchmarker)}",
                    f"Telemetry root: {display_path(config.telemetry_root)}",
                    "Common args: --worker-modes 16 --threads 16 --raw-oxide-presets --sync-after-extract --quiet",
                ]
            ),
            title="Benchmark runner",
        )
    )
    console.print(build_plan_table(config, source_sizes))
    console.print()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=36),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        expand=True,
    ) as progress:
        overall_task = progress.add_task(
            "batch progress",
            total=total_passes,
            completed=0,
        )
        current_task = progress.add_task("waiting", total=None)
        completed_passes = 0

        for job in config.jobs:
            result = run_job(
                console=console,
                progress=progress,
                overall_task=overall_task,
                current_task=current_task,
                config=config,
                job=job,
                source_bytes=source_sizes[job.name],
                completed_results=results,
                completed_passes=completed_passes,
                total_passes=total_passes,
            )
            if result.exit_code != 0:
                console.print(f"\n[red]Benchmark failed:[/red] {job.name} (exit {result.exit_code})")
                console.print(f"Log: {display_path(result.log_path)}\n")
                console.print(Panel(log_tail(result.log_path), title="Log tail", border_style="red"))
                return result.exit_code

            results.append(result)
            completed_passes += job.passes
            console.print()
            console.print(build_results_table(results))
            console.print(build_totals_table(results))
            console.print()

    console.print("[bold green]All benchmarks finished.[/bold green]")
    console.print(f"Telemetry saved under: {display_path(config.telemetry_root)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

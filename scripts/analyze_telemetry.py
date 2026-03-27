#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from rich import box
from rich.console import Console
from rich.table import Table


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_TELEMETRY_ROOT = BASE_DIR / "benchmark_telemetry"


@dataclass(frozen=True)
class StepRow:
    tool: str
    phase: str
    mode: str
    workers: str
    pass_num: int
    elapsed_ns: int
    throughput_mib_s: float
    output_bytes: int
    input_bytes: int
    ratio: float
    host_cpu_percent: float | None
    host_peak_rss_bytes: int | None
    host_io_read_bytes: int | None
    host_io_write_bytes: int | None
    telemetry_path: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze benchmark telemetry folders and extract summary data."
    )
    parser.add_argument(
        "telemetry_root",
        nargs="?",
        default=str(DEFAULT_TELEMETRY_ROOT),
        help="Telemetry root directory or a single run directory.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Analyze every run directory under the root.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of a text report.",
    )
    return parser.parse_args()


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_optional_int(value: object) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)


def parse_optional_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    return float(value)


def format_float(value: int | float | None, digits: int = 3) -> str:
    if value is None:
        return ""
    text = f"{float(value):.{digits}f}"
    return text.rstrip("0").rstrip(".")


def format_bytes(value: int | float | None) -> str:
    if value is None:
        return ""

    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    unit = 0
    while abs(size) >= 1024 and unit < len(units) - 1:
        size /= 1024
        unit += 1

    if unit == 0:
        return f"{int(size):,} {units[unit]}"
    return f"{size:.1f} {units[unit]}"


def format_duration_us(value: int | float | None) -> str:
    if value is None:
        return ""

    micros = float(value)
    if micros >= 1_000_000:
        return f"{micros / 1_000_000:.2f} s"
    if micros >= 1_000:
        return f"{micros / 1_000:.2f} ms"
    return f"{micros:.0f} us"


def format_duration_s(value: int | float | None) -> str:
    if value is None:
        return ""

    seconds = float(value)
    if seconds >= 60:
        return f"{seconds / 60:.2f} min"
    return f"{format_float(seconds, digits=3)} s"


def format_percent(value: int | float | None) -> str:
    if value is None:
        return ""
    return f"{format_float(value)}%"


def format_count(value: int | float | None) -> str:
    if value is None:
        return ""
    return f"{int(value):,}"


def format_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(format_value(item) for item in value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return format_float(value)
    return str(value)


def make_table(
    title: str,
    columns: list[str],
    rows: list[list[object]],
    *,
    right_align: set[int] | None = None,
) -> Table:
    table = Table(title=title or None, box=box.SIMPLE_HEAVY, show_lines=False)
    right_align = right_align or set()
    for index, column in enumerate(columns):
        table.add_column(
            column,
            justify="right" if index in right_align else "left",
            overflow="fold",
            no_wrap=False,
        )
    for row in rows:
        table.add_row(
            *(format_table_cell(columns[index], cell) for index, cell in enumerate(row))
        )
    return table


def format_table_cell(column: str, value: object) -> str:
    column_key = column.strip().lower()

    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(format_table_cell(column, item) for item in value)

    if column_key in {"seconds"}:
        return format_duration_s(value)
    if column_key == "cpu%":
        return format_percent(value)
    if column_key in {"ratio", "mib/s", "effective cores"}:
        return format_float(value)
    if column_key in {"passes", "samples", "rows", "groups", "ctx sw"}:
        return format_count(value)
    if column_key in {
        "out bytes",
        "input bytes",
        "peak rss",
        "io read",
        "io write",
        "disk δ",
        "output bytes",
        "reorder_bytes_peak",
    }:
        return format_bytes(value)

    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return format_float(value)
    return str(value)


def _numeric_summary(
    values: list[int | float], digits: int = 3
) -> dict[str, int | float]:
    if not values:
        return {}

    if all(isinstance(value, int) for value in values):
        ints = [int(value) for value in values]
        return {"min": min(ints), "avg": round(statistics.mean(ints)), "max": max(ints)}

    floats = [float(value) for value in values]
    return {
        "min": round(min(floats), digits),
        "avg": round(statistics.mean(floats), digits),
        "max": round(max(floats), digits),
    }


def load_steps_csv(path: Path) -> list[StepRow]:
    rows: list[StepRow] = []
    with path.open(newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for raw in reader:
            try:
                rows.append(
                    StepRow(
                        tool=str(raw["tool"]),
                        phase=str(raw["phase"]),
                        mode=str(raw["mode"]),
                        workers=str(raw["workers"]),
                        pass_num=int(raw["pass_num"]),
                        elapsed_ns=int(raw["elapsed_ns"]),
                        throughput_mib_s=float(raw["throughput_mib_s"]),
                        output_bytes=int(raw["output_bytes"]),
                        input_bytes=int(raw["input_bytes"]),
                        ratio=float(raw["ratio"]),
                        host_cpu_percent=parse_optional_float(
                            raw.get("host_cpu_percent")
                        ),
                        host_peak_rss_bytes=parse_optional_int(
                            raw.get("host_peak_rss_bytes")
                        ),
                        host_io_read_bytes=parse_optional_int(
                            raw.get("host_io_read_bytes")
                        ),
                        host_io_write_bytes=parse_optional_int(
                            raw.get("host_io_write_bytes")
                        ),
                        telemetry_path=str(raw.get("telemetry_path") or "") or None,
                    )
                )
            except (KeyError, ValueError):
                continue
    return rows


def find_run_dirs(path: Path, all_runs: bool) -> list[Path]:
    if not path.exists():
        return []

    if (path / "run.json").exists():
        return [path]

    runs = [
        child
        for child in path.iterdir()
        if child.is_dir() and (child / "run.json").exists()
    ]
    runs.sort(key=lambda p: p.name)
    if all_runs:
        return runs
    return runs[-1:] if runs else []


def group_steps(
    rows: Iterable[StepRow],
) -> dict[tuple[str, str, str, str], list[StepRow]]:
    grouped: dict[tuple[str, str, str, str], list[StepRow]] = {}
    for row in rows:
        grouped.setdefault((row.tool, row.phase, row.mode, row.workers), []).append(row)
    return grouped


def summarize_steps(rows: list[StepRow]) -> dict[str, object]:
    grouped = group_steps(rows)
    group_summaries: list[dict[str, object]] = []
    for (tool, phase, mode, workers), items in sorted(grouped.items()):
        elapsed_s = [item.elapsed_ns / 1_000_000_000 for item in items]
        throughput = [item.throughput_mib_s for item in items]
        output_bytes = [item.output_bytes for item in items]
        cpu_percent = [
            item.host_cpu_percent for item in items if item.host_cpu_percent is not None
        ]
        rss = [
            item.host_peak_rss_bytes
            for item in items
            if item.host_peak_rss_bytes is not None
        ]
        read_bytes = [
            item.host_io_read_bytes
            for item in items
            if item.host_io_read_bytes is not None
        ]
        write_bytes = [
            item.host_io_write_bytes
            for item in items
            if item.host_io_write_bytes is not None
        ]

        group_summaries.append(
            {
                "tool": tool,
                "phase": phase,
                "mode": mode,
                "workers": workers,
                "passes": len(items),
                "avg_seconds": round(statistics.mean(elapsed_s), 6),
                "avg_throughput_mib_s": round(statistics.mean(throughput), 3),
                "avg_output_bytes": round(statistics.mean(output_bytes)),
                "avg_ratio": round(statistics.mean(item.ratio for item in items), 6),
                "max_peak_rss_bytes": max(rss) if rss else None,
                "avg_host_cpu_percent": round(statistics.mean(cpu_percent), 3)
                if cpu_percent
                else None,
                "avg_host_io_read_bytes": round(statistics.mean(read_bytes))
                if read_bytes
                else None,
                "avg_host_io_write_bytes": round(statistics.mean(write_bytes))
                if write_bytes
                else None,
            }
        )

    fastest = min(rows, key=lambda row: row.elapsed_ns, default=None)
    smallest = min(rows, key=lambda row: row.output_bytes, default=None)

    return {
        "steps": group_summaries,
        "totals": {
            "rows": len(rows),
            "groups": len(grouped),
            "fastest_step": None
            if fastest is None
            else {
                "tool": fastest.tool,
                "phase": fastest.phase,
                "mode": fastest.mode,
                "workers": fastest.workers,
                "seconds": round(fastest.elapsed_ns / 1_000_000_000, 6),
            },
            "smallest_output_step": None
            if smallest is None
            else {
                "tool": smallest.tool,
                "phase": smallest.phase,
                "mode": smallest.mode,
                "workers": smallest.workers,
                "output_bytes": smallest.output_bytes,
                "ratio": round(smallest.ratio, 6),
            },
        },
    }


def resolve_telemetry_path(row: StepRow, run_dir: Path) -> Path | None:
    if row.telemetry_path:
        telemetry_path = Path(row.telemetry_path)
        if telemetry_path.exists():
            return telemetry_path

    local_path = (
        run_dir
        / "steps"
        / f"p{row.pass_num:02d}_{row.mode}_{row.workers}_{row.tool}_{row.phase}"
        / "telemetry.json"
    )
    return local_path if local_path.exists() else None


def summarize_host_telemetry(
    rows: list[StepRow], run_dir: Path
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str, str], dict[str, list[int | float]]] = {}

    for row in rows:
        telemetry_path = resolve_telemetry_path(row, run_dir)
        if not telemetry_path:
            continue

        payload = read_json(telemetry_path)
        host = payload.get("host") or {}
        if not isinstance(host, dict):
            continue

        key = (row.tool, row.phase, row.mode, row.workers)
        bucket = grouped.setdefault(key, {})

        for field in (
            "cpu_user_ns",
            "cpu_system_ns",
            "cpu_percent",
            "peak_rss_bytes",
            "final_rss_bytes",
            "io_read_bytes",
            "io_write_bytes",
            "io_read_count",
            "io_write_count",
            "voluntary_ctx_switches",
            "involuntary_ctx_switches",
            "minor_faults",
            "major_faults",
        ):
            value = host.get(field)
            if isinstance(value, (int, float)):
                bucket.setdefault(field, []).append(value)

        disk_before = host.get("disk_usage_before")
        disk_after = host.get("disk_usage_after")
        if isinstance(disk_before, dict) and isinstance(disk_after, dict):
            used_before = disk_before.get("used")
            used_after = disk_after.get("used")
            if isinstance(used_before, int) and isinstance(used_after, int):
                bucket.setdefault("disk_used_delta_bytes", []).append(
                    used_after - used_before
                )

    summaries: list[dict[str, object]] = []
    for (tool, phase, mode, workers), values in sorted(grouped.items()):
        samples = max((len(series) for series in values.values()), default=0)
        summaries.append(
            {
                "tool": tool,
                "phase": phase,
                "mode": mode,
                "workers": workers,
                "samples": samples,
                "avg_cpu_percent": round(statistics.mean(values["cpu_percent"]), 3)
                if values.get("cpu_percent")
                else None,
                "avg_cpu_user_ns": round(statistics.mean(values["cpu_user_ns"]))
                if values.get("cpu_user_ns")
                else None,
                "avg_cpu_system_ns": round(statistics.mean(values["cpu_system_ns"]))
                if values.get("cpu_system_ns")
                else None,
                "max_peak_rss_bytes": max(values["peak_rss_bytes"])
                if values.get("peak_rss_bytes")
                else None,
                "avg_io_read_bytes": round(statistics.mean(values["io_read_bytes"]))
                if values.get("io_read_bytes")
                else None,
                "avg_io_write_bytes": round(statistics.mean(values["io_write_bytes"]))
                if values.get("io_write_bytes")
                else None,
                "avg_ctx_switches": round(
                    statistics.mean(
                        [
                            *(values.get("voluntary_ctx_switches") or []),
                            *(values.get("involuntary_ctx_switches") or []),
                        ]
                    )
                )
                if (
                    values.get("voluntary_ctx_switches")
                    or values.get("involuntary_ctx_switches")
                )
                else None,
                "avg_minor_faults": round(statistics.mean(values["minor_faults"]))
                if values.get("minor_faults")
                else None,
                "avg_major_faults": round(statistics.mean(values["major_faults"]))
                if values.get("major_faults")
                else None,
                "avg_disk_used_delta_bytes": round(
                    statistics.mean(values["disk_used_delta_bytes"])
                )
                if values.get("disk_used_delta_bytes")
                else None,
            }
        )

    return summaries


def summarize_oxide_telemetry(
    rows: list[StepRow], run_dir: Path
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str, str], dict[str, list[int | float]]] = {}

    for row in rows:
        if row.tool != "oxide":
            continue

        telemetry_path = resolve_telemetry_path(row, run_dir)
        if not telemetry_path:
            continue

        payload = read_json(telemetry_path)
        oxide = payload.get("oxide") or payload
        if not isinstance(oxide, dict):
            continue

        runtime = oxide.get("runtime") or {}
        stage_timings = oxide.get("stage_timings") or {}
        queue_peaks = oxide.get("queue_peaks") or {}
        if (
            not isinstance(runtime, dict)
            or not isinstance(stage_timings, dict)
            or not isinstance(queue_peaks, dict)
        ):
            continue

        key = (row.tool, row.phase, row.mode, row.workers)
        bucket = grouped.setdefault(key, {})

        effective_cores = runtime.get("effective_cores")
        if isinstance(effective_cores, (int, float)):
            bucket.setdefault("effective_cores", []).append(effective_cores)

        for stage_key, value in stage_timings.items():
            if isinstance(value, int):
                bucket.setdefault(f"stage:{stage_key}", []).append(value)

        for queue_key, value in queue_peaks.items():
            if isinstance(value, int):
                bucket.setdefault(f"queue:{queue_key}", []).append(value)

    summaries: list[dict[str, object]] = []
    for (tool, phase, mode, workers), values in sorted(grouped.items()):
        stage_summaries = {
            key.split(":", 1)[1]: _numeric_summary(series)
            for key, series in values.items()
            if key.startswith("stage:")
        }
        queue_summaries = {
            key.split(":", 1)[1]: _numeric_summary(series)
            for key, series in values.items()
            if key.startswith("queue:")
        }

        summary: dict[str, object] = {
            "tool": tool,
            "phase": phase,
            "mode": mode,
            "workers": workers,
            "samples": len(next(iter(values.values()))) if values else 0,
        }
        if "effective_cores" in values:
            summary["effective_cores"] = _numeric_summary(values["effective_cores"])
        if stage_summaries:
            summary["stage_timings_us"] = stage_summaries
        if queue_summaries:
            summary["queue_peaks"] = queue_summaries
        summaries.append(summary)

    return summaries


def format_oxide_metric(name: str, value: int | float | None, *, kind: str) -> str:
    if kind == "stage":
        return format_duration_us(value)

    if kind == "queue":
        if value is None:
            return ""
        if "bytes" in name or name in {"configured_inflight", "max_inflight_bytes"}:
            return format_bytes(value)
        return format_value(value)

    return format_value(value)


def flatten_oxide_stage_rows(
    oxide_telemetry: list[dict[str, object]],
) -> list[list[object]]:
    rows: list[list[object]] = []
    for group in oxide_telemetry:
        stage_timings = group.get("stage_timings_us")
        if not isinstance(stage_timings, dict):
            continue
        for metric, stats in sorted(stage_timings.items()):
            if not isinstance(stats, dict):
                continue
            rows.append(
                [
                    group.get("tool"),
                    group.get("phase"),
                    group.get("mode"),
                    group.get("workers"),
                    metric,
                    format_oxide_metric(metric, stats.get("min"), kind="stage"),
                    format_oxide_metric(metric, stats.get("avg"), kind="stage"),
                    format_oxide_metric(metric, stats.get("max"), kind="stage"),
                ]
            )
    return rows


def flatten_oxide_queue_rows(
    oxide_telemetry: list[dict[str, object]],
) -> list[list[object]]:
    rows: list[list[object]] = []
    for group in oxide_telemetry:
        queue_peaks = group.get("queue_peaks")
        if not isinstance(queue_peaks, dict):
            continue
        for metric, stats in sorted(queue_peaks.items()):
            if not isinstance(stats, dict):
                continue
            rows.append(
                [
                    group.get("tool"),
                    group.get("phase"),
                    group.get("mode"),
                    group.get("workers"),
                    metric,
                    format_oxide_metric(metric, stats.get("min"), kind="queue"),
                    format_oxide_metric(metric, stats.get("avg"), kind="queue"),
                    format_oxide_metric(metric, stats.get("max"), kind="queue"),
                ]
            )
    return rows


def summarize_run(run_dir: Path) -> dict[str, object]:
    run_json = read_json(run_dir / "run.json")
    summary_json = (
        read_json(run_dir / "summary.json")
        if (run_dir / "summary.json").exists()
        else None
    )
    steps = load_steps_csv(run_dir / "steps.csv")

    return {
        "run_dir": str(run_dir),
        "run": run_json,
        "summary": summary_json,
        "analysis": summarize_steps(steps),
        "host_telemetry": summarize_host_telemetry(steps, run_dir),
        "oxide_telemetry": summarize_oxide_telemetry(steps, run_dir),
    }


def print_text_report(console: Console, payload: dict[str, object]) -> None:
    run = payload.get("run") or {}
    analysis = payload.get("analysis") or {}
    totals = analysis.get("totals") or {}
    steps = analysis.get("steps") or []
    host_telemetry = payload.get("host_telemetry") or []
    oxide_telemetry = payload.get("oxide_telemetry") or []

    console.print(
        make_table(
            "run summary",
            ["field", "value"],
            [
                ["run_dir", payload.get("run_dir")],
                ["source", run.get("source")],
                ["workers", run.get("worker_modes")],
                ["completed_at", (payload.get("summary") or {}).get("completed_at")],
                ["rows", totals.get("rows")],
                ["groups", totals.get("groups")],
                [
                    "fastest",
                    None
                    if not totals.get("fastest_step")
                    else (
                        f"{totals['fastest_step']['tool']} {totals['fastest_step']['phase']} "
                        f"mode={totals['fastest_step']['mode']} workers={totals['fastest_step']['workers']} "
                        f"{format_duration_s(totals['fastest_step']['seconds'])}"
                    ),
                ],
                [
                    "smallest",
                    None
                    if not totals.get("smallest_output_step")
                    else (
                        f"{totals['smallest_output_step']['tool']} {totals['smallest_output_step']['phase']} "
                        f"mode={totals['smallest_output_step']['mode']} workers={totals['smallest_output_step']['workers']} "
                        f"{format_bytes(totals['smallest_output_step']['output_bytes'])}"
                    ),
                ],
            ],
        )
    )

    if steps:
        console.print()
        console.print(
            make_table(
                "step averages",
                [
                    "tool",
                    "phase",
                    "mode",
                    "workers",
                    "passes",
                    "seconds",
                    "MiB/s",
                    "out bytes",
                    "ratio",
                    "cpu%",
                    "peak rss",
                    "io read",
                    "io write",
                ],
                [
                    [
                        row["tool"],
                        row["phase"],
                        row["mode"],
                        row["workers"],
                        row["passes"],
                        row["avg_seconds"],
                        row["avg_throughput_mib_s"],
                        row["avg_output_bytes"],
                        row["avg_ratio"],
                        row["avg_host_cpu_percent"],
                        row["max_peak_rss_bytes"],
                        row["avg_host_io_read_bytes"],
                        row["avg_host_io_write_bytes"],
                    ]
                    for row in steps
                ],
                right_align={4, 5, 6, 7, 8, 9, 10, 11, 12},
            )
        )

    if host_telemetry:
        console.print()
        console.print(
            make_table(
                "host telemetry",
                [
                    "tool",
                    "phase",
                    "mode",
                    "workers",
                    "samples",
                    "cpu%",
                    "peak rss",
                    "io read",
                    "io write",
                    "ctx sw",
                    "minor faults",
                    "major faults",
                    "disk Δ",
                ],
                [
                    [
                        row["tool"],
                        row["phase"],
                        row["mode"],
                        row["workers"],
                        row["samples"],
                        row["avg_cpu_percent"],
                        row["max_peak_rss_bytes"],
                        row["avg_io_read_bytes"],
                        row["avg_io_write_bytes"],
                        row["avg_ctx_switches"],
                        row["avg_minor_faults"],
                        row["avg_major_faults"],
                        row["avg_disk_used_delta_bytes"],
                    ]
                    for row in host_telemetry
                ],
                right_align={4, 5, 6, 7, 8, 9, 10, 11, 12},
            )
        )

    if oxide_telemetry:
        console.print()
        console.print(
            make_table(
                "oxide telemetry",
                [
                    "tool",
                    "phase",
                    "mode",
                    "workers",
                    "samples",
                    "effective cores",
                ],
                [
                    [
                        row["tool"],
                        row["phase"],
                        row["mode"],
                        row["workers"],
                        row["samples"],
                        row.get("effective_cores", {}).get("avg")
                        if isinstance(row.get("effective_cores"), dict)
                        else row.get("effective_cores"),
                    ]
                    for row in oxide_telemetry
                ],
                right_align={4, 5},
            )
        )

        stage_rows = flatten_oxide_stage_rows(oxide_telemetry)
        if stage_rows:
            console.print()
            console.print(
                make_table(
                    "oxide stage timings",
                    ["tool", "phase", "mode", "workers", "metric", "min", "avg", "max"],
                    stage_rows,
                    right_align={5, 6, 7},
                )
            )

        queue_rows = flatten_oxide_queue_rows(oxide_telemetry)
        if queue_rows:
            console.print()
            console.print(
                make_table(
                    "oxide queue peaks",
                    ["tool", "phase", "mode", "workers", "metric", "min", "avg", "max"],
                    queue_rows,
                    right_align={5, 6, 7},
                )
            )


def main() -> int:
    args = parse_args()
    root = Path(args.telemetry_root)
    run_dirs = find_run_dirs(root, args.all)
    console = Console(width=220)

    if not run_dirs:
        raise SystemExit(f"No telemetry runs found under: {root}")

    payload = [summarize_run(run_dir) for run_dir in run_dirs]
    output: object = payload[0] if len(payload) == 1 else payload

    if args.json:
        print(json.dumps(output, indent=2, sort_keys=True))
    else:
        if len(payload) == 1:
            print_text_report(console, payload[0])
        else:
            for item in payload:
                print_text_report(console, item)
                console.print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

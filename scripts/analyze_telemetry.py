#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


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
    }


def print_text_report(payload: dict[str, object]) -> None:
    run = payload.get("run") or {}
    analysis = payload.get("analysis") or {}
    totals = analysis.get("totals") or {}
    steps = analysis.get("steps") or []

    print(f"run: {payload.get('run_dir')}")
    print(f"source: {run.get('source')}")
    print(f"workers: {run.get('worker_modes')}")
    if payload.get("summary"):
        print(f"summary: {payload['summary'].get('completed_at')}")
    print(f"rows: {totals.get('rows')}  groups: {totals.get('groups')}")

    if totals.get("fastest_step"):
        fastest = totals["fastest_step"]
        print(
            "fastest: "
            f"{fastest['tool']} {fastest['phase']} mode={fastest['mode']} workers={fastest['workers']} "
            f"{fastest['seconds']}s"
        )
    if totals.get("smallest_output_step"):
        smallest = totals["smallest_output_step"]
        print(
            "smallest: "
            f"{smallest['tool']} {smallest['phase']} mode={smallest['mode']} workers={smallest['workers']} "
            f"{smallest['output_bytes']} bytes"
        )

    print("\nper-group averages:")
    for row in steps:
        print(
            f"- {row['mode']} {row['workers']} {row['tool']} {row['phase']}: "
            f"{row['avg_seconds']}s, {row['avg_throughput_mib_s']} MiB/s, ratio={row['avg_ratio']}"
        )


def main() -> int:
    args = parse_args()
    root = Path(args.telemetry_root)
    run_dirs = find_run_dirs(root, args.all)

    if not run_dirs:
        raise SystemExit(f"No telemetry runs found under: {root}")

    payload = [summarize_run(run_dir) for run_dir in run_dirs]
    output: object = payload[0] if len(payload) == 1 else payload

    if args.json:
        print(json.dumps(output, indent=2, sort_keys=True))
    else:
        if len(payload) == 1:
            print_text_report(payload[0])
        else:
            for item in payload:
                print_text_report(item)
                print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

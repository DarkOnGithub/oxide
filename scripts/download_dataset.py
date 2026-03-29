#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
import ssl
import subprocess
import sys
import tarfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path


SSL_CONTEXT = ssl.create_default_context()


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    kind: str  # "archive", "file", "git", or "hg"
    source: str
    archive_type: str | None = None
    clone_depth: int | None = None


DATASETS: dict[str, DatasetSpec] = {
    "silesia": DatasetSpec(
        name="silesia",
        kind="archive",
        source="https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip",
        archive_type="zip",
    ),
    "enwik9": DatasetSpec(
        name="enwik9",
        kind="archive",
        source="http://mattmahoney.net/dc/enwik9.zip",
        archive_type="zip",
    ),
    "linux": DatasetSpec(
        name="linux",
        kind="archive",
        source="https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.13.tar.xz",
        archive_type="tar",
    ),
    "nyctaxi": DatasetSpec(
        name="nyctaxi",
        kind="file",
        # Note: Using 2015 data to guarantee a pure ~2GB CSV, as newer TLC data defaults to Parquet
        source="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-01.csv",
    ),
    "logs": DatasetSpec(
        name="logs",
        kind="archive",
        source="https://zenodo.org/records/3227177/files/HDFS_1.tar.gz",
        archive_type="tar",
    ),
    "div2k": DatasetSpec(
        name="div2k",
        kind="archive",
        source="http://data.vision.ee.ethz.ch/cvl/DIV2K/DIV2K_valid_HR.zip",
        archive_type="zip",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download datasets for the archiver benchmark suite."
    )
    parser.add_argument(
        "dataset",
        nargs="?",
        default="all",
        choices=[*DATASETS.keys(), "all"],
        help="Specific dataset to download (default: all).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("datasets"),
        help="Where datasets are stored.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Remove any existing target directory before downloading.",
    )
    return parser.parse_args()


def ensure_empty(path: Path, force: bool) -> None:
    if path.exists():
        if path.is_dir():
            if any(path.iterdir()):
                if force:
                    shutil.rmtree(path)
                else:
                    raise FileExistsError(
                        f"Target already exists: {path}. Use --force to replace it."
                    )
        elif force:
            path.unlink()
        else:
            raise FileExistsError(
                f"Target already exists: {path}. Use --force to replace it."
            )


def download_file(url: str, dest: Path) -> None:
    print(f"Downloading {url}")
    tmp_path = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=600, context=SSL_CONTEXT) as response:
        with tmp_path.open("wb") as out_file:
            shutil.copyfileobj(response, out_file, length=1024 * 1024)
    tmp_path.replace(dest)


def extract_archive(archive_path: Path, target_dir: Path, archive_type: str) -> None:
    print(f"Extracting {archive_path.name}")
    if archive_type == "zip":
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(target_dir)
    elif archive_type == "tar":
        with tarfile.open(archive_path, "r:*") as tf:
            tf.extractall(target_dir)
    else:
        raise ValueError(f"Unsupported archive type: {archive_type}")


def clone_repo(
    kind: str, source: str, target_dir: Path, depth: int | None = None
) -> None:
    command = [kind, "clone"]
    if kind == "git" and depth is not None:
        command += ["--depth", str(depth)]
    command += [source, str(target_dir)]
    print("Running:", " ".join(command))
    subprocess.run(command, check=True)


def download_dataset(spec: DatasetSpec, output_dir: Path, force: bool) -> None:
    target_dir = output_dir / spec.name
    ensure_empty(target_dir, force)

    if spec.kind == "archive":
        target_dir.mkdir(parents=True, exist_ok=True)
        archive_name = Path(spec.source.split("?")[0]).name
        archive_path = target_dir / archive_name
        download_file(spec.source, archive_path)
        extract_archive(archive_path, target_dir, spec.archive_type or "zip")
        archive_path.unlink(missing_ok=True)
    elif spec.kind == "file":
        target_dir.mkdir(parents=True, exist_ok=True)
        file_name = Path(spec.source.split("?")[0]).name
        file_path = target_dir / file_name
        download_file(spec.source, file_path)
    elif spec.kind in {"git", "hg"}:
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        clone_repo(spec.kind, spec.source, target_dir, spec.clone_depth)
    else:
        raise ValueError(f"Unsupported dataset kind: {spec.kind}")

    print(f"Done: {spec.name} -> {target_dir}\n")


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    targets = list(DATASETS.values()) if args.dataset == "all" else [DATASETS[args.dataset]]
    for spec in targets:
        download_dataset(spec, args.output_dir, args.force)

    print("All requested datasets have been successfully prepared.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
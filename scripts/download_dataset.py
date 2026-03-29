#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
import ssl
import subprocess
import tarfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

SSL_CONTEXT = ssl.create_default_context()

@dataclass(frozen=True)
class DatasetSpec:
    name: str
    kind: str  # "archive", "file", "git"
    source: str
    archive_type: str | None = None
    clone_depth: int | None = None

# Individual data components
COMPONENTS = {
    "silesia": DatasetSpec("silesia", "archive", "https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip", "zip"),
    "enwik9": DatasetSpec("enwik9", "archive", "http://mattmahoney.net/dc/enwik9.zip", "zip"),
    "linux": DatasetSpec("linux", "archive", "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.13.tar.xz", "tar"),
    "nyctaxi": DatasetSpec("nyctaxi", "file", "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-01.csv"),
    "logs": DatasetSpec("logs", "archive", "https://zenodo.org/records/3227177/files/HDFS_1.tar.gz", "tar"),
    "div2k": DatasetSpec("div2k", "archive", "http://data.vision.ee.ethz.ch/cvl/DIV2K/DIV2K_valid_HR.zip", "zip"),
    "chromium": DatasetSpec("chromium", "git", "https://chromium.googlesource.com/chromium/src", clone_depth=1),
}

# Tiers mapping to components
TIERS = {
    "200mb": ["silesia"],
    "1gb": ["enwik9"],
    "5gb": ["silesia", "enwik9", "linux", "nyctaxi", "logs"], # Total ~4.9GB
    "6gb": ["silesia", "enwik9", "linux", "nyctaxi", "logs", "div2k"], # Total ~6GB with Images
    "10gb": ["chromium"], # Huge source tree
}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download archiver benchmark datasets by tier.")
    parser.add_argument(
        "tier",
        nargs="?",
        default="6gb",
        choices=[*TIERS.keys(), "all"],
        help="Dataset tier to download (default: 6gb).",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("datasets"), help="Storage directory.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing data.")
    return parser.parse_args()

def ensure_empty(path: Path, force: bool) -> None:
    if path.exists():
        if force:
            shutil.rmtree(path) if path.is_dir() else path.unlink()
        else:
            raise FileExistsError(f"Target exists: {path}. Use --force to replace.")

def download_file(url: str, dest: Path) -> None:
    print(f"Downloading {url}")
    tmp_path = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=600, context=SSL_CONTEXT) as resp:
        with tmp_path.open("wb") as f:
            shutil.copyfileobj(resp, f, length=1024 * 1024)
    tmp_path.replace(dest)

def extract_archive(archive_path: Path, target_dir: Path, archive_type: str) -> None:
    print(f"Extracting {archive_path.name}...")
    if archive_type == "zip":
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(target_dir)
    elif archive_type == "tar":
        with tarfile.open(archive_path, "r:*") as tf:
            tf.extractall(target_dir)

def download_component(spec: DatasetSpec, output_dir: Path, force: bool) -> None:
    target_dir = output_dir / spec.name
    if target_dir.exists() and not force:
        print(f"Skipping {spec.name} (already exists).")
        return
    ensure_empty(target_dir, force)

    if spec.kind == "archive":
        target_dir.mkdir(parents=True, exist_ok=True)
        archive_path = target_dir / Path(spec.source.split("?")[0]).name
        download_file(spec.source, archive_path)
        extract_archive(archive_path, target_dir, spec.archive_type or "zip")
        archive_path.unlink()
    elif spec.kind == "file":
        target_dir.mkdir(parents=True, exist_ok=True)
        download_file(spec.source, target_dir / Path(spec.source).name)
    elif spec.kind == "git":
        cmd = ["git", "clone", "--depth", str(spec.clone_depth or 1), spec.source, str(target_dir)]
        subprocess.run(cmd, check=True)

def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    selected_components = []
    if args.tier == "all":
        selected_components = list(COMPONENTS.keys())
    else:
        selected_components = TIERS[args.tier]

    for comp_key in selected_components:
        download_component(COMPONENTS[comp_key], args.output_dir, args.force)
    
    print(f"\nFinished preparing the {args.tier} suite.")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
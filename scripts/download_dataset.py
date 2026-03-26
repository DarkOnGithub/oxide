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
    tier: str
    name: str
    kind: str  # "archive", "git", or "hg"
    source: str
    archive_type: str | None = None
    clone_depth: int | None = None


DATASETS: dict[str, DatasetSpec] = {
    "200mb": DatasetSpec(
        tier="200mb",
        name="silesia",
        kind="archive",
        source="https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip",
        archive_type="zip",
    ),
    "1gb": DatasetSpec(
        tier="1gb",
        name="linux",
        kind="git",
        source="https://github.com/torvalds/linux.git",
        clone_depth=1,
    ),
    "5gb": DatasetSpec(
        tier="5gb",
        name="mozilla-central",
        kind="hg",
        source="https://hg.mozilla.org/mozilla-central/",
    ),
    "10gb": DatasetSpec(
        tier="10gb",
        name="chromium-src",
        kind="git",
        source="https://chromium.googlesource.com/chromium/src",
        clone_depth=1,
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a dataset by target size tier."
    )
    parser.add_argument(
        "tier",
        nargs="?",
        default="200mb",
        choices=[*DATASETS.keys(), "all"],
        help="Dataset tier to download (default: 200mb).",
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
        archive_name = Path(spec.source).name
        archive_path = target_dir / archive_name
        download_file(spec.source, archive_path)
        extract_archive(archive_path, target_dir, spec.archive_type or "zip")
        archive_path.unlink(missing_ok=True)
    elif spec.kind in {"git", "hg"}:
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        clone_repo(spec.kind, spec.source, target_dir, spec.clone_depth)
    else:
        raise ValueError(f"Unsupported dataset kind: {spec.kind}")

    print(f"Done: {spec.tier} -> {target_dir}")


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    tiers = list(DATASETS.values()) if args.tier == "all" else [DATASETS[args.tier]]
    for spec in tiers:
        download_dataset(spec, args.output_dir, args.force)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

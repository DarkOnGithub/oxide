#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
import ssl
import subprocess
import tarfile
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path


SSL_CONTEXT = ssl.create_default_context()
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    kind: str  # "archive", "file", "git"
    source: str
    archive_type: str | None = None
    clone_depth: int | None = None
    optional: bool = False
    fallback_git: str | None = None


COMPONENTS = {
    "silesia": DatasetSpec(
        "silesia", "archive",
        "https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip", "zip"
    ),
    "enwik9": DatasetSpec(
        "enwik9", "archive",
        "https://mattmahoney.net/dc/enwik9.zip", "zip"
    ),
    "linux": DatasetSpec(
        "linux", "archive",
        "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.13.tar.xz", "tar"
    ),
    "nyctaxi": DatasetSpec(
        "nyctaxi", "file",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-01.parquet"
    ),
    "logs": DatasetSpec(
        "logs", "archive",
        "https://zenodo.org/api/records/3227177/files/HDFS_1.tar.gz/content",
        "tar",
        optional=True,
        fallback_git="https://github.com/logpai/loghub.git",
    ),
    "div2k": DatasetSpec(
        "div2k", "archive",
        "https://data.vision.ee.ethz.ch/cvl/DIV2K/DIV2K_valid_HR.zip", "zip"
    ),
    "chromium": DatasetSpec(
        "chromium", "git",
        "https://chromium.googlesource.com/chromium/src", clone_depth=1
    ),
}


TIERS = {
    "200mb": ["silesia"],
    "1gb": ["enwik9"],
    "5gb": ["silesia", "enwik9", "linux", "nyctaxi", "logs"],
    "6gb": ["silesia", "enwik9", "linux", "nyctaxi", "logs", "div2k"],
    "10gb": ["chromium"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download archiver benchmark datasets by tier.")
    parser.add_argument("tier", nargs="?", default="6gb", choices=[*TIERS.keys(), "all"])
    parser.add_argument("--output-dir", type=Path, default=Path("datasets"))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--keep-archives", action="store_true")
    return parser.parse_args()


def ensure_empty(path: Path, force: bool) -> None:
    if path.exists():
        if force:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        else:
            raise FileExistsError(f"Target exists: {path}. Use --force to replace.")


def filename_from_url(url: str, response=None) -> str:
    if response is not None:
        cd = response.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            raw = cd.split("filename=", 1)[1].strip().strip('"')
            if raw:
                return Path(raw).name
    parsed = urllib.parse.urlparse(url)
    return Path(parsed.path).name or "download.bin"


def download_file(url: str, dest: Path | None = None) -> Path:
    print(f"Downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=600, context=SSL_CONTEXT) as resp:
        if dest is None:
            dest = Path(filename_from_url(resp.geturl(), resp))
        tmp_path = dest.with_suffix(dest.suffix + ".part")
        with tmp_path.open("wb") as f:
            shutil.copyfileobj(resp, f, length=1024 * 1024)
    tmp_path.replace(dest)
    return dest


def is_within_directory(directory: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(directory.resolve())
        return True
    except ValueError:
        return False


def safe_extract_zip(archive_path: Path, target_dir: Path) -> None:
    with zipfile.ZipFile(archive_path, "r") as zf:
        for member in zf.infolist():
            member_path = target_dir / member.filename
            if not is_within_directory(target_dir, member_path):
                raise RuntimeError(f"Unsafe zip path detected: {member.filename}")
        zf.extractall(target_dir)


def safe_extract_tar(archive_path: Path, target_dir: Path) -> None:
    with tarfile.open(archive_path, "r:*") as tf:
        for member in tf.getmembers():
            member_path = target_dir / member.name
            if not is_within_directory(target_dir, member_path):
                raise RuntimeError(f"Unsafe tar path detected: {member.name}")
        tf.extractall(target_dir)


def extract_archive(archive_path: Path, target_dir: Path, archive_type: str) -> None:
    print(f"Extracting {archive_path.name}...")
    if archive_type == "zip":
        safe_extract_zip(archive_path, target_dir)
    elif archive_type == "tar":
        safe_extract_tar(archive_path, target_dir)
    else:
        raise ValueError(f"Unsupported archive type: {archive_type}")


def clone_git(url: str, target_dir: Path, depth: int = 1) -> None:
    subprocess.run(
        ["git", "clone", "--depth", str(depth), url, str(target_dir)],
        check=True,
    )


def prepare_logs_fallback(spec: DatasetSpec, target_dir: Path, force: bool) -> None:
    if not spec.fallback_git:
        raise RuntimeError("No fallback configured for logs")

    print("Zenodo blocked with HTTP 403. Falling back to Loghub GitHub source tree.")
    clone_git(spec.fallback_git, target_dir, depth=1)

    hdfs_dir = target_dir / "HDFS"
    if not hdfs_dir.exists():
        raise RuntimeError("Fallback repo cloned, but HDFS subdirectory not found.")


def download_component(spec: DatasetSpec, output_dir: Path, force: bool, keep_archives: bool) -> None:
    target_dir = output_dir / spec.name
    if target_dir.exists() and not force:
        print(f"Skipping {spec.name} (already exists).")
        return

    ensure_empty(target_dir, force)

    try:
        if spec.kind == "archive":
            target_dir.mkdir(parents=True, exist_ok=True)
            archive_path = download_file(spec.source, None)
            archive_dest = target_dir / archive_path.name
            shutil.move(str(archive_path), archive_dest)
            extract_archive(archive_dest, target_dir, spec.archive_type or "zip")
            if not keep_archives:
                archive_dest.unlink()

        elif spec.kind == "file":
            target_dir.mkdir(parents=True, exist_ok=True)
            dest_name = Path(urllib.parse.urlparse(spec.source).path).name
            download_file(spec.source, target_dir / dest_name)

        elif spec.kind == "git":
            clone_git(spec.source, target_dir, spec.clone_depth or 1)

        else:
            raise ValueError(f"Unsupported component kind: {spec.kind}")

    except urllib.error.HTTPError as e:
        if spec.name == "logs" and e.code == 403:
            ensure_empty(target_dir, True)
            target_dir.mkdir(parents=True, exist_ok=True)
            prepare_logs_fallback(spec, target_dir, force)
            return
        if spec.optional:
            print(f"Warning: failed to fetch optional dataset '{spec.name}': HTTP {e.code}")
            return
        raise


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    selected_components = list(COMPONENTS.keys()) if args.tier == "all" else TIERS[args.tier]

    for comp_key in selected_components:
        download_component(COMPONENTS[comp_key], args.output_dir, args.force, args.keep_archives)

    print(f"\\nFinished preparing the {args.tier} suite.")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
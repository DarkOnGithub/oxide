import os
import urllib.request
import concurrent.futures
import tarfile
import zipfile
import shutil
import ssl
import subprocess
from pathlib import Path

# Required for converting PNG/JPG to uncompressed BMP
try:
    from PIL import Image
except ImportError:
    print("❌ Pillow is required for BMP conversion. Please run: pip install Pillow")
    exit(1)

BASE_DIR = Path("datasets")
ssl_context = ssl._create_unverified_context()

DATASETS = {
    "IMAGES_BMP": [
        # 100 2K images. PNG -> BMP expands to ~600 MB uncompressed
        "http://data.vision.ee.ethz.ch/cvl/DIV2K/DIV2K_valid_HR.zip",
    ],
    "AUDIO_WAV": [
        # 2000 uncompressed WAV files ~ 800 MB
        "https://github.com/karolpiczak/ESC-50/archive/refs/heads/master.zip"
    ],
    "TEXT": [
        # ~1.5 GB total uncompressed text
        "https://s3.amazonaws.com/fast-ai-nlp/wikitext-103.tgz",
        "http://mattmahoney.net/dc/enwik9.zip" 
    ],
    "STRUCTURED": [
        # 3 years of NYC Taxi data ~ 1.5 GB
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{m:02d}.parquet"
        for year in [2021, 2022, 2023] for m in range(1, 13)
    ],
    "BINARY": [
        # ~170 MB uncompressed binary
        "https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz",
    ]
}

def convert_to_bmp(directory: Path):
    """Converts downloaded images to uncompressed BMPs and deletes the originals."""
    for img_path in directory.rglob("*"):
        if img_path.is_file() and img_path.suffix.lower() in ['.png', '.jpg', '.jpeg']:
            bmp_path = img_path.with_suffix('.bmp')
            try:
                with Image.open(img_path) as img:
                    img.save(bmp_path, 'BMP')
                img_path.unlink() # Remove the compressed original
            except Exception as e:
                print(f"⚠️ Failed to convert {img_path.name}: {e}")

def process_file(url: str, category_dir: Path):
    # 1. NAME MAPPING
    if "DIV2K" in url: filename = "DIV2K_HighRes.zip"
    elif "ESC-50" in url: filename = "ESC50_Audio.zip"
    else: filename = url.split('/')[-1].split('?')[0]

    filepath = category_dir / filename
    
    # 2. STRICT SKIP CHECK
    indicator_map = {
        "DIV2K_HighRes.zip": "DIV2K_valid_HR",
        "ESC50_Audio.zip": "ESC-50-master",
        "wikitext-103.tgz": "wikitext-103",
        "cifar-10-binary.tar.gz": "cifar-10-batches-bin",
        "enwik9.zip": "enwik9",
    }
    
    indicator = indicator_map.get(filename)
    if indicator and (category_dir / indicator).exists():
        return f"⏭️  SKIPPED: {filename} (Already extracted)"
    
    if filepath.suffix == '.parquet' and filepath.exists():
        return f"⏭️  SKIPPED: {filename}"

    # 3. DOWNLOAD
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        req = urllib.request.Request(url, headers=headers)
        
        print(f"📥 Downloading: {filename}...")
        with urllib.request.urlopen(req, timeout=600, context=ssl_context) as response:
            tmp_path = filepath.with_suffix(filepath.suffix + ".tmp")
            with tmp_path.open('wb') as out_file:
                shutil.copyfileobj(response, out_file, length=1024*1024)
            tmp_path.rename(filepath)
    except Exception as e:
        return f"❌ DOWNLOAD FAILED {filename}: {e}"

    # 4. EXTRACTION & POST-PROCESSING
    try:
        if filepath.suffix in ['.zip', '.gz', '.tgz']:
            print(f"📦 Extracting: {filename}...")
            if filepath.suffix == '.zip':
                with zipfile.ZipFile(filepath, 'r') as zf:
                    zf.extractall(category_dir)
            else:
                with tarfile.open(filepath, 'r:gz') as tf:
                    tf.extractall(category_dir)
            
            # Cleanup archive to save space
            filepath.unlink()

            # 5. POST-PROCESSING (Convert images to BMP)
            if category_dir.name == "IMAGES_BMP":
                print(f"🎨 Converting {filename} to raw BMP...")
                convert_to_bmp(category_dir)

        return f"✅ DONE: {filename}"
    except Exception as e:
        if filepath.exists(): filepath.unlink()
        return f"⚠️ EXTRACTION FAILED {filename}: {e}"

def main():
    BASE_DIR.mkdir(exist_ok=True)
    tasks = []
    
    for cat, urls in DATASETS.items():
        cat_dir = BASE_DIR / cat
        cat_dir.mkdir(exist_ok=True)
        for url in urls:
            tasks.append((url, cat_dir))

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_file, url, d) for url, d in tasks]
        for f in concurrent.futures.as_completed(futures):
            print(f.result())
    
    print("\n📊 Final Disk Usage:")
    subprocess.run(["du", "-h", str(BASE_DIR), "--max-depth=1"])

if __name__ == "__main__":
    main()

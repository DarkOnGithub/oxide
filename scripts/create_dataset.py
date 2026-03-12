import os
import urllib.request
import concurrent.futures
import tarfile
import zipfile
import shutil
import ssl
import subprocess

BASE_DIR = "datasets"
ssl_context = ssl._create_unverified_context()

DATASETS = {
    "IMAGES_BMP": [
        "http://data.vision.ee.ethz.ch/cvl/DIV2K/DIV2K_valid_HR.zip",
    ],
    "AUDIO_WAV": [
        "https://github.com/karolpiczak/ESC-50/archive/refs/heads/master.zip"
    ],
    "TEXT": [
        "https://s3.amazonaws.com/fast-ai-nlp/wikitext-103.tgz",
        "http://mattmahoney.net/dc/enwik9.zip" # Adds exactly 1GB uncompressed
    ],
    "STRUCTURED": [
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{m:02d}.parquet"
        for year in [2022, 2023] for m in range(1, 13)
    ],
    "BINARY": [
        "https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz",
        "https://biometrics.nist.gov/cs_links/EMNIST/gzip.zip"
    ]
}

def process_file(url, category_dir):
    # 1. NAME MAPPING
    if "DIV2K" in url: filename = "DIV2K_HighRes.zip"
    elif "ESC-50" in url: filename = "ESC50_Audio.zip"
    else: filename = url.split('/')[-1].split('?')[0]

    filepath = os.path.join(category_dir, filename)
    
    # 2. STRICT SKIP CHECK (To preserve your existing 4GB)
    indicator_map = {
        "DIV2K_HighRes.zip": "DIV2K_valid_HR",
        "ESC50_Audio.zip": "ESC-50-master",
        "wikitext-103.tgz": "wikitext-103",
        "cifar-10-binary.tar.gz": "cifar-10-batches-bin",
        "enwik9.zip": "enwik9", # The uncompressed file name
    }
    
    # Check if folder or uncompressed file already exists
    indicator = indicator_map.get(filename)
    if indicator and os.path.exists(os.path.join(category_dir, indicator)):
        return f"⏭️  SKIPPED: {filename} (Already exists)"
    
    # Check for raw parquet files
    if filename.endswith('.parquet') and os.path.exists(filepath):
        return f"⏭️  SKIPPED: {filename}"

    # 3. DOWNLOAD
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        req = urllib.request.Request(url, headers=headers)
        
        print(f"📥 Downloading: {filename}...")
        with urllib.request.urlopen(req, timeout=600, context=ssl_context) as response:
            tmp_path = filepath + ".tmp"
            with open(tmp_path, 'wb') as out_file:
                shutil.copyfileobj(response, out_file, length=1024*1024)
            os.rename(tmp_path, filepath)
    except Exception as e:
        return f"❌ DOWNLOAD FAILED {filename}: {e}"

    # 4. EXTRACTION
    try:
        print(f"📦 Extracting: {filename}...")
        if filepath.endswith('.zip'):
            with zipfile.ZipFile(filepath, 'r') as zf:
                zf.extractall(category_dir)
            os.remove(filepath)
        elif filepath.endswith(('.tar.gz', '.tgz')):
            with tarfile.open(filepath, 'r:gz') as tf:
                tf.extractall(category_dir)
            os.remove(filepath)
        return f"✅ DONE: {filename}"
    except Exception as e:
        if os.path.exists(filepath): os.remove(filepath)
        return f"⚠️ EXTRACTION FAILED {filename}: {e}"

def main():
    os.makedirs(BASE_DIR, exist_ok=True)
    tasks = []
    for cat, urls in DATASETS.items():
        cat_dir = os.path.join(BASE_DIR, cat)
        os.makedirs(cat_dir, exist_ok=True)
        for url in urls:
            tasks.append((url, cat_dir))

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_file, url, d) for url, d in tasks]
        for f in concurrent.futures.as_completed(futures):
            print(f.result())
    
    print("\n📊 Final Disk Usage:")
    subprocess.run(["du", "-h", BASE_DIR, "--max-depth=1"])

if __name__ == "__main__":
    main()
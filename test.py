import os
import urllib.request
import urllib.error
import concurrent.futures
import tarfile
import zipfile
import gzip
import shutil

# Define the base directory for downloads
BASE_DIR = "datasets"

# Grouping datasets by category with their direct URLs
DATASETS = {
    "IMAGES": [
        "https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz",
        "https://www.cs.toronto.edu/~kriz/cifar-100-python.tar.gz",
        "https://www.robots.ox.ac.uk/~vgg/data/flowers/102/102flowers.tgz"
    ],
    "TEXT": [
        "https://s3.amazonaws.com/fast-ai-nlp/wikitext-103.tgz"
    ],
    "STRUCTURED": [
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{m:02d}.parquet"
        for m in range(1, 11)
    ],
    "AUDIO": [
        "https://github.com/karoldvl/ESC-50/archive/master.zip"
    ],
    "BINARY": [
        "https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz",
        "https://biometrics.nist.gov/cs_links/EMNIST/gzip.zip",
        "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/train-images-idx3-ubyte.gz",
        "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/train-labels-idx1-ubyte.gz",
        "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/t10k-images-idx3-ubyte.gz",
        "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/t10k-labels-idx1-ubyte.gz"
    ]
}

def process_file(url, category_dir):
    """Downloads and extracts a single file with retries and corruption checks."""
    filename = url.split('/')[-1]
    filepath = os.path.join(category_dir, filename)
    
    # 1. DOWNLOAD PHASE (with retries)
    max_retries = 3
    download_success = False

    # Skip download if we already successfully unzipped/extracted it previously
    # Checking for known extracted folders/files avoids re-downloading if the archive was deleted
    extracted_indicators = {
        "cifar-10-python.tar.gz": os.path.join(category_dir, "cifar-10-batches-py"),
        "cifar-100-python.tar.gz": os.path.join(category_dir, "cifar-100-python"),
        "cifar-10-binary.tar.gz": os.path.join(category_dir, "cifar-10-batches-bin"),
        "102flowers.tgz": os.path.join(category_dir, "jpg"),
        "wikitext-103.tgz": os.path.join(category_dir, "wikitext-103"),
        "master.zip": os.path.join(category_dir, "ESC-50-master"),
        "gzip.zip": os.path.join(category_dir, "gzip")
    }

    # If the file is already extracted, skip entirely
    if filename in extracted_indicators and os.path.exists(extracted_indicators[filename]):
        return f"⏭️  SKIPPED (Already extracted): {filename}"
        
    # If it's a parquet file that doesn't need extraction and is already downloaded, skip
    if filename.endswith('.parquet') and os.path.exists(filepath):
        return f"⏭️  SKIPPED (Already downloaded): {filename}"
        
    # If it's a .gz file (like Fashion-MNIST) and the extracted .ubyte exists, skip
    extracted_gz_path = filepath[:-3] if filepath.endswith('.gz') and not filepath.endswith('.tar.gz') else None
    if extracted_gz_path and os.path.exists(extracted_gz_path):
        return f"⏭️  SKIPPED (Already extracted): {filename}"

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=30) as response:
                
                # Get expected file size
                expected_size = response.info().get('Content-Length')
                expected_size = int(expected_size) if expected_size else None

                with open(filepath, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
                
                # Verify the file size matches what the server promised
                if expected_size and os.path.getsize(filepath) != expected_size:
                    raise EOFError(f"Incomplete download: {os.path.getsize(filepath)} / {expected_size} bytes")
                
                download_success = True
                break # Success, exit retry loop

        except Exception as e:
            if os.path.exists(filepath):
                os.remove(filepath) # Delete the corrupted partial file
            if attempt < max_retries - 1:
                print(f"  ↻ Retrying {filename} (Attempt {attempt + 2}/{max_retries})...")
                continue # Try again silently
            return f"❌ ERROR downloading {filename} after {max_retries} attempts: {e}"

    if not download_success:
        return f"❌ ERROR: Failed to download {filename}"

    # 2. EXTRACTION PHASE
    try:
        if filepath.endswith('.zip'):
            with zipfile.ZipFile(filepath, 'r') as zf:
                zf.extractall(category_dir)
            return f"✅ DONE (Downloaded & Unzipped): {filename}"
            
        elif filepath.endswith('.tar.gz') or filepath.endswith('.tgz'):
            with tarfile.open(filepath, 'r:gz') as tf:
                # Python 3.12+ security standard to prevent directory traversal
                if hasattr(tarfile, 'data_filter'):
                    tf.extractall(category_dir, filter='data')
                else:
                    tf.extractall(category_dir)
            return f"✅ DONE (Downloaded & Extracted TAR): {filename}"
            
        elif filepath.endswith('.gz') and not filepath.endswith('.tar.gz'):
            # For standalone .gz files (like Fashion-MNIST .ubyte.gz)
            extracted_path = filepath[:-3] # Remove '.gz'
            if not os.path.exists(extracted_path):
                with gzip.open(filepath, 'rb') as f_in, open(extracted_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            return f"✅ DONE (Downloaded & Gunzipped): {filename}"
            
        else:
            # Parquet files or files that don't need extraction
            return f"✅ DONE (Downloaded, no extraction needed): {filename}"
            
    except Exception as e:
        # If extraction fails (e.g. EOFError), the file is corrupt. 
        # Delete it so it doesn't block future attempts.
        if os.path.exists(filepath):
            os.remove(filepath)
        return f"⚠️ Extraction FAILED for {filename} (File deleted, please re-run to try again): {e}"

def main():
    print(f"Creating base directory: {BASE_DIR}")
    os.makedirs(BASE_DIR, exist_ok=True)

    # Build a list of all tasks
    tasks = []
    for category, urls in DATASETS.items():
        category_dir = os.path.join(BASE_DIR, category)
        os.makedirs(category_dir, exist_ok=True)
        for url in urls:
            tasks.append((url, category_dir))

    print(f"\n🚀 Starting parallel download & extraction for {len(tasks)} files...")
    print("This will automatically skip files you have already downloaded and extracted.\n")

    # Use ThreadPoolExecutor to run tasks concurrently
    # max_workers=5 keeps it fast without overwhelming the host servers (rate-limiting)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks to the executor
        future_to_url = {executor.submit(process_file, url, cat_dir): url for url, cat_dir in tasks}
        
        # As each task completes, yield the result
        for future in concurrent.futures.as_completed(future_to_url):
            result = future.result()
            print(result)

    print("\n" + "=" * 60)
    print("🎉 All downloads and extractions complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()

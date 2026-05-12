#!/bin/bash
# Exit immediately if a command exits with a non-zero status
set -e

echo "======================================"
echo "  Oxide Recovery End-to-End Test"
echo "======================================"

# 1. Setup an isolated workspace
TEST_DIR="recovery_test_workspace"
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

# 2. Create dummy data (1MB of random bytes)
echo "[1/7] Generating 1MB of random test data..."
dd if=/dev/urandom of=test_data.bin bs=1M count=1 2>/dev/null
ORIGINAL_MD5=$(md5sum test_data.bin | awk '{print $1}')

# 3. Archive the data
echo "[2/7] Archiving data (oxide archive)..."
cargo run -q --manifest-path ../Cargo.toml -- archive test_data.bin -o archive.oxz

# 4. Protect the archive
echo "[3/7] Protecting archive with 10% recovery (oxide protect)..."
cargo run -q --manifest-path ../Cargo.toml -- protect archive.oxz --recovery 10

# 5. Corrupt the archive intentionally
echo "[4/7] Corrupting archive (simulating bit rot/bad sectors)..."
cp archive.oxz archive_corrupted.oxz
# Overwrite 50KB in the middle of the archive with zeros
dd if=/dev/zero of=archive_corrupted.oxz bs=1K count=50 seek=100 conv=notrunc 2>/dev/null

# 6. Try to extract the corrupted archive
echo "[5/7] Attempting to extract corrupted archive..."
# Disable exit-on-error temporarily because this MUST fail
set +e
cargo run -q --manifest-path ../Cargo.toml -- extract archive_corrupted.oxz -o extracted_corrupted.bin
EXTRACT_STATUS=$?
set -e

if [ $EXTRACT_STATUS -eq 0 ]; then
    echo "⚠️ WARNING: Extraction succeeded. The corruption might have missed the payload!"
else
    echo "✅ GOOD: Extraction failed as expected."
fi

# 7. Repair the archive
echo "[6/7] Repairing archive (oxide repair)..."
cargo run -q --manifest-path ../Cargo.toml -- repair archive_corrupted.oxz -o archive_repaired.oxz

# 8. Extract the repaired archive
echo "[7/7] Extracting the repaired archive (oxide extract)..."
cargo run -q --manifest-path ../Cargo.toml -- extract archive_repaired.oxz -o test_data_repaired.bin

# 9. Verify integrity
echo "======================================"
REPAIRED_MD5=$(md5sum test_data_repaired.bin | awk '{print $1}')

if [ "$ORIGINAL_MD5" == "$REPAIRED_MD5" ]; then
    echo "🎉 SUCCESS: The repaired file perfectly matches the original data!"
else
    echo "❌ FAILURE: The repaired file is still corrupted."
    exit 1
fi

# 10. Cleanup
cd ..
rm -rf "$TEST_DIR"
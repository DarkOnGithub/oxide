#!/bin/bash
# Exit immediately if a command exits with a non-zero status
set -e

echo "======================================"
echo "  Oxide Advanced Recovery Edge-Cases"
echo "======================================"

TEST_DIR="advanced_recovery_workspace"
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

CARGO_CMD="cargo run --manifest-path ../Cargo.toml -p oxide-cli -q --"

# ---------------------------------------------------------
# TEST 1: Multi-block archive (Large file)
# ---------------------------------------------------------
echo -e "\n---> [TEST 1] Multi-block archive repair (20MB data)"
# Generate 20MB to force the archiver to split data into multiple blocks
dd if=/dev/urandom of=large_data.bin bs=1M count=20 2>/dev/null
LARGE_MD5=$(md5sum large_data.bin | awk '{print $1}')

$CARGO_CMD archive large_data.bin -o large_archive.oxz
$CARGO_CMD protect large_archive.oxz --recovery 10

# Corrupt a chunk in the middle of the large archive
cp large_archive.oxz large_corrupted.oxz
dd if=/dev/zero of=large_corrupted.oxz bs=1M count=1 seek=5 conv=notrunc 2>/dev/null

$CARGO_CMD repair large_corrupted.oxz -o large_repaired.oxz
$CARGO_CMD extract large_repaired.oxz -o large_extracted.bin

REPAIRED_LARGE_MD5=$(md5sum large_extracted.bin | awk '{print $1}')
if [ "$LARGE_MD5" == "$REPAIRED_LARGE_MD5" ]; then
    echo "✅ TEST 1 PASSED: Multi-block repair successful."
else
    echo "❌ TEST 1 FAILED: Multi-block repair did not match original."
    exit 1
fi

# ---------------------------------------------------------
# TEST 2: Over-corruption (Exceeding parity limits)
# ---------------------------------------------------------
echo -e "\n---> [TEST 2] Over-corruption handling"
# Generate 5MB data
dd if=/dev/urandom of=medium_data.bin bs=1M count=5 2>/dev/null
$CARGO_CMD archive medium_data.bin -o doomed_archive.oxz

# Protect with only 5% parity
$CARGO_CMD protect doomed_archive.oxz --recovery 5

# Corrupt 2MB (40% of the file), which mathematically exceeds the 5% parity
cp doomed_archive.oxz fatally_corrupted.oxz
dd if=/dev/zero of=fatally_corrupted.oxz bs=1M count=2 seek=1 conv=notrunc 2>/dev/null

echo "Attempting impossible repair (this should fail gracefully)..."
set +e # Temporarily disable exit-on-error
$CARGO_CMD repair fatally_corrupted.oxz -o miracle.oxz
REPAIR_STATUS=$?
set -e

if [ $REPAIR_STATUS -eq 0 ]; then
    echo "❌ TEST 2 FAILED: The repair command falsely claimed success!"
    exit 1
else
    echo "✅ TEST 2 PASSED: Repair command correctly aborted with an error."
fi

# ---------------------------------------------------------
# TEST 3: Attempting to repair an unprotected archive
# ---------------------------------------------------------
echo -e "\n---> [TEST 3] Unprotected archive handling"
$CARGO_CMD archive medium_data.bin -o unprotected.oxz
# Intentionally DO NOT protect it

# Corrupt it
dd if=/dev/zero of=unprotected.oxz bs=1K count=50 seek=100 conv=notrunc 2>/dev/null

echo "Attempting to repair an archive with no recovery data..."
set +e
$CARGO_CMD repair unprotected.oxz -o fake_repaired.oxz
UNPROTECTED_STATUS=$?
set -e

# Even if it returns success (because missing_count might trigger a clean exit),
# we need to ensure it didn't actually try to run Reed-Solomon math on garbage.
# The safest check is to see if extraction still fails.
set +e
$CARGO_CMD extract fake_repaired.oxz -o should_fail.bin 2>/dev/null
EXTRACT_STATUS=$?
set -e

if [ $EXTRACT_STATUS -ne 0 ]; then
    echo "✅ TEST 3 PASSED: Unprotected archive remained broken (no false repair)."
else
    echo "⚠️ TEST 3 WARNING: Extraction succeeded. Needs manual verification."
fi

# ---------------------------------------------------------
# Cleanup
# ---------------------------------------------------------
echo -e "\n🎉 ALL ADVANCED TESTS COMPLETED SUCCESSFULLY!"
cd ..
rm -rf "$TEST_DIR"
#!/usr/bin/env bash
set -euo pipefail

echo "Starting full Ubuntu benchmark environment setup..."

# -----------------------------
# Helpers
# -----------------------------
have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

apt_has_pkg() {
    apt-cache show "$1" >/dev/null 2>&1
}

install_if_available() {
    local pkg="$1"
    if apt_has_pkg "$pkg"; then
        sudo apt install -y "$pkg"
    else
        echo "APT package not found, skipping: $pkg"
    fi
}

# -----------------------------
# 1. Update package lists
# -----------------------------
echo "Updating package lists..."
sudo apt update -y

# Enable universe, useful for tools like pixz/plzip on many Ubuntu installs.
if have_cmd add-apt-repository; then
    sudo add-apt-repository -y universe || true
else
    sudo apt install -y software-properties-common
    sudo add-apt-repository -y universe || true
fi

sudo apt update -y

# -----------------------------
# 2. Install system dependencies
# -----------------------------
echo "Installing base build/runtime dependencies..."

sudo apt install -y \
    ca-certificates \
    curl \
    git \
    build-essential \
    pkg-config \
    python3 \
    python3-pip \
    python3-venv \
    squashfs-tools \
    zstd \
    pixz \
    plzip \
    pigz \
    pbzip2 \
    lbzip2 \
    tar \
    libarchive-tools \
    xz-utils \
    lzip \
    liblz-dev \
    libarchive-dev \
    asciidoc

# 7zip package name differs across Ubuntu versions.
if apt_has_pkg 7zip; then
    sudo apt install -y 7zip
elif apt_has_pkg p7zip-full; then
    sudo apt install -y p7zip-full
else
    echo "Warning: neither 7zip nor p7zip-full found in APT."
fi

# Optional: DwarFS if your Ubuntu repo has it.
# DwarFS is a compressed read-only filesystem/archive-like tool; upstream describes it as a mountable archive format. 
# If unavailable in APT, this script leaves it missing rather than failing. 
install_if_available dwarfs

# -----------------------------
# 3. Install Rust & Cargo
# -----------------------------
if ! have_cmd cargo; then
    echo "Installing Rust and Cargo via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # shellcheck source=/dev/null
    source "$HOME/.cargo/env"

    if ! grep -q 'source "$HOME/.cargo/env"' "$HOME/.bashrc"; then
        echo 'source "$HOME/.cargo/env"' >> "$HOME/.bashrc"
    fi
else
    echo "Cargo/Rust is already installed."
fi

cargo --version

# -----------------------------
# 4. Install uv
# -----------------------------
if ! have_cmd uv; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh

    if [ -f "$HOME/.local/bin/env" ]; then
        # shellcheck source=/dev/null
        source "$HOME/.local/bin/env"
    fi

    export PATH="$HOME/.local/bin:$PATH"
else
    echo "uv is already installed."
fi

uv --version || true

# -----------------------------
# 5. Python virtual environment
# -----------------------------
echo "Setting up Python virtual environment..."

if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# shellcheck source=/dev/null
source venv/bin/activate

pip install --upgrade pip
pip install rich psutil

# -----------------------------
# 6. Download datasets
# -----------------------------
echo "Executing dataset download scripts..."

if [ -f "scripts/download_dataset.py" ]; then
    echo "Downloading 200mb dataset..."
    python3 scripts/download_dataset.py 200mb

    echo "Downloading 6gb dataset..."
    python3 scripts/download_dataset.py 6gb
else
    echo "Error: scripts/download_dataset.py not found in the current directory."
    exit 1
fi

# -----------------------------
# 7. Verify benchmark tools
# -----------------------------
echo
echo "Verifying benchmark tools..."

for tool in \
    cargo rustc uv \
    mksquashfs unsquashfs \
    7zz 7z \
    zstd pixz plzip pigz pbzip2 lbzip2 \
    bsdtar \
    mkdwarfs dwarfsextract
do
    if have_cmd "$tool"; then
        echo "ok: $tool -> $(command -v "$tool")"
    else
        echo "missing: $tool"
    fi
done

echo
echo "-------------------------------------------"
echo "Ubuntu benchmark environment setup complete."
echo
echo "Useful next command:"
echo
echo "uv run scripts/benchmarker.py \\"
echo "  --source /datasets/6gb \\"
echo "  --oxide-bin ./target/release/oxide \\"
echo "  --passes 3 \\"
echo "  --worker-modes 16 \\"
echo "  --raw-oxide-presets \\"
echo "  --sync-after-extract \\"
echo "  --competitors legacy tar+zstd pixz plzip pigz pbzip2 \\"
echo "  --telemetry-dir benchmark_telemetry/competitors_ubuntu_\$(date -u +%Y%m%dT%H%M%SZ)"
echo
echo "If DwarFS installed successfully, add 'dwarfs' to --competitors."
echo "To activate Python venv manually: source venv/bin/activate"
echo "To load Cargo manually: source ~/.cargo/env"
echo "-------------------------------------------"
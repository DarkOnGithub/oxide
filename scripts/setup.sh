#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting full environment setup..."

# 1. Update package lists
echo "Updating package lists..."
sudo apt update -y

# 2. Install System Dependencies
# Includes 7zip, squashfs-tools, and Python environment tools
echo "Installing 7zip, squashfs-tools, curl, build-essential, python3-pip, and python3-venv..."
sudo apt install -y 7zip squashfs-tools curl build-essential python3-pip python3-venv

# 3. Install Rust
if ! command -v rustc &> /dev/null; then
    echo "Installing Rust via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
else
    echo "Rust is already installed."
fi

# 4. Set up Python Virtual Environment and Install Rich
# Ubuntu 24.04 requires a virtual environment for pip installs
echo "Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# Activate venv and install rich
source venv/bin/activate
pip install --upgrade pip
pip install rich

# 5. Run Dataset Downloads
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

echo "-------------------------------------------"
echo "All tasks complete!"
echo "To use this environment again, run: source venv/bin/activate"
echo "-------------------------------------------"
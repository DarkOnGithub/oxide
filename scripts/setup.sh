#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting full environment setup..."

# 1. Update package lists
echo "Updating package lists..."
sudo apt update -y

# 2. Install System Dependencies
echo "Installing 7zip, squashfs-tools, curl, build-essential, python3-pip, and python3-venv..."
sudo apt install -y 7zip squashfs-tools curl build-essential python3-pip python3-venv

# 3. Install Rust & Cargo
if ! command -v cargo &> /dev/null; then
    echo "Installing Rust and Cargo via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    
    # Load Cargo into the current script session
    source "$HOME/.cargo/env"
    
    # Ensure Cargo is available in future terminal sessions
    if ! grep -q 'source "$HOME/.cargo/env"' "$HOME/.bashrc"; then
        echo 'source "$HOME/.cargo/env"' >> "$HOME/.bashrc"
    fi
else
    echo "Cargo/Rust is already installed."
fi

# Verify Cargo is working
cargo --version

# 4. Set up Python Virtual Environment and Install Rich
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
echo "Cargo is now available as a command."
echo "To use the Python environment, run: source venv/bin/activate"
echo "To use Cargo in this current window, run: source ~/.bashrc"
echo "-------------------------------------------"
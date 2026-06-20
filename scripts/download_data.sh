#!/bin/bash
# Download Sentiment140 dataset
# Dataset: https://www.kaggle.com/datasets/kazanova/sentiment140

KAGGLE_CMD="kaggle datasets download -d kazanova/sentiment140 --unzip -p ./data/raw"
DIRECT_URL="https://raw.githubusercontent.com/zayed孤儿/Sentiment140Dataset/master/training.1600000.processed.noemoticon.csv"

set -e

echo "=== Downloading Sentiment140 dataset ==="

cd "$(dirname "$0")/.."

# Method 1: Kaggle CLI
if command -v kaggle &> /dev/null; then
    echo "[Method 1] Using Kaggle CLI..."
    mkdir -p data/raw
    $KAGGLE_CMD
    mv data/raw/*.csv data/raw/sentiment140_full.csv 2>/dev/null || true
    echo "Dataset downloaded via Kaggle."
else
    echo "[Method 2] Direct URL (Wget/Curl)..."
    mkdir -p data/raw
    curl -L -o data/raw/sentiment140_full.csv "$DIRECT_URL" --progress-bar
    echo "Dataset downloaded via Curl."
fi

# Count lines
if [ -f data/raw/sentiment140_full.csv ]; then
    echo "Dataset size: $(wc -l < data/raw/sentiment140_full.csv) lines"
    echo "File size: $(du -h data/raw/sentiment140_full.csv | cut -f1)"
fi

echo "=== Download complete ==="

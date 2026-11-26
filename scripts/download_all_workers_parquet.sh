#!/usr/bin/env bash
set -e

# ==============================================================================
# Script: download_all_workers_parquet.sh
# Purpose: Downloads all Parquet files from Server 2, Server 3, and Server 4
# ==============================================================================

BASE_DOWNLOAD_DIR="./downloads"
mkdir -p "$BASE_DOWNLOAD_DIR/server2"
mkdir -p "$BASE_DOWNLOAD_DIR/server3"
mkdir -p "$BASE_DOWNLOAD_DIR/server4"

echo "============================================================"
echo "📥 DOWNLOADING PARQUET FILES FROM ALL 3 WORKERS"
echo "============================================================"

echo ""
echo "🔹 [1/3] Downloading from Server 2 (nirmal@173.249.56.10)..."
scp -r nirmal@173.249.56.10:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DOWNLOAD_DIR/server2/" 2>/dev/null || echo "   (No files or already copied)"

echo ""
echo "🔹 [2/3] Downloading from Server 3 (app_user@20.77.104.131)..."
scp -r app_user@20.77.104.131:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DOWNLOAD_DIR/server3/" 2>/dev/null || echo "   (No files or already copied)"

echo ""
echo "🔹 [3/3] Downloading from Server 4 (root@173.249.52.244)..."
scp -r root@173.249.52.244:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DOWNLOAD_DIR/server4/" 2>/dev/null || echo "   (No files or already copied)"

echo ""
echo "============================================================"
echo "✅ All downloads complete! Summary of downloaded files:"
echo "============================================================"
find "$BASE_DOWNLOAD_DIR" -type f -name "*.parquet"

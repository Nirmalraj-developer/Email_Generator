#!/usr/bin/env bash
set -e

# ==============================================================================
# Script: fresh_download_cluster_parquet.sh
# Purpose: Downloads cleanly organized Parquet files from all workers
# ==============================================================================

BASE_DIR="./downloads"
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR/server2_173.249.56.10"
mkdir -p "$BASE_DIR/server3_20.77.104.131"
mkdir -p "$BASE_DIR/server4_173.249.52.244"

echo "=================================================================="
echo "📥 FRESHLY DOWNLOADING PARQUET DATA FROM CLUSTER WORKERS"
echo "=================================================================="

echo ""
echo "🔹 [1/3] Downloading from Server 2 (nirmal@173.249.56.10)..."
scp -r nirmal@173.249.56.10:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DIR/server2_173.249.56.10/" 2>/dev/null || true

echo ""
echo "🔹 [2/3] Downloading from Server 3 (app_user@20.77.104.131)..."
scp -r app_user@20.77.104.131:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DIR/server3_20.77.104.131/" 2>/dev/null || true

echo ""
echo "🔹 [3/3] Downloading from Server 4 (root@173.249.52.244)..."
scp -r root@173.249.52.244:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DIR/server4_173.249.52.244/" 2>/dev/null || true

echo ""
echo "=================================================================="
echo "✅ Download Complete! File Hierarchy:"
echo "=================================================================="
find "$BASE_DIR" -type f -name "*.parquet" | sort

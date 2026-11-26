#!/usr/bin/env bash
set -e

# ==============================================================================
# Script: export_all_and_report.sh
# Purpose: Downloads all Parquet files from active workers (Server 2 & Server 4),
#          converts to neat JSON per server, and generates an executive audit report.
# ==============================================================================

BASE_DIR="./downloads"
mkdir -p "$BASE_DIR/server2_173.249.56.10"
mkdir -p "$BASE_DIR/server4_173.249.52.244"

# Remove any empty or unused server3 folder if it has no data
if [ -d "$BASE_DIR/server3_207.180.202.124" ] && [ -z "$(ls -A "$BASE_DIR/server3_207.180.202.124" 2>/dev/null)" ]; then
  rm -rf "$BASE_DIR/server3_207.180.202.124"
fi
if [ -d "$BASE_DIR/server3_20.77.104.131" ] && [ -z "$(ls -A "$BASE_DIR/server3_20.77.104.131" 2>/dev/null)" ]; then
  rm -rf "$BASE_DIR/server3_20.77.104.131"
fi

echo "=================================================================="
echo "📥 DOWNLOADING PARQUET DATA FROM CLUSTER WORKERS"
echo "=================================================================="

echo "🔹 [1/2] Downloading from Server 2 (173.249.56.10)..."
scp -r nirmal@173.249.56.10:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DIR/server2_173.249.56.10/" 2>/dev/null || true

echo "🔹 [2/2] Downloading from Server 4 (173.249.52.244)..."
scp -r root@173.249.52.244:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DIR/server4_173.249.52.244/" 2>/dev/null || true

echo ""
echo "=================================================================="
echo "⚙️ RUNNING STREAMING EXPORT & EXECUTIVE AUDIT REPORT"
echo "=================================================================="
node scripts/generate_neat_exports_and_report.js ./downloads

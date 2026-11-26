#!/usr/bin/env bash
set -e

# ==============================================================================
# Script: export_and_audit_3stages.sh
# Purpose: Downloads & exports the 3 clean stages (valid, webhook_triggers, invalid)
# ==============================================================================

BASE_DIR="./downloads"
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR/server2_173.249.56.10"
mkdir -p "$BASE_DIR/server3_20.77.104.131"
mkdir -p "$BASE_DIR/server4_173.249.52.244"

echo "=================================================================="
echo "📥 DOWNLOADING 3-STAGE DATA FROM ALL WORKERS"
echo "=================================================================="

echo "🔹 [1/3] Server 2 (173.249.56.10)..."
scp -r nirmal@173.249.56.10:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DIR/server2_173.249.56.10/" 2>/dev/null || true

echo "🔹 [2/3] Server 3 (20.77.104.131)..."
scp -r app_user@20.77.104.131:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DIR/server3_20.77.104.131/" 2>/dev/null || true

echo "🔹 [3/3] Server 4 (173.249.52.244)..."
scp -r root@173.249.52.244:~/EmailFinder_workerNode/Email_Generator/data/* "$BASE_DIR/server4_173.249.52.244/" 2>/dev/null || true

echo ""
echo "=================================================================="
echo "📦 CONVERTING PARQUET TO JSON PER SERVER"
echo "=================================================================="
node scripts/exportParquetToJsonPerServer.js ./downloads

echo ""
echo "=================================================================="
echo "📊 GENERATING PERSON-LEVEL TALLY REPORT"
echo "=================================================================="
node scripts/exportPersonLevelTally.js

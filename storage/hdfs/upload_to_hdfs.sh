#!/usr/bin/env bash

# =============================================================================
# Upload Logistics & Supply Chain data to HDFS
# =============================================================================
#
# Purpose:
#   Create the project directory structure in HDFS and upload raw/processed
#   datasets from the local project into the Hadoop Distributed File System.
#
# Expected local structure:
#   data/raw/
#   data/processed/
#
# Expected HDFS structure:
#   /logistics/raw/
#   /logistics/processed/
#
# Usage:
#   chmod +x storage/hdfs/upload_to_hdfs.sh
#   ./storage/hdfs/upload_to_hdfs.sh
#
# Optional:
#   HDFS_USER=hduser ./storage/hdfs/upload_to_hdfs.sh
#   HDFS_NAMENODE=hdfs://localhost:9000 ./storage/hdfs/upload_to_hdfs.sh
# =============================================================================

set -euo pipefail

HDFS_USER="${HDFS_USER:-$(whoami)}"
HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://localhost:9000}"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RAW_DIR="${PROJECT_ROOT}/data/raw"
PROCESSED_DIR="${PROJECT_ROOT}/data/processed"

HDFS_ROOT="${HDFS_NAMENODE}/user/${HDFS_USER}/logistics"
HDFS_RAW="${HDFS_ROOT}/raw"
HDFS_PROCESSED="${HDFS_ROOT}/processed"

echo "============================================================"
echo "Logistics & Supply Chain - HDFS Upload"
echo "============================================================"
echo "Project root : ${PROJECT_ROOT}"
echo "HDFS root    : ${HDFS_ROOT}"
echo

# ---------------------------------------------------------------------------
# Validate local directories
# ---------------------------------------------------------------------------

if [[ ! -d "${RAW_DIR}" ]]; then
    echo "ERROR: Raw data directory not found: ${RAW_DIR}"
    exit 1
fi

if [[ ! -d "${PROCESSED_DIR}" ]]; then
    echo "ERROR: Processed data directory not found: ${PROCESSED_DIR}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Validate Hadoop CLI
# ---------------------------------------------------------------------------

if ! command -v hdfs >/dev/null 2>&1; then
    echo "ERROR: 'hdfs' command not found."
    echo "Make sure Hadoop is installed and HADOOP_HOME/bin is in PATH."
    exit 1
fi

# ---------------------------------------------------------------------------
# Create HDFS directories
# ---------------------------------------------------------------------------

echo "[1/3] Creating HDFS directories..."

hdfs dfs -mkdir -p "${HDFS_RAW}"
hdfs dfs -mkdir -p "${HDFS_PROCESSED}"

# ---------------------------------------------------------------------------
# Upload datasets
# ---------------------------------------------------------------------------

echo "[2/3] Uploading raw data..."

if find "${RAW_DIR}" -maxdepth 1 -type f -name "*.csv" | grep -q .; then
    hdfs dfs -put -f "${RAW_DIR}"/*.csv "${HDFS_RAW}/"
else
    echo "No raw CSV files found. Skipping raw upload."
fi

echo "Uploading processed data..."

if find "${PROCESSED_DIR}" -maxdepth 1 -type f -name "*.csv" | grep -q .; then
    hdfs dfs -put -f "${PROCESSED_DIR}"/*.csv "${HDFS_PROCESSED}/"
else
    echo "No processed CSV files found. Skipping processed upload."
fi

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------

echo "[3/3] Verifying HDFS contents..."
echo

echo "Raw data:"
hdfs dfs -ls "${HDFS_RAW}" || true

echo
echo "Processed data:"
hdfs dfs -ls "${HDFS_PROCESSED}" || true

echo
echo "============================================================"
echo "HDFS upload completed."
echo "============================================================"

#!/usr/bin/env bash
#
# Logistics Supply Chain Intelligence
# Local cleanup script
#
# Usage:
#   bash scripts/cleanup.sh
#
# By default this removes generated runtime artifacts while preserving:
#   - source code
#   - raw project datasets
#   - processed datasets
#   - SQL, dashboards, documentation and configuration
#
# Optional:
#   CLEAN_DATA=true bash scripts/cleanup.sh
#   CLEAN_ENV=true bash scripts/cleanup.sh
#   CLEAN_ALL=true bash scripts/cleanup.sh
#
# CLEAN_DATA=true removes generated processed/output artifacts and the
# synthetic raw datasets. CLEAN_ENV=true removes the local .env and virtual
# environment. CLEAN_ALL=true performs both categories.
#
# This script asks for confirmation before destructive cleanup.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

CLEAN_DATA="${CLEAN_DATA:-false}"
CLEAN_ENV="${CLEAN_ENV:-false}"
CLEAN_ALL="${CLEAN_ALL:-false}"

if [[ "$CLEAN_ALL" == "true" ]]; then
    CLEAN_DATA="true"
    CLEAN_ENV="true"
fi

echo "============================================================"
echo " Logistics Supply Chain Intelligence - Cleanup"
echo "============================================================"
echo "Project root: $PROJECT_ROOT"
echo

echo "Cleanup mode:"
echo "  CLEAN_DATA=$CLEAN_DATA"
echo "  CLEAN_ENV=$CLEAN_ENV"
echo "  CLEAN_ALL=$CLEAN_ALL"
echo

# ---------------------------------------------------------------------------
# Build the list of paths that will be removed.
# ---------------------------------------------------------------------------

REMOVE_PATHS=(
    "logs"
    "tmp"
    "analytics/output"
    ".pytest_cache"
)

if [[ "$CLEAN_DATA" == "true" ]]; then
    REMOVE_PATHS+=(
        "data/raw/orders.csv"
        "data/raw/customers.csv"
        "data/raw/drivers.csv"
        "data/raw/vehicles.csv"
        "data/raw/warehouses.csv"
        "data/raw/gps_tracking.csv"
        "data/raw/weather.csv"
        "data/raw/traffic.csv"
        "data/raw/roads.csv"
        "data/raw/delivery_updates.csv"
        "data/processed/orders_clean.csv"
        "data/processed/gps_clean.csv"
        "data/processed/weather_clean.csv"
        "data/processed/delivery_features.csv"
        "data/sample/sample_data.csv"
    )
fi

if [[ "$CLEAN_ENV" == "true" ]]; then
    REMOVE_PATHS+=(
        ".env"
        ".venv"
    )
fi

echo "The following paths may be removed:"
for path in "${REMOVE_PATHS[@]}"; do
    echo "  - $path"
done

echo
read -r -p "Continue? Type 'yes' to confirm: " confirmation

if [[ "$confirmation" != "yes" ]]; then
    echo "Cleanup cancelled."
    exit 0
fi

# ---------------------------------------------------------------------------
# Remove paths safely.
# ---------------------------------------------------------------------------

echo
echo "Cleaning..."

for relative_path in "${REMOVE_PATHS[@]}"; do
    path="$PROJECT_ROOT/$relative_path"

    if [[ -e "$path" || -L "$path" ]]; then
        rm -rf -- "$path"
        echo "  Removed: $relative_path"
    else
        echo "  Skip:    $relative_path (not present)"
    fi
done

# ---------------------------------------------------------------------------
# Recreate directories that the project expects to exist.
# ---------------------------------------------------------------------------

mkdir -p \
    data/raw \
    data/processed \
    data/sample \
    analytics/output \
    logs \
    tmp

echo
echo "Base runtime directories recreated."

# ---------------------------------------------------------------------------
# Final notes.
# ---------------------------------------------------------------------------

echo
echo "============================================================"
echo " Cleanup completed"
echo "============================================================"

if [[ "$CLEAN_DATA" == "true" ]]; then
    echo "Synthetic data was removed."
    echo "Regenerate it with:"
    echo "  python scripts/generate_data.py"
fi

if [[ "$CLEAN_ENV" == "true" ]]; then
    echo "Local environment files were removed."
    echo "Recreate the environment with:"
    echo "  bash scripts/setup.sh"
fi

echo
echo "Source code, SQL, dashboards, documentation and configuration"
echo "files were preserved."
echo "============================================================"

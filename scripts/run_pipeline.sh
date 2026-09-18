#!/usr/bin/env bash
#
# Logistics Supply Chain Intelligence
# End-to-end local pipeline runner
#
# Usage:
#   bash scripts/run_pipeline.sh
#
# Optional environment variables:
#   RUN_INGESTION=true|false
#   RUN_SPARK=true|false
#   RUN_DBT=true|false
#   RUN_ANALYTICS=true|false
#   RUN_TESTS=true|false
#   RUN_STREAMING=true|false
#   PYTHON_BIN=python3
#   SPARK_SUBMIT=spark-submit
#   DBT_BIN=dbt
#
# Defaults are intentionally conservative:
# - Batch ingestion runs.
# - Spark batch transformations run when spark-submit is available.
# - dbt runs only when the dbt executable is available.
# - Analytics runs.
# - Tests run.
# - Streaming is opt-in because it requires Kafka/Spark streaming services.
#
# The script fails clearly when a required stage fails. Optional external
# integrations are skipped with an explanation when their local tooling is
# unavailable.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
DBT_BIN="${DBT_BIN:-dbt}"

RUN_INGESTION="${RUN_INGESTION:-true}"
RUN_SPARK="${RUN_SPARK:-true}"
RUN_DBT="${RUN_DBT:-true}"
RUN_ANALYTICS="${RUN_ANALYTICS:-true}"
RUN_TESTS="${RUN_TESTS:-true}"
RUN_STREAMING="${RUN_STREAMING:-false}"

echo "============================================================"
echo " Logistics Supply Chain Intelligence - Pipeline Runner"
echo "============================================================"
echo "Project root: $PROJECT_ROOT"
echo

require_file() {
    local file="$1"

    if [[ ! -f "$file" ]]; then
        echo "ERROR: Required file not found: $file"
        exit 1
    fi
}

run_python() {
    local script="$1"
    shift || true

    require_file "$script"

    echo
    echo ">>> Python: $script"
    "$PYTHON_BIN" "$script" "$@"
}

run_spark() {
    local script="$1"
    shift || true

    require_file "$script"

    if ! command -v "$SPARK_SUBMIT" >/dev/null 2>&1; then
        echo "WARNING: $SPARK_SUBMIT is not available."
        echo "Skipping Spark stage: $script"
        return 0
    fi

    echo
    echo ">>> Spark: $script"
    "$SPARK_SUBMIT" "$script" "$@"
}

# ---------------------------------------------------------------------------
# 0. Environment checks
# ---------------------------------------------------------------------------

echo "[0/7] Checking runtime..."

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "ERROR: Python executable '$PYTHON_BIN' was not found."
    exit 1
fi

mkdir -p \
    data/raw \
    data/processed \
    analytics/output \
    logs \
    tmp

echo "Python: $("$PYTHON_BIN" --version)"
echo "Pipeline configuration:"
echo "  RUN_INGESTION=$RUN_INGESTION"
echo "  RUN_SPARK=$RUN_SPARK"
echo "  RUN_DBT=$RUN_DBT"
echo "  RUN_ANALYTICS=$RUN_ANALYTICS"
echo "  RUN_TESTS=$RUN_TESTS"
echo "  RUN_STREAMING=$RUN_STREAMING"

# ---------------------------------------------------------------------------
# 1. Batch ingestion
# ---------------------------------------------------------------------------

if [[ "$RUN_INGESTION" == "true" ]]; then
    echo
    echo "[1/7] Running batch ingestion..."

    run_python ingestion/batch/orders_ingestion.py
    run_python ingestion/batch/customer_ingestion.py
    run_python ingestion/batch/warehouse_ingestion.py
else
    echo
    echo "[1/7] Batch ingestion disabled."
fi

# ---------------------------------------------------------------------------
# 2. Spark batch transformations
# ---------------------------------------------------------------------------

if [[ "$RUN_SPARK" == "true" ]]; then
    echo
    echo "[2/7] Running Spark batch transformations..."

    if command -v "$SPARK_SUBMIT" >/dev/null 2>&1; then
        run_spark spark/batch/clean_orders.py
        run_spark spark/batch/clean_gps.py
        run_spark spark/batch/clean_weather.py
        run_spark spark/batch/create_features.py
    else
        echo "WARNING: spark-submit is not installed or not on PATH."
        echo "Skipping Spark transformations."
        echo "Install Apache Spark or set SPARK_SUBMIT to its executable."
    fi
else
    echo
    echo "[2/7] Spark transformations disabled."
fi

# ---------------------------------------------------------------------------
# 3. dbt transformations
# ---------------------------------------------------------------------------

if [[ "$RUN_DBT" == "true" ]]; then
    echo
    echo "[3/7] Running dbt transformations..."

    if command -v "$DBT_BIN" >/dev/null 2>&1; then
        pushd dbt >/dev/null

        "$DBT_BIN" debug --profiles-dir .
        "$DBT_BIN" run --profiles-dir .
        "$DBT_BIN" test --profiles-dir .

        popd >/dev/null
    else
        echo "WARNING: dbt executable was not found."
        echo "Skipping dbt stage."
        echo "Install dbt and configure the project profile before enabling it."
    fi
else
    echo
    echo "[3/7] dbt transformations disabled."
fi

# ---------------------------------------------------------------------------
# 4. Analytics and ML
# ---------------------------------------------------------------------------

if [[ "$RUN_ANALYTICS" == "true" ]]; then
    echo
    echo "[4/7] Running analytics and ML..."

    run_python analytics/python/eda.py
    run_python analytics/python/feature_engineering.py
    run_python analytics/python/demand_forecasting.py
    run_python analytics/python/delivery_prediction.py
    run_python analytics/python/route_analysis.py
else
    echo
    echo "[4/7] Analytics disabled."
fi

# ---------------------------------------------------------------------------
# 5. Streaming
# ---------------------------------------------------------------------------

if [[ "$RUN_STREAMING" == "true" ]]; then
    echo
    echo "[5/7] Running streaming pipeline..."

    if ! command -v "$SPARK_SUBMIT" >/dev/null 2>&1; then
        echo "ERROR: RUN_STREAMING=true but spark-submit is unavailable."
        exit 1
    fi

    echo "Streaming requires a reachable Kafka broker."
    echo "The producer/consumer scripts are designed for the project's"
    echo "documented Kafka topics."

    run_python kafka/producers/order_producer.py --help >/dev/null
    run_python kafka/producers/gps_producer.py --help >/dev/null

    echo "Streaming components are available."
    echo "Start Kafka and run the required producers/streaming jobs separately"
    echo "when a long-running streaming session is desired."
else
    echo
    echo "[5/7] Streaming disabled (default)."
fi

# ---------------------------------------------------------------------------
# 6. Tests
# ---------------------------------------------------------------------------

if [[ "$RUN_TESTS" == "true" ]]; then
    echo
    echo "[6/7] Running test suite..."

    if "$PYTHON_BIN" -m pytest --version >/dev/null 2>&1; then
        "$PYTHON_BIN" -m pytest tests/ -v
    else
        echo "WARNING: pytest is not available in the selected Python environment."
        echo "Skipping tests."
    fi
else
    echo
    echo "[6/7] Tests disabled."
fi

# ---------------------------------------------------------------------------
# 7. Output summary
# ---------------------------------------------------------------------------

echo
echo "[7/7] Checking pipeline outputs..."

OUTPUTS=(
    "data/processed/orders_clean.csv"
    "data/processed/gps_clean.csv"
    "data/processed/weather_clean.csv"
    "data/processed/delivery_features.csv"
)

for output in "${OUTPUTS[@]}"; do
    if [[ -f "$output" ]]; then
        echo "  OK    $output"
    else
        echo "  MISS  $output"
    fi
done

echo
echo "============================================================"
echo " Pipeline run completed"
echo "============================================================"
echo
echo "Useful commands:"
echo "  Full batch + analytics + tests:"
echo "    bash scripts/run_pipeline.sh"
echo
echo "  Skip Spark:"
echo "    RUN_SPARK=false bash scripts/run_pipeline.sh"
echo
echo "  Skip dbt:"
echo "    RUN_DBT=false bash scripts/run_pipeline.sh"
echo
echo "  Skip analytics:"
echo "    RUN_ANALYTICS=false bash scripts/run_pipeline.sh"
echo
echo "  Run streaming checks:"
echo "    RUN_STREAMING=true bash scripts/run_pipeline.sh"
echo "============================================================"

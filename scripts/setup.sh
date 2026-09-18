#!/usr/bin/env bash
#
# Logistics Supply Chain Intelligence
# Local project setup script
#
# Usage:
#   bash scripts/setup.sh
#
# What this script does:
#   1. Checks required local commands.
#   2. Creates the project's expected directories.
#   3. Creates a Python virtual environment when possible.
#   4. Installs Python dependencies into that environment.
#   5. Creates a local .env from .env.example if .env does not exist.
#   6. Creates required Kafka topic definitions when Kafka CLI is available.
#
# The script does NOT start production services, create cloud resources, or
# require cloud credentials. AWS/API integrations remain optional.
#
# Safe to run multiple times.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

VENV_DIR="${VENV_DIR:-.venv}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "============================================================"
echo " Logistics Supply Chain Intelligence - Setup"
echo "============================================================"
echo "Project root: $PROJECT_ROOT"
echo

# ---------------------------------------------------------------------------
# 1. Check Python
# ---------------------------------------------------------------------------

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "ERROR: Python executable '$PYTHON_BIN' was not found."
    echo "Install Python 3.10+ and run this script again."
    exit 1
fi

PYTHON_VERSION="$("$PYTHON_BIN" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"

echo "Python: $PYTHON_VERSION"

if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
    echo "ERROR: Python 3.10 or newer is required."
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Create project directories
# ---------------------------------------------------------------------------

echo
echo "[1/6] Creating project directories..."

mkdir -p \
    data/raw \
    data/processed \
    data/sample \
    analytics/output \
    analytics/output/eda \
    analytics/output/forecasting \
    analytics/output/prediction \
    analytics/output/routes \
    logs \
    models \
    notebooks \
    tmp

echo "Directory structure ready."

# ---------------------------------------------------------------------------
# 3. Create virtual environment
# ---------------------------------------------------------------------------

echo
echo "[2/6] Preparing Python virtual environment..."

if [[ ! -d "$VENV_DIR" ]]; then
    "$PYTHON_BIN" -m venv "$VENV_DIR"
    echo "Created virtual environment: $VENV_DIR"
else
    echo "Virtual environment already exists: $VENV_DIR"
fi

if [[ -x "$VENV_DIR/bin/python" ]]; then
    VENV_PYTHON="$VENV_DIR/bin/python"
    VENV_PIP="$VENV_DIR/bin/pip"
elif [[ -x "$VENV_DIR/Scripts/python.exe" ]]; then
    VENV_PYTHON="$VENV_DIR/Scripts/python.exe"
    VENV_PIP="$VENV_DIR/Scripts/pip.exe"
else
    echo "ERROR: Could not locate Python inside $VENV_DIR."
    exit 1
fi

echo "Virtual environment Python: $VENV_PYTHON"

# ---------------------------------------------------------------------------
# 4. Install Python dependencies
# ---------------------------------------------------------------------------

echo
echo "[3/6] Installing Python dependencies..."

if [[ ! -f requirements.txt ]]; then
    echo "ERROR: requirements.txt was not found."
    exit 1
fi

"$VENV_PYTHON" -m pip install --upgrade pip
"$VENV_PIP" install -r requirements.txt

echo "Python dependencies installed."

# ---------------------------------------------------------------------------
# 5. Prepare environment configuration
# ---------------------------------------------------------------------------

echo
echo "[4/6] Preparing environment configuration..."

if [[ ! -f .env.example ]]; then
    echo "WARNING: .env.example was not found; skipping environment template."
else
    if [[ ! -f .env ]]; then
        cp .env.example .env
        echo "Created .env from .env.example."
        echo "Review .env before enabling external APIs or cloud services."
    else
        echo ".env already exists; leaving it unchanged."
    fi
fi

# ---------------------------------------------------------------------------
# 6. Check optional infrastructure and create local Kafka topics
# ---------------------------------------------------------------------------

echo
echo "[5/6] Checking optional infrastructure..."

check_command() {
    local command_name="$1"

    if command -v "$command_name" >/dev/null 2>&1; then
        echo "  OK      $command_name"
        return 0
    fi

    echo "  SKIP    $command_name not found"
    return 1
}

check_command docker || true
check_command docker-compose || true
check_command hdfs || true
check_command spark-submit || true
check_command hive || true
check_command airflow || true
check_command dbt || true

# Kafka topic creation is intentionally optional. The project may use Docker
# Kafka or an independently installed Kafka distribution.
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo
echo "[6/6] Checking Kafka topic tooling..."

if command -v kafka-topics.sh >/dev/null 2>&1; then
    echo "Kafka CLI detected. Attempting idempotent topic creation..."

    TOPICS=(
        "orders"
        "gps_tracking"
        "delivery_updates"
        "traffic_updates"
        "weather_updates"
    )

    for topic in "${TOPICS[@]}"; do
        if kafka-topics.sh \
            --bootstrap-server "$KAFKA_BOOTSTRAP" \
            --create \
            --if-not-exists \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 1 >/dev/null 2>&1; then
            echo "  Kafka topic ready: $topic"
        else
            echo "  Kafka topic not created: $topic"
            echo "  Make sure Kafka is running at $KAFKA_BOOTSTRAP."
        fi
    done
else
    echo "Kafka CLI not found."
    echo "Topics are documented in kafka/topics.md."
    echo "Create them when Kafka is available."
fi

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------

echo
echo "============================================================"
echo " Setup completed"
echo "============================================================"
echo
echo "Next steps:"
echo "  1. Activate the virtual environment:"
echo "       source $VENV_DIR/bin/activate"
echo
echo "  2. Review environment settings:"
echo "       .env"
echo
echo "  3. Generate/refresh project data:"
echo "       python scripts/generate_data.py"
echo
echo "  4. Run tests:"
echo "       pytest tests/ -v"
echo
echo "  5. Start optional Docker infrastructure:"
echo "       docker compose up -d"
echo
echo "  6. Run the project pipeline:"
echo "       bash scripts/run_pipeline.sh"
echo
echo "Cloud/API services are optional and require valid credentials."
echo "============================================================"

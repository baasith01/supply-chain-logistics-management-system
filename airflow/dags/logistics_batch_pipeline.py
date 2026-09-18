"""
Airflow DAG: Logistics Batch Pipeline
=====================================

Purpose
-------
Orchestrate the batch portion of the Logistics & Supply Chain
Intelligence pipeline.

Flow
----
1. Ingest raw orders
2. Ingest raw customers
3. Ingest raw warehouses
4. Clean orders with Spark
5. Clean GPS data with Spark
6. Clean weather data with Spark
7. Create delivery features with Spark
8. Run downstream dbt transformations

The DAG uses BashOperator so the same scripts can be executed
manually outside Airflow. Paths are resolved from the project root
rather than depending on the Airflow working directory.

Important
---------
- This DAG orchestrates the project's existing scripts; it does not
  duplicate their transformation logic.
- API ingestion and Kafka streaming are separate pipeline paths.
- Database/storage credentials should be supplied through Airflow
  connections/environment variables, not hard-coded here.
- The dbt task assumes dbt is installed and the dbt project is
  available at <project_root>/dbt.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


# -------------------------------------------------------------------
# Project paths
# -------------------------------------------------------------------

PROJECT_ROOT = Path(
    __file__
).resolve().parents[2]

DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"


# -------------------------------------------------------------------
# Default DAG configuration
# -------------------------------------------------------------------

default_args = {
    "owner": "logistics-data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------

with DAG(
    dag_id="logistics_batch_pipeline",
    description=(
        "Batch ingestion, Spark transformation, feature creation, "
        "and dbt analytics pipeline for logistics data."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=[
        "logistics",
        "batch",
        "spark",
        "dbt",
        "analytics",
    ],
) as dag:

    # ---------------------------------------------------------------
    # Batch ingestion
    # ---------------------------------------------------------------

    ingest_orders = BashOperator(
        task_id="ingest_orders",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python ingestion/batch/orders_ingestion.py"
        ),
    )

    ingest_customers = BashOperator(
        task_id="ingest_customers",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python ingestion/batch/customer_ingestion.py"
        ),
    )

    ingest_warehouses = BashOperator(
        task_id="ingest_warehouses",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python ingestion/batch/warehouse_ingestion.py"
        ),
    )

    # ---------------------------------------------------------------
    # Spark transformations
    # ---------------------------------------------------------------

    clean_orders = BashOperator(
        task_id="clean_orders",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit spark/batch/clean_orders.py"
        ),
    )

    clean_gps = BashOperator(
        task_id="clean_gps",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit spark/batch/clean_gps.py"
        ),
    )

    clean_weather = BashOperator(
        task_id="clean_weather",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit spark/batch/clean_weather.py"
        ),
    )

    create_features = BashOperator(
        task_id="create_delivery_features",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit spark/batch/create_features.py"
        ),
    )

    # ---------------------------------------------------------------
    # dbt transformation layer
    # ---------------------------------------------------------------

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {PROJECT_ROOT / 'dbt'} && "
            "dbt run"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {PROJECT_ROOT / 'dbt'} && "
            "dbt test"
        ),
    )

    # ---------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------
    #
    # Independent source ingestion tasks run first.
    # Orders are required before order cleaning.
    # GPS and weather cleaning can run independently once their
    # source data is available.
    #
    # Feature creation waits for all relevant cleaned datasets.
    # dbt runs after the Spark transformation stage and then tests
    # the resulting analytical models.
    # ---------------------------------------------------------------

    [
        ingest_orders,
        ingest_customers,
        ingest_warehouses,
    ]

    ingest_orders >> clean_orders
    ingest_orders >> create_features
    clean_orders >> create_features

    ingest_customers >> create_features
    ingest_warehouses >> create_features

    clean_gps >> create_features
    clean_weather >> create_features

    # The GPS and weather cleaning tasks depend on their respective
    # raw datasets being available. They are separated from the
    # ingestion tasks because those datasets may arrive through
    # other project ingestion paths as well.
    #
    # For the standard batch execution, explicit upstream checks
    # can be added when those ingestion jobs are scheduled.
    #
    # Current transformation chain:
    create_features >> dbt_run >> dbt_test

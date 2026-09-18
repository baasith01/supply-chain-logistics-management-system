"""
Airflow DAG: Realtime Pipeline
==============================

Purpose
-------
Orchestrate the project's near-real-time logistics processing path.

Flow
----
1. Start/validate the GPS Kafka stream
2. Start/validate the order Kafka stream
3. Run Spark Structured Streaming for GPS data
4. Run Spark Structured Streaming for order data
5. Run the realtime delivery operational stream

Important
---------
- Kafka producers are long-running processes when configured for
  continuous streaming. Airflow is therefore used here primarily
  to launch and supervise bounded/controlled streaming jobs.
- The DAG uses configurable execution timeouts so a task does not
  remain active indefinitely.
- This project simulates real-time activity using synthetic data;
  production deployments would use managed Kafka/streaming services.
- Credentials and infrastructure endpoints belong in environment
  variables or Airflow connections, not in this DAG.
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


# -------------------------------------------------------------------
# Default configuration
# -------------------------------------------------------------------

default_args = {
    "owner": "logistics-data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------

with DAG(
    dag_id="realtime_pipeline",
    description=(
        "Kafka and Spark Structured Streaming orchestration for "
        "near-real-time logistics data."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=14),
    tags=[
        "logistics",
        "realtime",
        "kafka",
        "spark",
        "streaming",
    ],
) as dag:

    # ---------------------------------------------------------------
    # Kafka producers
    # ---------------------------------------------------------------
    #
    # These producer tasks publish a bounded batch of synthetic
    # events during each DAG run. The --max-records arguments keep
    # Airflow tasks finite and observable.
    #
    # Continuous production can still be run independently with
    # the same producer scripts when required.
    # ---------------------------------------------------------------

    publish_gps_events = BashOperator(
        task_id="publish_gps_events",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python kafka/producers/gps_producer.py "
            "--max-records 100"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    publish_order_events = BashOperator(
        task_id="publish_order_events",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python kafka/producers/order_producer.py "
            "--max-records 50"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # ---------------------------------------------------------------
    # Spark streaming consumers
    # ---------------------------------------------------------------

    gps_stream = BashOperator(
        task_id="gps_stream",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit spark/streaming/gps_stream.py "
            "--once"
        ),
        execution_timeout=timedelta(minutes=8),
    )

    order_stream = BashOperator(
        task_id="order_stream",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit spark/streaming/order_stream.py "
            "--once"
        ),
        execution_timeout=timedelta(minutes=8),
    )

    # ---------------------------------------------------------------
    # Realtime operational processing
    # ---------------------------------------------------------------

    realtime_delivery = BashOperator(
        task_id="realtime_delivery",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit spark/streaming/realtime_delivery.py "
            "--once"
        ),
        execution_timeout=timedelta(minutes=8),
    )

    # ---------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------
    #
    # GPS events -> GPS Spark stream
    # Order events -> Order Spark stream
    # Both streams -> realtime delivery processing
    # ---------------------------------------------------------------

    publish_gps_events >> gps_stream
    publish_order_events >> order_stream

    [gps_stream, order_stream] >> realtime_delivery

"""
Airflow DAG: Daily Analytics Pipeline
=====================================

Purpose
-------
Run the daily analytics and KPI layer after the core batch
transformation pipeline has produced clean/processed data.

Flow
----
1. Run Spark delivery metrics
2. Run Spark driver metrics
3. Run Spark warehouse metrics
4. Run Spark route metrics
5. Run Python EDA
6. Run feature engineering
7. Run demand forecasting
8. Run delivery-delay prediction
9. Run route analysis

This DAG is intentionally separate from the batch ingestion DAG:
- logistics_batch_pipeline.py handles ingestion and core cleaning.
- daily_analytics_pipeline.py handles recurring analytics/ML jobs.

Important
---------
- Analytics jobs consume the processed datasets created by the
  upstream pipeline.
- ML outputs are analytical predictions and should not be treated
  as production decisions without validation.
- Credentials and external service configuration belong in
  environment variables or Airflow connections.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


# -------------------------------------------------------------------
# Project path
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------

with DAG(
    dag_id="daily_analytics_pipeline",
    description=(
        "Daily logistics KPI, exploratory analytics, feature "
        "engineering, forecasting, prediction, and route analysis."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=[
        "logistics",
        "analytics",
        "ml",
        "daily",
    ],
) as dag:

    # ---------------------------------------------------------------
    # Operational KPI jobs
    # ---------------------------------------------------------------

    delivery_metrics = BashOperator(
        task_id="delivery_metrics",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python spark/jobs/delivery_metrics.py"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    driver_metrics = BashOperator(
        task_id="driver_metrics",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python spark/jobs/driver_metrics.py"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    warehouse_metrics = BashOperator(
        task_id="warehouse_metrics",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python spark/jobs/warehouse_metrics.py"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    route_metrics = BashOperator(
        task_id="route_metrics",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python spark/jobs/route_metrics.py"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    # ---------------------------------------------------------------
    # Analytical / ML jobs
    # ---------------------------------------------------------------

    eda = BashOperator(
        task_id="eda",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python analytics/python/eda.py"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    feature_engineering = BashOperator(
        task_id="feature_engineering",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python analytics/python/feature_engineering.py"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    demand_forecasting = BashOperator(
        task_id="demand_forecasting",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python analytics/python/demand_forecasting.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    delivery_prediction = BashOperator(
        task_id="delivery_prediction",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python analytics/python/delivery_prediction.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    route_analysis = BashOperator(
        task_id="route_analysis",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python analytics/python/route_analysis.py"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    # ---------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------
    #
    # KPI jobs can run independently after the upstream batch
    # pipeline has produced processed data.
    #
    # EDA and feature engineering are the preparation stage for
    # analytical/ML work.
    #
    # Forecasting and delivery prediction depend on features.
    # Route analysis depends on route metrics.
    # ---------------------------------------------------------------

    [
        delivery_metrics,
        driver_metrics,
        warehouse_metrics,
        route_metrics,
    ] >> eda

    eda >> feature_engineering

    feature_engineering >> [
        demand_forecasting,
        delivery_prediction,
    ]

    route_metrics >> route_analysis

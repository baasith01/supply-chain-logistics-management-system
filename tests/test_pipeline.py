"""
End-to-end pipeline contract tests.

Run:
    pytest tests/test_pipeline.py -v

These tests verify that the major project stages are present, executable at
the file level, and connected through expected inputs/outputs. They do not
require Kafka, HDFS, S3, Spark, Airflow, dbt, or Power BI services to be
running.

The goal is to catch broken project structure and obvious integration
regressions before attempting a full local deployment.
"""

from pathlib import Path
import ast
import csv
import re

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


EXPECTED_FILES = [
    "README.md",
    "requirements.txt",
    "docker-compose.yml",
    ".env.example",
    "data/raw/orders.csv",
    "data/raw/customers.csv",
    "data/raw/drivers.csv",
    "data/raw/vehicles.csv",
    "data/raw/warehouses.csv",
    "data/raw/gps_tracking.csv",
    "data/raw/weather.csv",
    "data/raw/traffic.csv",
    "data/raw/roads.csv",
    "data/raw/delivery_updates.csv",
    "data/processed/orders_clean.csv",
    "data/processed/gps_clean.csv",
    "data/processed/weather_clean.csv",
    "data/processed/delivery_features.csv",
    "ingestion/api/weather_api.py",
    "ingestion/api/traffic_api.py",
    "ingestion/api/maps_api.py",
    "ingestion/batch/orders_ingestion.py",
    "ingestion/batch/customer_ingestion.py",
    "ingestion/batch/warehouse_ingestion.py",
    "ingestion/streaming/gps_producer.py",
    "ingestion/streaming/order_producer.py",
    "ingestion/streaming/kafka_config.py",
    "kafka/topics.md",
    "kafka/producers/gps_producer.py",
    "kafka/producers/order_producer.py",
    "kafka/consumers/gps_consumer.py",
    "kafka/consumers/order_consumer.py",
    "storage/hdfs/upload_to_hdfs.sh",
    "storage/hdfs/hdfs_structure.md",
    "storage/s3/upload_to_s3.py",
    "hive/databases/logistics_db.sql",
    "hive/tables/orders.sql",
    "hive/tables/customers.sql",
    "hive/tables/drivers.sql",
    "hive/tables/vehicles.sql",
    "hive/tables/warehouses.sql",
    "hive/tables/gps_tracking.sql",
    "hive/tables/weather.sql",
    "hive/tables/traffic.sql",
    "spark/batch/clean_orders.py",
    "spark/batch/clean_gps.py",
    "spark/batch/clean_weather.py",
    "spark/batch/create_features.py",
    "spark/streaming/gps_stream.py",
    "spark/streaming/order_stream.py",
    "spark/streaming/realtime_delivery.py",
    "spark/jobs/delivery_metrics.py",
    "spark/jobs/driver_metrics.py",
    "spark/jobs/warehouse_metrics.py",
    "spark/jobs/route_metrics.py",
    "warehouse/schema.sql",
    "warehouse/dimensions/dim_customer.sql",
    "warehouse/dimensions/dim_driver.sql",
    "warehouse/dimensions/dim_vehicle.sql",
    "warehouse/dimensions/dim_warehouse.sql",
    "warehouse/dimensions/dim_location.sql",
    "warehouse/dimensions/dim_date.sql",
    "warehouse/facts/fact_orders.sql",
    "warehouse/facts/fact_delivery.sql",
    "warehouse/facts/fact_gps.sql",
    "warehouse/facts/fact_traffic.sql",
    "dbt/dbt_project.yml",
    "dbt/models/staging/stg_orders.sql",
    "dbt/models/staging/stg_gps.sql",
    "dbt/models/staging/stg_weather.sql",
    "dbt/models/staging/stg_traffic.sql",
    "dbt/models/intermediate/int_delivery.sql",
    "dbt/models/intermediate/int_routes.sql",
    "dbt/models/intermediate/int_driver_performance.sql",
    "dbt/models/marts/delivery_mart.sql",
    "dbt/models/marts/logistics_mart.sql",
    "dbt/models/marts/warehouse_mart.sql",
    "dbt/models/marts/executive_mart.sql",
    "dbt/tests/unique_orders.sql",
    "dbt/tests/valid_delivery_status.sql",
    "airflow/dags/logistics_batch_pipeline.py",
    "airflow/dags/realtime_pipeline.py",
    "airflow/dags/daily_analytics_pipeline.py",
    "analytics/sql/kpi_analysis.sql",
    "analytics/sql/delivery_analysis.sql",
    "analytics/sql/customer_analysis.sql",
    "analytics/sql/driver_analysis.sql",
    "analytics/sql/warehouse_analysis.sql",
    "analytics/python/eda.py",
    "analytics/python/feature_engineering.py",
    "analytics/python/demand_forecasting.py",
    "analytics/python/delivery_prediction.py",
    "analytics/python/route_analysis.py",
    "dashboards/powerbi/dashboard_documentation.md",
    "monitoring/prometheus/prometheus.yml",
    "monitoring/grafana/dashboard.json",
    "tests/test_ingestion.py",
    "tests/test_transformations.py",
    "tests/test_data_quality.py",
]


PYTHON_FILES = [
    "ingestion/api/weather_api.py",
    "ingestion/api/traffic_api.py",
    "ingestion/api/maps_api.py",
    "ingestion/batch/orders_ingestion.py",
    "ingestion/batch/customer_ingestion.py",
    "ingestion/batch/warehouse_ingestion.py",
    "ingestion/streaming/gps_producer.py",
    "ingestion/streaming/order_producer.py",
    "ingestion/streaming/kafka_config.py",
    "kafka/producers/gps_producer.py",
    "kafka/producers/order_producer.py",
    "kafka/consumers/gps_consumer.py",
    "kafka/consumers/order_consumer.py",
    "storage/s3/upload_to_s3.py",
    "spark/batch/clean_orders.py",
    "spark/batch/clean_gps.py",
    "spark/batch/clean_weather.py",
    "spark/batch/create_features.py",
    "spark/streaming/gps_stream.py",
    "spark/streaming/order_stream.py",
    "spark/streaming/realtime_delivery.py",
    "spark/jobs/delivery_metrics.py",
    "spark/jobs/driver_metrics.py",
    "spark/jobs/warehouse_metrics.py",
    "spark/jobs/route_metrics.py",
    "airflow/dags/logistics_batch_pipeline.py",
    "airflow/dags/realtime_pipeline.py",
    "airflow/dags/daily_analytics_pipeline.py",
    "analytics/python/eda.py",
    "analytics/python/feature_engineering.py",
    "analytics/python/demand_forecasting.py",
    "analytics/python/delivery_prediction.py",
    "analytics/python/route_analysis.py",
    "tests/test_ingestion.py",
    "tests/test_transformations.py",
    "tests/test_data_quality.py",
]


def project_path(relative_path):
    return PROJECT_ROOT / relative_path


def read_text(relative_path):
    path = project_path(relative_path)
    if not path.exists():
        pytest.fail(f"Required project file is missing: {path}")
    return path.read_text(encoding="utf-8")


def test_project_structure_is_present():
    """All major frozen-tree files except generated PBIX/screenshots exist."""
    missing = [
        relative_path
        for relative_path in EXPECTED_FILES
        if not project_path(relative_path).is_file()
    ]

    assert not missing, (
        "Missing expected project files:\n"
        + "\n".join(f"- {item}" for item in missing)
    )


@pytest.mark.parametrize("relative_path", PYTHON_FILES)
def test_python_files_parse(relative_path):
    """Every Python module should at least be syntactically valid."""
    source = read_text(relative_path)

    try:
        ast.parse(source, filename=relative_path)
    except SyntaxError as exc:
        pytest.fail(
            f"{relative_path} contains a Python syntax error: {exc}"
        )


@pytest.mark.parametrize(
    "relative_path",
    [
        "hive/databases/logistics_db.sql",
        "hive/tables/orders.sql",
        "hive/tables/customers.sql",
        "hive/tables/drivers.sql",
        "hive/tables/vehicles.sql",
        "hive/tables/warehouses.sql",
        "hive/tables/gps_tracking.sql",
        "hive/tables/weather.sql",
        "hive/tables/traffic.sql",
        "warehouse/schema.sql",
        "warehouse/dimensions/dim_customer.sql",
        "warehouse/dimensions/dim_driver.sql",
        "warehouse/dimensions/dim_vehicle.sql",
        "warehouse/dimensions/dim_warehouse.sql",
        "warehouse/dimensions/dim_location.sql",
        "warehouse/dimensions/dim_date.sql",
        "warehouse/facts/fact_orders.sql",
        "warehouse/facts/fact_delivery.sql",
        "warehouse/facts/fact_gps.sql",
        "warehouse/facts/fact_traffic.sql",
        "dbt/models/staging/stg_orders.sql",
        "dbt/models/staging/stg_gps.sql",
        "dbt/models/staging/stg_weather.sql",
        "dbt/models/staging/stg_traffic.sql",
        "dbt/models/intermediate/int_delivery.sql",
        "dbt/models/intermediate/int_routes.sql",
        "dbt/models/intermediate/int_driver_performance.sql",
        "dbt/models/marts/delivery_mart.sql",
        "dbt/models/marts/logistics_mart.sql",
        "dbt/models/marts/warehouse_mart.sql",
        "dbt/models/marts/executive_mart.sql",
    ],
)
def test_sql_files_are_non_empty(relative_path):
    """Core SQL assets should contain actual SQL."""
    text = read_text(relative_path).strip()

    assert len(text) > 20
    assert re.search(
        r"\b(SELECT|CREATE|INSERT|WITH|DROP|ALTER)\b",
        text,
        flags=re.IGNORECASE,
    ), f"{relative_path} does not appear to contain SQL"


def test_raw_to_processed_pipeline_contract():
    """Core cleaning outputs must exist after the transformation stage."""
    required_outputs = [
        "data/processed/orders_clean.csv",
        "data/processed/gps_clean.csv",
        "data/processed/weather_clean.csv",
        "data/processed/delivery_features.csv",
    ]

    for relative_path in required_outputs:
        path = project_path(relative_path)
        assert path.is_file(), f"Missing transformation output: {relative_path}"
        assert path.stat().st_size > 0, (
            f"Transformation output is empty: {relative_path}"
        )


def test_orders_pipeline_preserves_order_population():
    """The standard cleaning/features pipeline should preserve source orders."""
    raw_path = project_path("data/raw/orders.csv")
    clean_path = project_path("data/processed/orders_clean.csv")
    features_path = project_path("data/processed/delivery_features.csv")

    with raw_path.open(newline="", encoding="utf-8") as handle:
        raw_reader = csv.DictReader(handle)
        raw_ids = {row["order_id"] for row in raw_reader}

    with clean_path.open(newline="", encoding="utf-8") as handle:
        clean_reader = csv.DictReader(handle)
        clean_ids = {row["order_id"] for row in clean_reader}

    with features_path.open(newline="", encoding="utf-8") as handle:
        feature_reader = csv.DictReader(handle)
        feature_ids = {row["order_id"] for row in feature_reader}

    assert clean_ids == raw_ids
    assert feature_ids == raw_ids


def test_kafka_topics_are_documented():
    """The streaming layer should document all five intended topics."""
    text = read_text("kafka/topics.md").lower()

    topics = [
        "orders",
        "gps_tracking",
        "delivery_updates",
        "traffic_updates",
        "weather_updates",
    ]

    missing = [topic for topic in topics if topic not in text]

    assert not missing, f"Kafka topic documentation is missing: {missing}"


def test_prometheus_configuration_contains_core_targets():
    """Monitoring configuration should expose the core scrape jobs."""
    text = read_text("monitoring/prometheus/prometheus.yml")

    assert "job_name: prometheus" in text
    assert "job_name: windows_exporter" in text
    assert "host.docker.internal:9182" in text
    assert "scrape_interval: 15s" in text


def test_grafana_dashboard_is_valid_json():
    """The Grafana dashboard artifact should be valid JSON."""
    import json

    text = read_text("monitoring/grafana/dashboard.json")
    dashboard = json.loads(text)

    assert dashboard["title"]
    assert dashboard["uid"]
    assert isinstance(dashboard["panels"], list)
    assert len(dashboard["panels"]) > 0


def test_dbt_project_has_expected_models():
    """dbt project should contain staging, intermediate and mart layers."""
    text = read_text("dbt/dbt_project.yml")

    for token in ["staging", "intermediate", "marts"]:
        assert token in text


def test_airflow_dags_have_dag_definitions():
    """Each scheduled pipeline file should define an Airflow DAG."""
    dag_files = [
        "airflow/dags/logistics_batch_pipeline.py",
        "airflow/dags/realtime_pipeline.py",
        "airflow/dags/daily_analytics_pipeline.py",
    ]

    for relative_path in dag_files:
        text = read_text(relative_path)

        assert re.search(
            r"\bDAG\b",
            text,
        ), f"{relative_path} does not appear to define an Airflow DAG"


def test_analytics_layer_contains_required_capabilities():
    """Analytics layer should include EDA, forecasting, prediction and routing."""
    required = {
        "analytics/python/eda.py",
        "analytics/python/feature_engineering.py",
        "analytics/python/demand_forecasting.py",
        "analytics/python/delivery_prediction.py",
        "analytics/python/route_analysis.py",
    }

    for relative_path in required:
        path = project_path(relative_path)
        assert path.is_file()
        assert path.stat().st_size > 1000


def test_ml_scripts_contain_expected_modeling_components():
    """Baseline ML scripts should reference their intended estimators."""
    demand = read_text("analytics/python/demand_forecasting.py")
    delivery = read_text("analytics/python/delivery_prediction.py")

    assert "LinearRegression" in demand
    assert "LogisticRegression" in delivery
    assert "train_test_split" in delivery or "chronological" in delivery.lower()


def test_project_has_documented_powerbi_layer():
    """Power BI documentation should exist even though PBIX is desktop-generated."""
    text = read_text("dashboards/powerbi/dashboard_documentation.md").lower()

    for required_phrase in [
        "executive overview",
        "delivery operations",
        "fleet intelligence",
        "warehouse intelligence",
        "financial analysis",
        "dax",
    ]:
        assert required_phrase in text


def test_no_hardcoded_aws_credentials_in_s3_uploader():
    """S3 ingestion must rely on the AWS credential chain/environment."""
    text = read_text("storage/s3/upload_to_s3.py")

    forbidden_patterns = [
        r"aws_access_key_id\s*=",
        r"aws_secret_access_key\s*=",
        r"AKIA[0-9A-Z]{16}",
    ]

    for pattern in forbidden_patterns:
        assert not re.search(
            pattern,
            text,
            flags=re.IGNORECASE,
        ), f"Potential hardcoded AWS credential found: {pattern}"


def test_environment_template_contains_core_configuration():
    """The example environment file should document major integrations."""
    text = read_text(".env.example").upper()

    required_tokens = [
        "KAFKA",
        "AWS",
        "WEATHER",
        "AIRFLOW",
        "SPARK",
    ]

    for token in required_tokens:
        assert token in text


def test_requirements_contains_core_project_dependencies():
    """Dependency file should cover the major Python integrations."""
    text = read_text("requirements.txt").lower()

    required_packages = [
        "pandas",
        "numpy",
        "requests",
        "pyspark",
        "kafka-python",
        "scikit-learn",
        "dbt-core",
        "pytest",
    ]

    missing = [
        package
        for package in required_packages
        if package not in text
    ]

    assert not missing, f"requirements.txt is missing: {missing}"


def test_docker_compose_contains_core_infrastructure():
    """Local orchestration should define the core monitoring/data services."""
    text = read_text("docker-compose.yml").lower()

    for service in ["kafka", "postgres", "prometheus", "grafana"]:
        assert service in text


def test_shell_scripts_have_sensible_entry_points():
    """HDFS/setup scripts should contain executable shell commands."""
    scripts = [
        "storage/hdfs/upload_to_hdfs.sh",
        "scripts/setup.sh",
        "scripts/run_pipeline.sh",
        "scripts/cleanup.sh",
    ]

    for relative_path in scripts:
        path = project_path(relative_path)

        # The frozen tree may be assembled incrementally, so report a clear
        # failure if a script is missing rather than silently skipping it.
        assert path.is_file(), f"Missing shell script: {relative_path}"

        text = path.read_text(encoding="utf-8")

        assert "#!" in text or "bash" in text.lower()
        assert len(text.strip()) > 30


def test_pipeline_has_clear_stage_documentation():
    """README should describe the project's main end-to-end flow."""
    text = read_text("README.md").lower()

    stages = [
        "ingestion",
        "kafka",
        "spark",
        "hive",
        "dbt",
        "airflow",
        "power bi",
        "prometheus",
        "grafana",
    ]

    missing = [stage for stage in stages if stage not in text]

    assert not missing, (
        f"README is missing pipeline-stage documentation: {missing}"
    )

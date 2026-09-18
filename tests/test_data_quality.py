"""
Data-quality tests for the Logistics Supply Chain Intelligence project.

Run:
    pytest tests/test_data_quality.py -v

These checks are intentionally independent of Spark, Kafka, Airflow, dbt,
and external APIs. They validate the quality of the project's CSV data
contracts before downstream analytics and ML consume them.
"""

from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def read_csv(directory, filename):
    path = directory / filename

    if not path.exists():
        pytest.fail(f"Missing required dataset: {path}")

    return pd.read_csv(path)


@pytest.fixture(scope="session")
def raw():
    return {
        "orders": read_csv(RAW_DIR, "orders.csv"),
        "customers": read_csv(RAW_DIR, "customers.csv"),
        "drivers": read_csv(RAW_DIR, "drivers.csv"),
        "vehicles": read_csv(RAW_DIR, "vehicles.csv"),
        "warehouses": read_csv(RAW_DIR, "warehouses.csv"),
        "gps": read_csv(RAW_DIR, "gps_tracking.csv"),
        "weather": read_csv(RAW_DIR, "weather.csv"),
        "traffic": read_csv(RAW_DIR, "traffic.csv"),
        "roads": read_csv(RAW_DIR, "roads.csv"),
        "delivery_updates": read_csv(RAW_DIR, "delivery_updates.csv"),
    }


@pytest.fixture(scope="session")
def processed():
    return {
        "orders": read_csv(PROCESSED_DIR, "orders_clean.csv"),
        "gps": read_csv(PROCESSED_DIR, "gps_clean.csv"),
        "weather": read_csv(PROCESSED_DIR, "weather_clean.csv"),
        "delivery_features": read_csv(
            PROCESSED_DIR, "delivery_features.csv"
        ),
    }


def assert_no_blank_values(df, columns, dataset_name):
    for column in columns:
        assert column in df.columns, (
            f"{dataset_name}: missing required column {column}"
        )

        values = df[column]

        assert values.notna().all(), (
            f"{dataset_name}: null values found in {column}"
        )

        if values.dtype == "object":
            assert values.astype(str).str.strip().ne("").all(), (
                f"{dataset_name}: blank values found in {column}"
            )


def test_required_identifier_fields_have_no_nulls(raw):
    """Critical entity identifiers must always be populated."""
    checks = {
        "orders": ["order_id", "customer_id", "warehouse_id", "driver_id", "vehicle_id"],
        "customers": ["customer_id"],
        "drivers": ["driver_id"],
        "vehicles": ["vehicle_id", "driver_id"],
        "warehouses": ["warehouse_id"],
        "delivery_updates": [
            "event_id",
            "order_id",
            "driver_id",
            "vehicle_id",
        ],
    }

    for dataset_name, columns in checks.items():
        assert_no_blank_values(raw[dataset_name], columns, dataset_name)


def test_primary_keys_are_unique(raw):
    """Entity and event tables should not contain duplicate primary keys."""
    keys = {
        "orders": "order_id",
        "customers": "customer_id",
        "drivers": "driver_id",
        "vehicles": "vehicle_id",
        "warehouses": "warehouse_id",
        "roads": "road_id",
        "delivery_updates": "event_id",
    }

    for dataset_name, key in keys.items():
        df = raw[dataset_name]
        assert df[key].is_unique, (
            f"{dataset_name}: duplicate primary key values in {key}"
        )


def test_order_numeric_ranges(raw):
    """Order financial and operational measurements must be sensible."""
    df = raw["orders"]

    for column in [
        "order_value",
        "delivery_distance_km",
        "promised_delivery_time_min",
        "actual_delivery_time_min",
    ]:
        values = pd.to_numeric(df[column], errors="coerce")

        assert values.notna().all(), (
            f"orders: non-numeric values found in {column}"
        )
        assert (values >= 0).all(), (
            f"orders: negative values found in {column}"
        )


def test_customer_type_is_not_blank(raw):
    assert_no_blank_values(
        raw["customers"],
        ["customer_type", "region"],
        "customers",
    )


def test_driver_rating_and_experience_are_valid(raw):
    df = raw["drivers"]

    rating = pd.to_numeric(df["rating"], errors="coerce")
    experience = pd.to_numeric(df["experience_years"], errors="coerce")

    assert rating.notna().all()
    assert rating.between(0, 5).all(), "Driver rating must be between 0 and 5"

    assert experience.notna().all()
    assert (experience >= 0).all(), (
        "Driver experience cannot be negative"
    )


def test_vehicle_capacity_and_odometer_are_valid(raw):
    df = raw["vehicles"]

    capacity = pd.to_numeric(df["capacity_kg"], errors="coerce")
    odometer = pd.to_numeric(df["odometer_km"], errors="coerce")
    year = pd.to_numeric(df["manufacture_year"], errors="coerce")

    assert capacity.notna().all()
    assert (capacity > 0).all()

    assert odometer.notna().all()
    assert (odometer >= 0).all()

    assert year.notna().all()
    assert year.between(1900, 2100).all()


def test_warehouse_coordinates_and_capacity_are_valid(raw):
    df = raw["warehouses"]

    latitude = pd.to_numeric(df["latitude"], errors="coerce")
    longitude = pd.to_numeric(df["longitude"], errors="coerce")
    capacity = pd.to_numeric(df["capacity_units"], errors="coerce")

    assert latitude.notna().all()
    assert longitude.notna().all()
    assert latitude.between(-90, 90).all()
    assert longitude.between(-180, 180).all()

    assert capacity.notna().all()
    assert (capacity > 0).all()


def test_gps_quality_ranges(raw):
    df = raw["gps"]

    latitude = pd.to_numeric(df["latitude"], errors="coerce")
    longitude = pd.to_numeric(df["longitude"], errors="coerce")
    speed = pd.to_numeric(df["speed_kmph"], errors="coerce")
    heading = pd.to_numeric(df["heading"], errors="coerce")
    signal = pd.to_numeric(df["signal_quality"], errors="coerce")

    assert latitude.notna().all()
    assert longitude.notna().all()
    assert latitude.between(-90, 90).all()
    assert longitude.between(-180, 180).all()

    assert speed.notna().all()
    assert (speed >= 0).all()

    assert heading.notna().all()
    assert heading.between(0, 360).all()

    assert signal.notna().all()
    assert signal.between(0, 100).all()


def test_weather_quality_ranges(raw):
    df = raw["weather"]

    latitude = pd.to_numeric(df["latitude"], errors="coerce")
    longitude = pd.to_numeric(df["longitude"], errors="coerce")
    humidity = pd.to_numeric(df["humidity_pct"], errors="coerce")
    rainfall = pd.to_numeric(df["rainfall_mm"], errors="coerce")

    assert latitude.notna().all()
    assert longitude.notna().all()
    assert latitude.between(-90, 90).all()
    assert longitude.between(-180, 180).all()

    assert humidity.notna().all()
    assert humidity.between(0, 100).all()

    assert rainfall.notna().all()
    assert (rainfall >= 0).all()


def test_traffic_quality_ranges(raw):
    df = raw["traffic"]

    score = pd.to_numeric(df["traffic_score"], errors="coerce")
    speed = pd.to_numeric(df["average_speed_kmph"], errors="coerce")

    assert score.notna().all()
    assert score.between(0, 100).all()

    assert speed.notna().all()
    assert (speed >= 0).all()


def test_road_quality_ranges(raw):
    df = raw["roads"]

    distance = pd.to_numeric(df["distance_km"], errors="coerce")
    lanes = pd.to_numeric(df["lanes"], errors="coerce")
    speed_limit = pd.to_numeric(df["speed_limit_kmph"], errors="coerce")

    assert distance.notna().all()
    assert (distance > 0).all()

    assert lanes.notna().all()
    assert (lanes > 0).all()

    assert speed_limit.notna().all()
    assert (speed_limit > 0).all()


def test_delivery_update_quality(raw):
    df = raw["delivery_updates"]

    delay = pd.to_numeric(df["delay_minutes"], errors="coerce")
    timestamps = pd.to_datetime(
        df["event_timestamp"],
        errors="coerce",
    )

    assert delay.notna().all()
    assert (delay >= 0).all()

    assert timestamps.notna().all()


def test_orders_reference_master_data(raw):
    """Check referential integrity across the main order entities."""
    orders = raw["orders"]

    references = {
        "customer_id": raw["customers"]["customer_id"],
        "warehouse_id": raw["warehouses"]["warehouse_id"],
        "driver_id": raw["drivers"]["driver_id"],
        "vehicle_id": raw["vehicles"]["vehicle_id"],
    }

    for column, master_ids in references.items():
        known = set(master_ids.astype(str))
        referenced = set(orders[column].astype(str))

        assert referenced.issubset(known), (
            f"orders: unknown {column} references found"
        )


def test_delivery_updates_reference_orders_and_fleet(raw):
    """Delivery events should point to known operational entities."""
    updates = raw["delivery_updates"]

    references = {
        "order_id": raw["orders"]["order_id"],
        "driver_id": raw["drivers"]["driver_id"],
        "vehicle_id": raw["vehicles"]["vehicle_id"],
    }

    for column, master_ids in references.items():
        known = set(master_ids.astype(str))
        referenced = set(updates[column].astype(str))

        assert referenced.issubset(known), (
            f"delivery_updates: unknown {column} references found"
        )


def test_processed_orders_preserve_order_count(raw, processed):
    """Cleaning should not silently drop orders in the standard pipeline."""
    assert len(processed["orders"]) == len(raw["orders"])


def test_processed_features_preserve_order_count(raw, processed):
    """Feature engineering is expected to remain one row per source order."""
    assert len(processed["delivery_features"]) == len(raw["orders"])


def test_processed_order_ids_match_source(raw, processed):
    """The processed order population should match the raw source population."""
    raw_ids = set(raw["orders"]["order_id"].astype(str))
    clean_ids = set(processed["orders"]["order_id"].astype(str))

    assert clean_ids == raw_ids


def test_processed_feature_ids_match_source(raw, processed):
    """ML features should cover the same order population as the source."""
    raw_ids = set(raw["orders"]["order_id"].astype(str))
    feature_ids = set(
        processed["delivery_features"]["order_id"].astype(str)
    )

    assert feature_ids == raw_ids


def test_processed_outputs_do_not_contain_duplicate_orders(processed):
    """Downstream order-level datasets must remain unique by order_id."""
    for dataset_name in ["orders", "delivery_features"]:
        df = processed[dataset_name]

        assert df["order_id"].notna().all()
        assert df["order_id"].is_unique, (
            f"{dataset_name}: duplicate order_id values"
        )


def test_processed_risk_scores_are_valid(processed):
    """Risk scores should remain in the documented 0-100 range."""
    df = processed["delivery_features"]

    for column in ["weather_risk", "delivery_risk"]:
        values = pd.to_numeric(df[column], errors="coerce")

        assert values.notna().all(), (
            f"delivery_features: invalid {column} values"
        )
        assert values.between(0, 100).all(), (
            f"delivery_features: {column} outside 0-100 range"
        )


def test_processed_delivery_flags_are_binary(processed):
    """Binary operational flags should contain only 0 and 1."""
    checks = {
        "orders": ["is_delayed"],
        "gps": ["is_moving", "is_low_signal"],
        "weather": [
            "is_rainy",
            "heavy_rain_flag",
            "high_humidity_flag",
        ],
        "delivery_features": ["is_delayed"],
    }

    for dataset_name, columns in checks.items():
        df = processed[dataset_name]

        for column in columns:
            values = pd.to_numeric(df[column], errors="coerce")

            assert values.notna().all(), (
                f"{dataset_name}: invalid values in {column}"
            )
            assert values.isin([0, 1]).all(), (
                f"{dataset_name}: {column} must contain only 0/1"
            )


def test_processed_numeric_columns_contain_no_negative_values(processed):
    """Derived measurements that cannot be negative must remain non-negative."""
    checks = {
        "orders": [
            "order_value",
            "delivery_distance_km",
            "delivery_delay_min",
            "delivery_per_km_value",
        ],
        "gps": ["speed_kmph"],
        "weather": ["rainfall_mm"],
    }

    for dataset_name, columns in checks.items():
        df = processed[dataset_name]

        for column in columns:
            values = pd.to_numeric(df[column], errors="coerce")

            assert values.notna().all(), (
                f"{dataset_name}: invalid numeric values in {column}"
            )
            assert (values >= 0).all(), (
                f"{dataset_name}: negative values in {column}"
            )


def test_processed_datasets_have_no_completely_empty_columns(processed):
    """A transformation must not produce columns containing only nulls."""
    for dataset_name, df in processed.items():
        empty_columns = [
            column for column in df.columns if df[column].isna().all()
        ]

        assert not empty_columns, (
            f"{dataset_name}: completely empty columns: {empty_columns}"
        )

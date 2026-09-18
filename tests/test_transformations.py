"""
Tests for transformation outputs in the Logistics Supply Chain Intelligence
project.

Run:
    pytest tests/test_transformations.py -v

The tests validate the project's processed CSV contracts without requiring
Spark to be installed or a Spark cluster to be running. They focus on:
- processed files existing
- expected transformation columns
- row-level calculations
- boolean/flag consistency
- sensible ranges
- join completeness
- no duplicate order-level feature rows
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def load_processed(filename):
    path = PROCESSED_DIR / filename
    if not path.exists():
        pytest.fail(
            f"Required processed dataset is missing: {path}. "
            "Run the transformation pipeline first."
        )
    return pd.read_csv(path)


@pytest.fixture(scope="session")
def orders_clean():
    return load_processed("orders_clean.csv")


@pytest.fixture(scope="session")
def gps_clean():
    return load_processed("gps_clean.csv")


@pytest.fixture(scope="session")
def weather_clean():
    return load_processed("weather_clean.csv")


@pytest.fixture(scope="session")
def delivery_features():
    return load_processed("delivery_features.csv")


def test_orders_clean_has_expected_columns(orders_clean):
    expected = {
        "order_id",
        "customer_id",
        "warehouse_id",
        "driver_id",
        "vehicle_id",
        "order_date",
        "region",
        "order_status",
        "delivery_status",
        "order_value",
        "delivery_distance_km",
        "promised_delivery_time_min",
        "actual_delivery_time_min",
        "delivery_delay_min",
        "is_delayed",
        "delivery_per_km_value",
    }

    assert expected.issubset(set(orders_clean.columns))


def test_orders_clean_order_id_is_unique(orders_clean):
    assert orders_clean["order_id"].notna().all()
    assert orders_clean["order_id"].is_unique


def test_delivery_delay_is_calculated_correctly(orders_clean):
    promised = pd.to_numeric(
        orders_clean["promised_delivery_time_min"], errors="coerce"
    )
    actual = pd.to_numeric(
        orders_clean["actual_delivery_time_min"], errors="coerce"
    )
    delay = pd.to_numeric(
        orders_clean["delivery_delay_min"], errors="coerce"
    )

    expected_delay = (actual - promised).clip(lower=0)

    assert np.allclose(
        delay.to_numpy(dtype=float),
        expected_delay.to_numpy(dtype=float),
        equal_nan=False,
    )


def test_is_delayed_matches_delay_value(orders_clean):
    delay = pd.to_numeric(
        orders_clean["delivery_delay_min"], errors="coerce"
    )
    is_delayed = pd.to_numeric(
        orders_clean["is_delayed"], errors="coerce"
    )

    expected = (delay > 0).astype(int)

    assert is_delayed.isin([0, 1]).all()
    assert (is_delayed == expected).all()


def test_delivery_per_km_value_is_calculated_correctly(orders_clean):
    value = pd.to_numeric(orders_clean["order_value"], errors="coerce")
    distance = pd.to_numeric(
        orders_clean["delivery_distance_km"], errors="coerce"
    )
    per_km = pd.to_numeric(
        orders_clean["delivery_per_km_value"], errors="coerce"
    )

    expected = value.div(distance.replace(0, np.nan))

    valid = expected.notna() & per_km.notna()

    assert np.allclose(
        per_km[valid].to_numpy(dtype=float),
        expected[valid].to_numpy(dtype=float),
        rtol=1e-6,
        atol=1e-6,
    )


def test_orders_clean_numeric_fields_are_non_negative(orders_clean):
    numeric_columns = [
        "order_value",
        "delivery_distance_km",
        "promised_delivery_time_min",
        "actual_delivery_time_min",
        "delivery_delay_min",
        "delivery_per_km_value",
    ]

    for column in numeric_columns:
        values = pd.to_numeric(orders_clean[column], errors="coerce")
        assert values.notna().all(), f"Invalid values in {column}"
        assert (values >= 0).all(), f"Negative values in {column}"


def test_gps_clean_has_expected_columns(gps_clean):
    expected = {
        "vehicle_id",
        "timestamp",
        "region",
        "latitude",
        "longitude",
        "speed_kmph",
        "heading",
        "vehicle_status",
        "ignition_status",
        "signal_quality",
        "date",
        "hour",
        "day_of_week",
        "is_moving",
        "is_low_signal",
    }

    assert expected.issubset(set(gps_clean.columns))


def test_gps_clean_coordinates_remain_valid(gps_clean):
    latitude = pd.to_numeric(gps_clean["latitude"], errors="coerce")
    longitude = pd.to_numeric(gps_clean["longitude"], errors="coerce")

    assert latitude.notna().all()
    assert longitude.notna().all()
    assert latitude.between(-90, 90).all()
    assert longitude.between(-180, 180).all()


def test_gps_movement_flag_is_consistent(gps_clean):
    speed = pd.to_numeric(gps_clean["speed_kmph"], errors="coerce")
    is_moving = pd.to_numeric(gps_clean["is_moving"], errors="coerce")

    expected = (speed > 0).astype(int)

    assert is_moving.isin([0, 1]).all()
    assert (is_moving == expected).all()


def test_gps_low_signal_flag_is_binary(gps_clean):
    flag = pd.to_numeric(gps_clean["is_low_signal"], errors="coerce")

    assert flag.notna().all()
    assert flag.isin([0, 1]).all()


def test_gps_temporal_columns_are_consistent(gps_clean):
    timestamps = pd.to_datetime(gps_clean["timestamp"], errors="coerce")

    assert timestamps.notna().all()

    expected_date = timestamps.dt.strftime("%Y-%m-%d")
    expected_hour = timestamps.dt.hour
    expected_day = timestamps.dt.dayofweek

    assert (gps_clean["date"].astype(str) == expected_date).all()
    assert (
        pd.to_numeric(gps_clean["hour"], errors="coerce") == expected_hour
    ).all()
    assert (
        pd.to_numeric(gps_clean["day_of_week"], errors="coerce")
        == expected_day
    ).all()


def test_weather_clean_has_expected_columns(weather_clean):
    expected = {
        "date",
        "location",
        "latitude",
        "longitude",
        "temperature_c",
        "humidity_pct",
        "rainfall_mm",
        "weather_condition",
        "is_rainy",
        "heavy_rain_flag",
        "high_humidity_flag",
        "weather_risk",
    }

    assert expected.issubset(set(weather_clean.columns))


def test_weather_rain_flag_is_consistent(weather_clean):
    rainfall = pd.to_numeric(
        weather_clean["rainfall_mm"], errors="coerce"
    )
    is_rainy = pd.to_numeric(
        weather_clean["is_rainy"], errors="coerce"
    )

    # The transformation treats positive rainfall as rainy.
    expected = (rainfall > 0).astype(int)

    assert is_rainy.isin([0, 1]).all()
    assert (is_rainy == expected).all()


def test_weather_risk_is_in_expected_domain(weather_clean):
    risk = pd.to_numeric(
        weather_clean["weather_risk"], errors="coerce"
    )

    assert risk.notna().all()
    assert risk.between(0, 100).all()


def test_weather_flags_are_binary(weather_clean):
    for column in ["is_rainy", "heavy_rain_flag", "high_humidity_flag"]:
        values = pd.to_numeric(weather_clean[column], errors="coerce")

        assert values.notna().all()
        assert values.isin([0, 1]).all(), (
            f"{column} must contain only 0/1 values"
        )


def test_delivery_features_has_one_row_per_order(delivery_features):
    assert "order_id" in delivery_features.columns
    assert delivery_features["order_id"].notna().all()
    assert delivery_features["order_id"].is_unique


def test_delivery_features_contains_expected_ml_columns(delivery_features):
    expected = {
        "order_id",
        "customer_id",
        "warehouse_id",
        "driver_id",
        "vehicle_id",
        "order_date",
        "region",
        "order_value",
        "delivery_distance_km",
        "promised_delivery_time_min",
        "actual_delivery_time_min",
        "delivery_delay_min",
        "is_delayed",
        "delivery_per_km_value",
        "order_month",
        "order_day",
        "order_day_of_week",
        "is_weekend",
        "delay_ratio",
        "distance_bucket",
        "order_value_bucket",
        "delay_severity",
        "weather_risk",
        "delivery_risk",
    }

    # The project may add additional engineered features, but these core
    # fields must remain available to downstream analytics/ML.
    assert expected.issubset(set(delivery_features.columns))


def test_delivery_features_delay_flag_matches_delay(delivery_features):
    delay = pd.to_numeric(
        delivery_features["delivery_delay_min"], errors="coerce"
    )
    flag = pd.to_numeric(
        delivery_features["is_delayed"], errors="coerce"
    )

    expected = (delay > 0).astype(int)

    assert flag.isin([0, 1]).all()
    assert (flag == expected).all()


def test_delivery_features_delay_ratio_is_non_negative(delivery_features):
    ratio = pd.to_numeric(
        delivery_features["delay_ratio"], errors="coerce"
    )

    assert ratio.notna().all()
    assert (ratio >= 0).all()


def test_delivery_features_risk_fields_are_valid(delivery_features):
    for column in ["weather_risk", "delivery_risk"]:
        values = pd.to_numeric(
            delivery_features[column], errors="coerce"
        )

        assert values.notna().all(), f"Invalid values in {column}"
        assert values.between(0, 100).all(), (
            f"{column} must remain between 0 and 100"
        )


def test_delivery_features_retain_raw_order_count(delivery_features):
    raw_path = RAW_DIR / "orders.csv"

    if not raw_path.exists():
        pytest.skip("Raw orders.csv is unavailable for row-count comparison.")

    raw = pd.read_csv(raw_path)

    assert len(delivery_features) == len(raw)


def test_transformation_outputs_have_no_completely_empty_columns(
    orders_clean,
    gps_clean,
    weather_clean,
    delivery_features,
):
    datasets = {
        "orders_clean": orders_clean,
        "gps_clean": gps_clean,
        "weather_clean": weather_clean,
        "delivery_features": delivery_features,
    }

    for name, df in datasets.items():
        empty_columns = [
            column for column in df.columns if df[column].isna().all()
        ]

        assert not empty_columns, (
            f"{name} contains completely empty columns: {empty_columns}"
        )

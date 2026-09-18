"""
Tests for the ingestion layer.

Run:
    pytest tests/test_ingestion.py -v

These tests focus on ingestion contracts:
- expected source files exist
- required columns are present
- key identifiers are non-null and unique where appropriate
- core numeric/date fields are valid
- duplicate records are not introduced

The tests intentionally use the project's CSV files instead of requiring
external APIs or Kafka services. This keeps the test suite deterministic and
safe to run locally or in CI.
"""

from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


EXPECTED_FILES = {
    "orders.csv": {
        "required": {
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
        },
        "key": "order_id",
    },
    "customers.csv": {
        "required": {
            "customer_id",
            "customer_name",
            "region",
            "customer_type",
            "registration_date",
        },
        "key": "customer_id",
    },
    "drivers.csv": {
        "required": {
            "driver_id",
            "driver_name",
            "region",
            "experience_years",
            "rating",
            "employment_status",
            "license_type",
        },
        "key": "driver_id",
    },
    "vehicles.csv": {
        "required": {
            "vehicle_id",
            "vehicle_type",
            "driver_id",
            "fuel_type",
            "capacity_kg",
            "manufacture_year",
            "maintenance_status",
            "vehicle_status",
            "odometer_km",
        },
        "key": "vehicle_id",
    },
    "warehouses.csv": {
        "required": {
            "warehouse_id",
            "warehouse_name",
            "region",
            "capacity_units",
            "latitude",
            "longitude",
        },
        "key": "warehouse_id",
    },
    "gps_tracking.csv": {
        "required": {
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
        },
        "key": None,
    },
    "weather.csv": {
        "required": {
            "date",
            "location",
            "latitude",
            "longitude",
            "temperature_c",
            "humidity_pct",
            "rainfall_mm",
            "weather_condition",
        },
        "key": None,
    },
    "traffic.csv": {
        "required": {
            "timestamp",
            "location",
            "latitude",
            "longitude",
            "traffic_score",
            "traffic_level",
            "average_speed_kmph",
        },
        "key": None,
    },
    "roads.csv": {
        "required": {
            "road_id",
            "start_location",
            "end_location",
            "road_type",
            "distance_km",
            "lanes",
            "speed_limit_kmph",
            "road_condition",
        },
        "key": "road_id",
    },
    "delivery_updates.csv": {
        "required": {
            "event_id",
            "order_id",
            "driver_id",
            "vehicle_id",
            "event_timestamp",
            "region",
            "event_type",
            "delivery_status",
            "delay_minutes",
            "source",
        },
        "key": "event_id",
    },
}


@pytest.fixture(scope="session")
def raw_data():
    """Load the project's raw CSV datasets once for the test session."""
    datasets = {}

    for filename in EXPECTED_FILES:
        path = RAW_DIR / filename

        if not path.exists():
            pytest.fail(
                f"Required ingestion fixture is missing: {path}. "
                "Generate or restore the project's raw sample data first."
            )

        datasets[filename] = pd.read_csv(path)

    return datasets


@pytest.mark.parametrize("filename", EXPECTED_FILES)
def test_expected_raw_file_exists(filename):
    """Every declared ingestion source should exist in data/raw."""
    assert (RAW_DIR / filename).is_file()


@pytest.mark.parametrize("filename", EXPECTED_FILES)
def test_required_columns_are_present(raw_data, filename):
    """Every ingestion source must satisfy its documented schema."""
    df = raw_data[filename]
    required = EXPECTED_FILES[filename]["required"]

    missing = required.difference(df.columns)

    assert not missing, (
        f"{filename} is missing required columns: "
        f"{sorted(missing)}"
    )


@pytest.mark.parametrize(
    "filename",
    [
        filename
        for filename, config in EXPECTED_FILES.items()
        if config["key"] is not None
    ],
)
def test_primary_keys_are_present_and_unique(raw_data, filename):
    """Master/event datasets should not contain null or duplicate identifiers."""
    df = raw_data[filename]
    key = EXPECTED_FILES[filename]["key"]

    assert df[key].notna().all(), f"{filename}: null values found in {key}"
    assert df[key].astype(str).str.strip().ne("").all(), (
        f"{filename}: blank values found in {key}"
    )
    assert df[key].is_unique, f"{filename}: duplicate values found in {key}"


def test_orders_have_valid_business_values(raw_data):
    """Orders should contain non-negative financial and distance values."""
    df = raw_data["orders.csv"]

    for column in [
        "order_value",
        "delivery_distance_km",
        "promised_delivery_time_min",
        "actual_delivery_time_min",
    ]:
        values = pd.to_numeric(df[column], errors="coerce")
        assert values.notna().all(), f"orders.csv: invalid numeric values in {column}"
        assert (values >= 0).all(), f"orders.csv: negative values in {column}"


def test_order_dates_are_parseable(raw_data):
    """Order dates must be valid dates before downstream transformation."""
    df = raw_data["orders.csv"]

    dates = pd.to_datetime(df["order_date"], errors="coerce")

    assert dates.notna().all(), "orders.csv contains invalid order_date values"


def test_customer_registration_dates_are_parseable(raw_data):
    """Customer registration dates must be valid dates."""
    df = raw_data["customers.csv"]

    dates = pd.to_datetime(df["registration_date"], errors="coerce")

    assert dates.notna().all(), (
        "customers.csv contains invalid registration_date values"
    )


def test_gps_coordinates_are_valid(raw_data):
    """GPS latitude/longitude values must be inside geographic bounds."""
    df = raw_data["gps_tracking.csv"]

    latitude = pd.to_numeric(df["latitude"], errors="coerce")
    longitude = pd.to_numeric(df["longitude"], errors="coerce")

    assert latitude.notna().all()
    assert longitude.notna().all()
    assert latitude.between(-90, 90).all()
    assert longitude.between(-180, 180).all()


def test_weather_coordinates_and_measurements_are_valid(raw_data):
    """Weather coordinates and basic measurements must be valid."""
    df = raw_data["weather.csv"]

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


def test_traffic_score_is_in_expected_range(raw_data):
    """Traffic score should follow the project's 0-100 scoring convention."""
    df = raw_data["traffic.csv"]

    score = pd.to_numeric(df["traffic_score"], errors="coerce")
    speed = pd.to_numeric(df["average_speed_kmph"], errors="coerce")

    assert score.notna().all()
    assert score.between(0, 100).all()
    assert speed.notna().all()
    assert (speed >= 0).all()


def test_delivery_updates_have_valid_delay_values(raw_data):
    """Delivery event delays must be numeric and non-negative."""
    df = raw_data["delivery_updates.csv"]

    delay = pd.to_numeric(df["delay_minutes"], errors="coerce")

    assert delay.notna().all()
    assert (delay >= 0).all()


def test_roads_have_valid_geometry_attributes(raw_data):
    """Road reference data must have sensible positive dimensions."""
    df = raw_data["roads.csv"]

    distance = pd.to_numeric(df["distance_km"], errors="coerce")
    lanes = pd.to_numeric(df["lanes"], errors="coerce")
    speed_limit = pd.to_numeric(df["speed_limit_kmph"], errors="coerce")

    assert distance.notna().all()
    assert (distance > 0).all()
    assert lanes.notna().all()
    assert (lanes > 0).all()
    assert speed_limit.notna().all()
    assert (speed_limit > 0).all()


def test_streaming_event_keys_reference_existing_entities(raw_data):
    """Streaming delivery events should reference known orders, drivers and vehicles."""
    updates = raw_data["delivery_updates.csv"]

    orders = set(raw_data["orders.csv"]["order_id"].astype(str))
    drivers = set(raw_data["drivers.csv"]["driver_id"].astype(str))
    vehicles = set(raw_data["vehicles.csv"]["vehicle_id"].astype(str))

    update_orders = set(updates["order_id"].astype(str))
    update_drivers = set(updates["driver_id"].astype(str))
    update_vehicles = set(updates["vehicle_id"].astype(str))

    assert update_orders.issubset(orders), (
        "delivery_updates.csv contains unknown order_id values"
    )
    assert update_drivers.issubset(drivers), (
        "delivery_updates.csv contains unknown driver_id values"
    )
    assert update_vehicles.issubset(vehicles), (
        "delivery_updates.csv contains unknown vehicle_id values"
    )


def test_orders_reference_existing_master_entities(raw_data):
    """Orders should reference customers, drivers, vehicles and warehouses."""
    orders = raw_data["orders.csv"]

    master_columns = {
        "customer_id": raw_data["customers.csv"]["customer_id"],
        "driver_id": raw_data["drivers.csv"]["driver_id"],
        "vehicle_id": raw_data["vehicles.csv"]["vehicle_id"],
        "warehouse_id": raw_data["warehouses.csv"]["warehouse_id"],
    }

    for order_column, master_series in master_columns.items():
        known_ids = set(master_series.astype(str))
        referenced_ids = set(orders[order_column].astype(str))

        assert referenced_ids.issubset(known_ids), (
            f"orders.csv contains unknown {order_column} values"
        )


def test_vehicle_driver_relationship_is_consistent(raw_data):
    """Each vehicle's assigned driver should exist in the driver master."""
    vehicles = raw_data["vehicles.csv"]
    drivers = set(raw_data["drivers.csv"]["driver_id"].astype(str))

    assigned_drivers = set(vehicles["driver_id"].astype(str))

    assert assigned_drivers.issubset(drivers), (
        "vehicles.csv contains driver_id values absent from drivers.csv"
    )

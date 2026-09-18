"""
Generate deterministic synthetic logistics data for the project.

Usage:
    python scripts/generate_data.py

Optional:
    python scripts/generate_data.py --seed 42 --orders 5000

The generated data is intentionally synthetic and is suitable for local
development, testing, demonstrations, Spark/Hive/dbt exercises, and Power BI
prototyping. It is not production or operational data.

Existing files in data/raw are overwritten when this script runs.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"


REGIONS = [
    ("Chennai", 13.0827, 80.2707),
    ("Coimbatore", 11.0168, 76.9558),
    ("Madurai", 9.9252, 78.1198),
    ("Trichy", 10.7905, 78.7047),
    ("Salem", 11.6643, 78.1460),
    ("Tirunelveli", 8.7139, 77.7567),
    ("Erode", 11.3410, 77.7172),
    ("Vellore", 12.9165, 79.1325),
]

WEATHER_CONDITIONS = [
    "Clear",
    "Cloudy",
    "Light Rain",
    "Heavy Rain",
    "Thunderstorm",
]

TRAFFIC_LEVELS = ["Low", "Moderate", "High", "Severe"]
ORDER_STATUSES = ["Placed", "Processing", "Shipped", "Completed", "Cancelled"]
DELIVERY_STATUSES = ["Delivered", "Delayed", "In Transit", "Failed"]
CUSTOMER_TYPES = ["Retail", "Business", "Enterprise"]
VEHICLE_TYPES = ["Van", "Truck", "Mini Truck", "Bike"]
FUEL_TYPES = ["Diesel", "Petrol", "Electric", "CNG"]
EMPLOYMENT_STATUSES = ["Active", "Inactive"]
LICENSE_TYPES = ["LMV", "HMV", "Commercial"]
MAINTENANCE_STATUSES = ["Good", "Due Soon", "Due"]
VEHICLE_STATUSES = ["Active", "Idle", "Maintenance", "Inactive"]
ROAD_TYPES = ["Highway", "Urban", "Rural", "Expressway"]
ROAD_CONDITIONS = ["Good", "Fair", "Poor"]
EVENT_TYPES = [
    "Order Created",
    "Picked Up",
    "In Transit",
    "Out for Delivery",
    "Delivered",
    "Delivery Exception",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate synthetic logistics project data."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible output.",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=5000,
        help="Number of synthetic orders.",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=1000,
        help="Number of customers.",
    )
    parser.add_argument(
        "--drivers",
        type=int,
        default=200,
        help="Number of drivers.",
    )
    parser.add_argument(
        "--vehicles",
        type=int,
        default=200,
        help="Number of vehicles.",
    )
    parser.add_argument(
        "--warehouses",
        type=int,
        default=10,
        help="Number of warehouses.",
    )
    parser.add_argument(
        "--gps-records",
        type=int,
        default=20000,
        help="Number of GPS observations.",
    )
    parser.add_argument(
        "--delivery-events",
        type=int,
        default=10000,
        help="Number of delivery update events.",
    )
    return parser.parse_args()


def region_lookup():
    return {
        name: {"latitude": latitude, "longitude": longitude}
        for name, latitude, longitude in REGIONS
    }


def weighted_regions(rng, size):
    names = [item[0] for item in REGIONS]
    # Larger metros receive somewhat higher synthetic demand.
    probabilities = np.array(
        [0.25, 0.16, 0.12, 0.12, 0.10, 0.08, 0.08, 0.09],
        dtype=float,
    )
    probabilities /= probabilities.sum()
    return rng.choice(names, size=size, p=probabilities)


def make_customers(rng, count):
    regions = weighted_regions(rng, count)

    return pd.DataFrame(
        {
            "customer_id": [f"C{i:05d}" for i in range(1, count + 1)],
            "customer_name": [f"Customer {i:05d}" for i in range(1, count + 1)],
            "region": regions,
            "customer_type": rng.choice(
                CUSTOMER_TYPES,
                size=count,
                p=[0.62, 0.28, 0.10],
            ),
            "registration_date": pd.date_range(
                "2024-01-01",
                periods=count,
                freq="D",
            ).strftime("%Y-%m-%d"),
        }
    )


def make_drivers(rng, count):
    regions = weighted_regions(rng, count)

    return pd.DataFrame(
        {
            "driver_id": [f"D{i:04d}" for i in range(1, count + 1)],
            "driver_name": [f"Driver {i:04d}" for i in range(1, count + 1)],
            "region": regions,
            "experience_years": rng.integers(1, 16, size=count),
            "rating": np.round(
                np.clip(rng.normal(4.1, 0.45, size=count), 2.5, 5.0),
                2,
            ),
            "employment_status": rng.choice(
                EMPLOYMENT_STATUSES,
                size=count,
                p=[0.94, 0.06],
            ),
            "license_type": rng.choice(
                LICENSE_TYPES,
                size=count,
                p=[0.45, 0.30, 0.25],
            ),
        }
    )


def make_vehicles(rng, count, drivers):
    vehicle_types = rng.choice(
        VEHICLE_TYPES,
        size=count,
        p=[0.35, 0.22, 0.28, 0.15],
    )

    capacity_map = {
        "Van": 1200,
        "Truck": 5000,
        "Mini Truck": 2500,
        "Bike": 80,
    }

    capacities = np.array(
        [
            max(
                40,
                int(
                    capacity_map[vehicle_type]
                    * rng.uniform(0.80, 1.20)
                ),
            )
            for vehicle_type in vehicle_types
        ]
    )

    manufacture_year = rng.integers(2016, 2026, size=count)
    odometer = np.maximum(
        5000,
        (2026 - manufacture_year) * rng.uniform(12000, 30000, size=count)
        + rng.normal(10000, 5000, size=count),
    ).astype(int)

    maintenance = rng.choice(
        MAINTENANCE_STATUSES,
        size=count,
        p=[0.72, 0.20, 0.08],
    )

    status = rng.choice(
        VEHICLE_STATUSES,
        size=count,
        p=[0.74, 0.14, 0.07, 0.05],
    )

    return pd.DataFrame(
        {
            "vehicle_id": [f"V{i:04d}" for i in range(1, count + 1)],
            "vehicle_type": vehicle_types,
            "driver_id": rng.choice(
                drivers["driver_id"].to_numpy(),
                size=count,
                replace=False if count <= len(drivers) else True,
            ),
            "fuel_type": rng.choice(
                FUEL_TYPES,
                size=count,
                p=[0.40, 0.25, 0.15, 0.20],
            ),
            "capacity_kg": capacities,
            "manufacture_year": manufacture_year,
            "maintenance_status": maintenance,
            "vehicle_status": status,
            "odometer_km": odometer,
        }
    )


def make_warehouses(rng, count):
    lookup = region_lookup()
    region_names = [item[0] for item in REGIONS]

    selected_regions = [
        region_names[i % len(region_names)] for i in range(count)
    ]

    rows = []

    for index in range(count):
        region = selected_regions[index]
        coords = lookup[region]

        rows.append(
            {
                "warehouse_id": f"W{index + 1:03d}",
                "warehouse_name": f"{region} Distribution Hub {index + 1:02d}",
                "region": region,
                "capacity_units": int(rng.integers(5000, 30000)),
                "latitude": round(
                    coords["latitude"] + rng.normal(0, 0.03),
                    6,
                ),
                "longitude": round(
                    coords["longitude"] + rng.normal(0, 0.03),
                    6,
                ),
            }
        )

    return pd.DataFrame(rows)


def make_orders(rng, count, customers, drivers, vehicles, warehouses):
    customer_ids = customers["customer_id"].to_numpy()
    driver_ids = drivers["driver_id"].to_numpy()
    vehicle_ids = vehicles["vehicle_id"].to_numpy()
    warehouse_ids = warehouses["warehouse_id"].to_numpy()

    dates = pd.date_range("2026-01-01", "2026-08-31", freq="D")
    order_dates = rng.choice(dates, size=count)

    regions = weighted_regions(rng, count)

    # Use region to bias warehouse assignment toward the same region.
    warehouse_by_region = {
        region: warehouses.loc[
            warehouses["region"] == region,
            "warehouse_id",
        ].tolist()
        for region in warehouses["region"].unique()
    }

    selected_warehouses = []
    for region in regions:
        candidates = warehouse_by_region.get(region, [])
        if candidates:
            selected_warehouses.append(rng.choice(candidates))
        else:
            selected_warehouses.append(rng.choice(warehouse_ids))

    distances = np.round(
        np.clip(
            rng.gamma(shape=2.5, scale=8.0, size=count),
            1.0,
            180.0,
        ),
        2,
    )

    order_values = np.round(
        np.clip(
            rng.lognormal(mean=7.0, sigma=0.65, size=count),
            100,
            25000,
        ),
        2,
    )

    promised = np.maximum(
        30,
        np.round(
            25 + distances * 3.0 + rng.normal(0, 20, size=count)
        ),
    ).astype(int)

    # Delay probability increases with distance and with a small random
    # operational effect. This creates realistic-looking analytical patterns.
    delay_probability = np.clip(
        0.10
        + (distances / 180) * 0.30
        + rng.uniform(0, 0.10, size=count),
        0.05,
        0.65,
    )

    delayed = rng.random(count) < delay_probability
    delay_minutes = np.where(
        delayed,
        np.maximum(
            5,
            np.round(
                rng.gamma(shape=2.0, scale=12.0, size=count)
                + distances * 0.08
            ),
        ),
        0,
    ).astype(int)

    actual = promised + delay_minutes

    delivery_status = np.where(
        rng.random(count) < 0.96,
        np.where(delayed, "Delayed", "Delivered"),
        rng.choice(["In Transit", "Failed"], size=count, p=[0.80, 0.20]),
    )

    order_status = np.where(
        delivery_status == "Delivered",
        "Completed",
        np.where(
            delivery_status == "Failed",
            "Cancelled",
            rng.choice(
                ["Placed", "Processing", "Shipped"],
                size=count,
                p=[0.15, 0.35, 0.50],
            ),
        ),
    )

    return pd.DataFrame(
        {
            "order_id": [f"O{i:06d}" for i in range(1, count + 1)],
            "customer_id": rng.choice(customer_ids, size=count),
            "warehouse_id": selected_warehouses,
            "driver_id": rng.choice(driver_ids, size=count),
            "vehicle_id": rng.choice(vehicle_ids, size=count),
            "order_date": pd.to_datetime(order_dates).strftime("%Y-%m-%d"),
            "region": regions,
            "order_status": order_status,
            "delivery_status": delivery_status,
            "order_value": order_values,
            "delivery_distance_km": distances,
            "promised_delivery_time_min": promised,
            "actual_delivery_time_min": actual,
        }
    )


def make_gps(rng, count, vehicles):
    vehicle_ids = rng.choice(
        vehicles["vehicle_id"].to_numpy(),
        size=count,
    )

    start = pd.Timestamp("2026-08-01")
    timestamps = start + pd.to_timedelta(
        rng.integers(0, 31 * 24 * 60, size=count),
        unit="m",
    )

    region_names = [item[0] for item in REGIONS]
    regions = rng.choice(region_names, size=count)

    lookup = region_lookup()

    latitudes = []
    longitudes = []

    for region in regions:
        coords = lookup[region]
        latitudes.append(coords["latitude"] + rng.normal(0, 0.04))
        longitudes.append(coords["longitude"] + rng.normal(0, 0.04))

    speed = np.round(
        np.clip(
            rng.normal(34, 18, size=count),
            0,
            95,
        ),
        2,
    )

    signal_quality = np.round(
        np.clip(rng.normal(88, 12, size=count), 25, 100),
        2,
    )

    return pd.DataFrame(
        {
            "vehicle_id": vehicle_ids,
            "timestamp": pd.to_datetime(timestamps).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "region": regions,
            "latitude": np.round(latitudes, 6),
            "longitude": np.round(longitudes, 6),
            "speed_kmph": speed,
            "heading": np.round(rng.uniform(0, 360, size=count), 2),
            "vehicle_status": rng.choice(
                ["Moving", "Idle", "Stopped"],
                size=count,
                p=[0.65, 0.25, 0.10],
            ),
            "ignition_status": rng.choice(
                ["On", "Off"],
                size=count,
                p=[0.72, 0.28],
            ),
            "signal_quality": signal_quality,
        }
    )


def make_weather(rng):
    lookup = region_lookup()
    dates = pd.date_range("2026-01-01", "2026-08-31", freq="D")

    rows = []

    for date in dates:
        for region, latitude, longitude in REGIONS:
            # Monsoon-like seasonal effect for a synthetic Tamil Nadu dataset.
            rainy_season = date.month in [6, 7, 8]
            rain_probability = 0.25 if rainy_season else 0.12

            rainfall = (
                rng.gamma(1.7, 8.0)
                if rng.random() < rain_probability
                else 0.0
            )

            if rainfall >= 30:
                condition = "Heavy Rain"
            elif rainfall > 0:
                condition = "Light Rain"
            else:
                condition = rng.choice(["Clear", "Cloudy"], p=[0.68, 0.32])

            if rng.random() < 0.025:
                condition = "Thunderstorm"
                rainfall = max(rainfall, rng.uniform(15, 55))

            temperature = (
                28
                + 3.0 * math.sin(
                    2 * math.pi * (date.dayofyear / 365.25)
                )
                + rng.normal(0, 1.5)
            )

            humidity = np.clip(
                65 + rainfall * 0.7 + rng.normal(0, 8),
                30,
                100,
            )

            rows.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "location": region,
                    "latitude": latitude,
                    "longitude": longitude,
                    "temperature_c": round(float(temperature), 2),
                    "humidity_pct": round(float(humidity), 2),
                    "rainfall_mm": round(float(rainfall), 2),
                    "weather_condition": condition,
                }
            )

    return pd.DataFrame(rows)


def make_traffic(rng):
    lookup = region_lookup()
    timestamps = pd.date_range(
        "2026-08-01",
        "2026-08-31 23:00:00",
        freq="h",
    )

    rows = []

    for timestamp in timestamps:
        rush_hour = timestamp.hour in [8, 9, 10, 17, 18, 19, 20]

        for region, latitude, longitude in REGIONS:
            baseline = 55 if rush_hour else 80
            average_speed = np.clip(
                rng.normal(baseline, 10),
                8,
                100,
            )

            traffic_score = np.clip(
                100 - average_speed + rng.normal(0, 5),
                0,
                100,
            )

            if traffic_score >= 75:
                level = "Severe"
            elif traffic_score >= 50:
                level = "High"
            elif traffic_score >= 25:
                level = "Moderate"
            else:
                level = "Low"

            rows.append(
                {
                    "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "location": region,
                    "latitude": latitude,
                    "longitude": longitude,
                    "traffic_score": round(float(traffic_score), 2),
                    "traffic_level": level,
                    "average_speed_kmph": round(float(average_speed), 2),
                }
            )

    return pd.DataFrame(rows)


def make_roads(rng):
    road_rows = []
    road_id = 1

    for start_region, start_lat, start_lon in REGIONS:
        for end_region, end_lat, end_lon in REGIONS:
            if start_region >= end_region:
                continue

            distance = math.sqrt(
                (start_lat - end_lat) ** 2
                + (start_lon - end_lon) ** 2
            ) * 111

            distance = max(5, distance * rng.uniform(1.1, 1.5))

            road_rows.append(
                {
                    "road_id": f"R{road_id:04d}",
                    "start_location": start_region,
                    "end_location": end_region,
                    "road_type": rng.choice(ROAD_TYPES),
                    "distance_km": round(distance, 2),
                    "lanes": int(rng.integers(2, 6)),
                    "speed_limit_kmph": int(
                        rng.choice([40, 50, 60, 80, 100])
                    ),
                    "road_condition": rng.choice(
                        ROAD_CONDITIONS,
                        p=[0.60, 0.30, 0.10],
                    ),
                }
            )
            road_id += 1

    return pd.DataFrame(road_rows)


def make_delivery_updates(rng, count, orders):
    selected_orders = rng.choice(
        orders["order_id"].to_numpy(),
        size=count,
        replace=True,
    )

    order_lookup = orders.set_index("order_id")

    event_types = rng.choice(
        EVENT_TYPES,
        size=count,
        p=[0.12, 0.14, 0.28, 0.20, 0.20, 0.06],
    )

    rows = []

    for index, (order_id, event_type) in enumerate(
        zip(selected_orders, event_types),
        start=1,
    ):
        order = order_lookup.loc[order_id]

        order_date = pd.Timestamp(order["order_date"])

        event_timestamp = order_date + pd.to_timedelta(
            int(rng.integers(1, 1440)),
            unit="m",
        )

        delay = int(
            max(
                0,
                rng.normal(
                    float(
                        max(
                            0,
                            order["actual_delivery_time_min"]
                            - order["promised_delivery_time_min"],
                        )
                    ),
                    8,
                ),
            )
        )

        rows.append(
            {
                "event_id": f"E{index:07d}",
                "order_id": order_id,
                "driver_id": order["driver_id"],
                "vehicle_id": order["vehicle_id"],
                "event_timestamp": event_timestamp.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "region": order["region"],
                "event_type": event_type,
                "delivery_status": order["delivery_status"],
                "delay_minutes": delay,
                "source": "synthetic_generator",
            }
        )

    return pd.DataFrame(rows)


def make_sample_data(orders):
    """Create a compact sample file for GitHub/demo use."""
    columns = [
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
    ]

    return orders[columns].head(100).copy()


def write_csv(df, filename, directory=RAW_DIR):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    df.to_csv(path, index=False)
    return path


def main():
    args = parse_args()

    if args.orders <= 0:
        raise ValueError("--orders must be greater than 0")
    if args.customers <= 0:
        raise ValueError("--customers must be greater than 0")
    if args.drivers <= 0:
        raise ValueError("--drivers must be greater than 0")
    if args.vehicles <= 0:
        raise ValueError("--vehicles must be greater than 0")
    if args.warehouses <= 0:
        raise ValueError("--warehouses must be greater than 0")

    rng = np.random.default_rng(args.seed)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating synthetic logistics data...")
    print(f"Seed: {args.seed}")

    customers = make_customers(rng, args.customers)
    drivers = make_drivers(rng, args.drivers)
    vehicles = make_vehicles(rng, args.vehicles, drivers)
    warehouses = make_warehouses(rng, args.warehouses)
    orders = make_orders(
        rng,
        args.orders,
        customers,
        drivers,
        vehicles,
        warehouses,
    )
    gps = make_gps(rng, args.gps_records, vehicles)
    weather = make_weather(rng)
    traffic = make_traffic(rng)
    roads = make_roads(rng)
    delivery_updates = make_delivery_updates(
        rng,
        args.delivery_events,
        orders,
    )

    datasets = {
        "orders.csv": orders,
        "customers.csv": customers,
        "drivers.csv": drivers,
        "vehicles.csv": vehicles,
        "warehouses.csv": warehouses,
        "gps_tracking.csv": gps,
        "weather.csv": weather,
        "traffic.csv": traffic,
        "roads.csv": roads,
        "delivery_updates.csv": delivery_updates,
    }

    for filename, dataframe in datasets.items():
        path = write_csv(dataframe, filename)
        print(f"  {filename:<22} {len(dataframe):>7,} rows -> {path}")

    sample_path = write_csv(
        make_sample_data(orders),
        "sample_data.csv",
        SAMPLE_DIR,
    )

    print(
        f"  {'sample_data.csv':<22} "
        f"{len(orders.head(100)):>7,} rows -> {sample_path}"
    )

    print()
    print("Generation complete.")
    print("Raw data is synthetic and intended for development/demo use.")
    print("Run the transformation pipeline after generation:")
    print("  bash scripts/run_pipeline.sh")


if __name__ == "__main__":
    main()

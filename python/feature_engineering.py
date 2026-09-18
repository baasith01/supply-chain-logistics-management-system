"""
Feature Engineering - Logistics & Supply Chain Intelligence Platform

Purpose:
    Build reusable machine-learning and analytics features from the project's
    cleaned logistics datasets.

Inputs:
    data/processed/
        orders_clean.csv
        gps_clean.csv
        weather_clean.csv

Fallback:
    If processed files are unavailable, the script can read the corresponding
    files from data/raw/.

Outputs:
    analytics/output/features/
        delivery_features.csv
        gps_features.csv
        customer_features.csv
        driver_features.csv
        warehouse_features.csv
        feature_summary.csv

Usage:
    python analytics/python/feature_engineering.py

Optional:
    python analytics/python/feature_engineering.py \
        --processed-dir data/processed \
        --raw-dir data/raw \
        --output-dir analytics/output/features

Design notes:
    - Raw and processed source files are never modified.
    - Features are deterministic and reproducible.
    - No target leakage is intentionally introduced into the delivery-delay
      feature set: actual_delivery_time_min is not used to calculate
      predictive pre-delivery features.
    - The output is suitable for downstream analytics and ML experiments.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build analytics and ML features for the logistics project."
    )
    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Directory containing cleaned datasets.",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Fallback directory containing raw datasets.",
    )
    parser.add_argument(
        "--output-dir",
        default="analytics/output/features",
        help="Directory for generated feature datasets.",
    )
    return parser.parse_args()


def load_csv(
    filename: str,
    processed_dir: Path,
    raw_dir: Path,
) -> pd.DataFrame | None:
    """Load processed data first, then fall back to raw data."""
    processed_path = processed_dir / filename
    raw_path = raw_dir / filename

    if processed_path.exists():
        print(f"[OK] Loading processed: {processed_path}")
        return pd.read_csv(processed_path)

    if raw_path.exists():
        print(f"[WARN] Processed file missing; using raw: {raw_path}")
        return pd.read_csv(raw_path)

    print(f"[WARN] Dataset not found: {filename}")
    return None


def to_numeric(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Safely convert selected columns to numeric values."""
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    return df


def add_calendar_features(
    df: pd.DataFrame,
    date_column: str,
) -> pd.DataFrame:
    """Add calendar features from a date/timestamp column."""
    df = df.copy()

    if date_column not in df.columns:
        return df

    dt = pd.to_datetime(
        df[date_column],
        errors="coerce",
    )

    df["feature_year"] = dt.dt.year
    df["feature_month"] = dt.dt.month
    df["feature_day"] = dt.dt.day
    df["feature_day_of_week"] = dt.dt.dayofweek
    df["feature_day_name"] = dt.dt.day_name()
    df["feature_week_of_year"] = dt.dt.isocalendar().week.astype("Int64")
    df["feature_is_weekend"] = (
        dt.dt.dayofweek >= 5
    ).astype("Int64")

    return df


def build_delivery_features(
    orders: pd.DataFrame,
    weather: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Build one row per order for delivery analytics and ML.

    Important:
        Features intended for pre-delivery prediction do not use
        actual_delivery_time_min or delivery_delay_min as predictors.
        Those fields are retained only as labels/analysis outcomes.
    """
    df = orders.copy()

    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(
            df["order_date"],
            errors="coerce",
        )

    df = to_numeric(
        df,
        [
            "order_value",
            "delivery_distance_km",
            "promised_delivery_time_min",
            "actual_delivery_time_min",
        ],
    )

    # Outcome/label fields.
    if {
        "actual_delivery_time_min",
        "promised_delivery_time_min",
    }.issubset(df.columns):
        df["delivery_delay_min"] = (
            df["actual_delivery_time_min"]
            - df["promised_delivery_time_min"]
        )

        df["is_delayed"] = (
            df["delivery_delay_min"] > 0
        ).astype(int)

    # Calendar features.
    df = add_calendar_features(
        df,
        "order_date",
    )

    if "order_date" in df.columns:
        df["feature_is_month_start"] = (
            df["order_date"].dt.is_month_start
        ).astype("Int64")

        df["feature_is_month_end"] = (
            df["order_date"].dt.is_month_end
        ).astype("Int64")

    # Distance/value features.
    if {
        "order_value",
        "delivery_distance_km",
    }.issubset(df.columns):
        safe_distance = df["delivery_distance_km"].replace(0, np.nan)

        df["order_value_per_km"] = (
            df["order_value"] / safe_distance
        )

        df["distance_value_interaction"] = (
            df["delivery_distance_km"]
            * df["order_value"]
        )

    if {
        "delivery_distance_km",
        "promised_delivery_time_min",
    }.issubset(df.columns):
        safe_time = df["promised_delivery_time_min"].replace(
            0,
            np.nan,
        )

        df["planned_speed_kmph"] = (
            df["delivery_distance_km"] / safe_time * 60
        )

    # Order-value buckets.
    if "order_value" in df.columns:
        df["order_value_band"] = pd.cut(
            df["order_value"],
            bins=[
                -np.inf,
                500,
                1000,
                2500,
                5000,
                np.inf,
            ],
            labels=[
                "Very Low",
                "Low",
                "Medium",
                "High",
                "Very High",
            ],
        )

    # Distance buckets.
    if "delivery_distance_km" in df.columns:
        df["distance_band"] = pd.cut(
            df["delivery_distance_km"],
            bins=[
                -np.inf,
                5,
                15,
                30,
                50,
                np.inf,
            ],
            labels=[
                "Short",
                "Medium",
                "Long",
                "Very Long",
                "Extreme",
            ],
        )

    # Delivery-risk label for downstream classification analysis.
    if "delivery_delay_min" in df.columns:
        df["delay_severity"] = pd.cut(
            df["delivery_delay_min"],
            bins=[
                -np.inf,
                0,
                10,
                30,
                60,
                np.inf,
            ],
            labels=[
                "On Time",
                "Minor Delay",
                "Moderate Delay",
                "Severe Delay",
                "Critical Delay",
            ],
            right=True,
        )

    # Weather enrichment.
    if weather is not None and not weather.empty:
        weather_df = weather.copy()

        if "date" in weather_df.columns:
            weather_df["date"] = pd.to_datetime(
                weather_df["date"],
                errors="coerce",
            )

        weather_df = to_numeric(
            weather_df,
            [
                "temperature_c",
                "humidity_pct",
                "rainfall_mm",
            ],
        )

        # Normalize weather join key.
        if "location" in weather_df.columns:
            weather_df["weather_region"] = (
                weather_df["location"]
                .astype(str)
                .str.strip()
                .str.lower()
            )
        elif "region" in weather_df.columns:
            weather_df["weather_region"] = (
                weather_df["region"]
                .astype(str)
                .str.strip()
                .str.lower()
            )

        if "region" in df.columns:
            df["weather_region"] = (
                df["region"]
                .astype(str)
                .str.strip()
                .str.lower()
            )

        weather_columns = [
            column
            for column in [
                "date",
                "weather_region",
                "temperature_c",
                "humidity_pct",
                "rainfall_mm",
                "weather_condition",
            ]
            if column in weather_df.columns
        ]

        if {
            "date",
            "weather_region",
        }.issubset(weather_df.columns) and {
            "order_date",
            "weather_region",
        }.issubset(df.columns):
            weather_lookup = (
                weather_df[weather_columns]
                .drop_duplicates(
                    ["date", "weather_region"],
                    keep="last",
                )
                .rename(
                    columns={
                        "date": "weather_date",
                    }
                )
            )

            df = df.merge(
                weather_lookup,
                left_on=[
                    "order_date",
                    "weather_region",
                ],
                right_on=[
                    "weather_date",
                    "weather_region",
                ],
                how="left",
            )

            df.drop(
                columns=["weather_date"],
                inplace=True,
                errors="ignore",
            )

        if "rainfall_mm" in df.columns:
            df["is_rainy"] = (
                df["rainfall_mm"].fillna(0) > 0
            ).astype(int)

            df["heavy_rain_flag"] = (
                df["rainfall_mm"].fillna(0) >= 10
            ).astype(int)

        if "humidity_pct" in df.columns:
            df["high_humidity_flag"] = (
                df["humidity_pct"].fillna(0) >= 80
            ).astype(int)

        if "rainfall_mm" in df.columns:
            df["weather_risk_score"] = (
                df["rainfall_mm"].fillna(0).clip(upper=50) * 2
            )

            if "humidity_pct" in df.columns:
                df["weather_risk_score"] += (
                    df["humidity_pct"]
                    .fillna(0)
                    .sub(70)
                    .clip(lower=0, upper=30)
                    / 3
                )

            df["weather_risk_score"] = (
                df["weather_risk_score"]
                .clip(upper=100)
                .round(2)
            )

            df["weather_risk_category"] = pd.cut(
                df["weather_risk_score"],
                bins=[
                    -np.inf,
                    20,
                    50,
                    75,
                    np.inf,
                ],
                labels=[
                    "Low",
                    "Moderate",
                    "High",
                    "Critical",
                ],
            )

        df.drop(
            columns=["weather_region"],
            inplace=True,
            errors="ignore",
        )

    return df


def build_gps_features(
    gps: pd.DataFrame,
) -> pd.DataFrame:
    """Build one row per GPS observation with movement and signal features."""
    df = gps.copy()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
        )

        df = add_calendar_features(
            df,
            "timestamp",
        )

        df["gps_hour"] = df["timestamp"].dt.hour

        df["is_peak_hour"] = (
            df["gps_hour"].isin([7, 8, 9, 17, 18, 19, 20])
        ).astype("Int64")

    df = to_numeric(
        df,
        [
            "speed_kmph",
            "heading",
            "signal_quality",
        ],
    )

    if "speed_kmph" in df.columns:
        df["is_moving"] = (
            df["speed_kmph"].fillna(0) > 3
        ).astype(int)

        df["is_high_speed"] = (
            df["speed_kmph"].fillna(0) > 80
        ).astype(int)

        df["speed_band"] = pd.cut(
            df["speed_kmph"],
            bins=[
                -np.inf,
                3,
                20,
                50,
                80,
                np.inf,
            ],
            labels=[
                "Stationary",
                "Slow",
                "Normal",
                "Fast",
                "Very Fast",
            ],
        )

    if "signal_quality" in df.columns:
        df["is_low_signal"] = (
            df["signal_quality"].fillna(0) < 50
        ).astype(int)

        df["signal_quality_band"] = pd.cut(
            df["signal_quality"],
            bins=[
                -np.inf,
                30,
                60,
                80,
                np.inf,
            ],
            labels=[
                "Poor",
                "Fair",
                "Good",
                "Excellent",
            ],
        )

    if {
        "vehicle_status",
        "ignition_status",
    }.issubset(df.columns):
        df["is_active_vehicle"] = (
            df["vehicle_status"]
            .astype(str)
            .str.lower()
            .eq("active")
            & df["ignition_status"]
            .astype(str)
            .str.lower()
            .isin(["on", "1", "true"])
        ).astype(int)

    return df


def build_customer_features(
    orders: pd.DataFrame,
) -> pd.DataFrame:
    """Create one row per customer."""
    required = {
        "customer_id",
        "order_id",
    }

    if not required.issubset(orders.columns):
        return pd.DataFrame()

    grouped = (
        orders.groupby("customer_id")
        .agg(
            total_orders=("order_id", "count"),
            total_order_value=("order_value", "sum"),
            average_order_value=("order_value", "mean"),
            average_distance_km=("delivery_distance_km", "mean"),
        )
        .reset_index()
    )

    if "delivery_delay_min" in orders.columns:
        delay = (
            orders.groupby("customer_id")
            .agg(
                average_delay_min=("delivery_delay_min", "mean"),
                delayed_orders=("is_delayed", "sum"),
            )
            .reset_index()
        )

        grouped = grouped.merge(
            delay,
            on="customer_id",
            how="left",
        )

        grouped["delay_rate"] = (
            grouped["delayed_orders"]
            / grouped["total_orders"].replace(0, np.nan)
        )

        grouped["on_time_rate"] = (
            1 - grouped["delay_rate"]
        )

    if "order_date" in orders.columns:
        dates = (
            orders.assign(
                order_date=pd.to_datetime(
                    orders["order_date"],
                    errors="coerce",
                )
            )
            .groupby("customer_id")
            .agg(
                first_order_date=("order_date", "min"),
                last_order_date=("order_date", "max"),
                active_order_days=("order_date", "nunique"),
            )
            .reset_index()
        )

        grouped = grouped.merge(
            dates,
            on="customer_id",
            how="left",
        )

        grouped["customer_lifetime_days"] = (
            grouped["last_order_date"]
            - grouped["first_order_date"]
        ).dt.days

    grouped["customer_value_segment"] = pd.cut(
        grouped["total_order_value"],
        bins=[
            -np.inf,
            5000,
            20000,
            50000,
            np.inf,
        ],
        labels=[
            "Low",
            "Medium",
            "High",
            "Very High",
        ],
    )

    grouped["customer_frequency_segment"] = pd.cut(
        grouped["total_orders"],
        bins=[
            -np.inf,
            1,
            3,
            8,
            15,
            np.inf,
        ],
        labels=[
            "One-Time",
            "Occasional",
            "Frequent",
            "Highly Frequent",
            "Very Highly Frequent",
        ],
    )

    return grouped


def build_driver_features(
    orders: pd.DataFrame,
) -> pd.DataFrame:
    """Create one row per driver."""
    required = {
        "driver_id",
        "order_id",
    }

    if not required.issubset(orders.columns):
        return pd.DataFrame()

    grouped = (
        orders.groupby("driver_id")
        .agg(
            total_orders=("order_id", "count"),
            total_order_value=("order_value", "sum"),
            average_order_value=("order_value", "mean"),
            average_distance_km=("delivery_distance_km", "mean"),
        )
        .reset_index()
    )

    if "delivery_delay_min" in orders.columns:
        grouped = grouped.merge(
            orders.groupby("driver_id")
            .agg(
                average_delay_min=("delivery_delay_min", "mean"),
                delayed_orders=("is_delayed", "sum"),
            )
            .reset_index(),
            on="driver_id",
            how="left",
        )

        grouped["delay_rate"] = (
            grouped["delayed_orders"]
            / grouped["total_orders"].replace(0, np.nan)
        )

        grouped["on_time_rate"] = (
            1 - grouped["delay_rate"]
        )

    grouped["driver_performance_category"] = np.select(
        [
            (grouped["on_time_rate"] >= 0.90)
            & (grouped["average_delay_min"] < 10),
            (grouped["on_time_rate"] >= 0.75)
            & (grouped["average_delay_min"] < 20),
            (grouped["on_time_rate"] >= 0.50)
            & (grouped["average_delay_min"] < 30),
        ],
        [
            "High Performer",
            "Good Performer",
            "Needs Improvement",
        ],
        default="High Risk",
    )

    return grouped


def build_warehouse_features(
    orders: pd.DataFrame,
) -> pd.DataFrame:
    """Create one row per warehouse using actual order workload."""
    required = {
        "warehouse_id",
        "order_id",
    }

    if not required.issubset(orders.columns):
        return pd.DataFrame()

    grouped = (
        orders.groupby("warehouse_id")
        .agg(
            total_orders=("order_id", "count"),
            total_order_value=("order_value", "sum"),
            average_order_value=("order_value", "mean"),
        )
        .reset_index()
    )

    if "delivery_delay_min" in orders.columns:
        grouped = grouped.merge(
            orders.groupby("warehouse_id")
            .agg(
                average_delay_min=("delivery_delay_min", "mean"),
                delayed_orders=("is_delayed", "sum"),
            )
            .reset_index(),
            on="warehouse_id",
            how="left",
        )

        grouped["delay_rate"] = (
            grouped["delayed_orders"]
            / grouped["total_orders"].replace(0, np.nan)
        )

    grouped["workload_segment"] = pd.qcut(
        grouped["total_orders"].rank(method="first"),
        q=4,
        labels=[
            "Low",
            "Medium",
            "High",
            "Very High",
        ],
        duplicates="drop",
    )

    return grouped


def create_feature_summary(
    features: dict[str, pd.DataFrame],
    output_dir: Path,
) -> None:
    """Write a compact feature inventory."""
    rows = []

    for dataset_name, df in features.items():
        for column in df.columns:
            rows.append(
                {
                    "feature_dataset": dataset_name,
                    "feature_name": column,
                    "data_type": str(df[column].dtype),
                    "rows": len(df),
                    "missing_count": int(df[column].isna().sum()),
                    "missing_pct": round(
                        df[column].isna().mean() * 100,
                        2,
                    ),
                    "unique_values": int(df[column].nunique(dropna=True)),
                }
            )

    summary = pd.DataFrame(rows)

    if not summary.empty:
        summary = summary.sort_values(
            ["feature_dataset", "feature_name"]
        )

    summary.to_csv(
        output_dir / "feature_summary.csv",
        index=False,
    )


def run_feature_engineering(
    processed_dir: Path,
    raw_dir: Path,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    orders = load_csv(
        "orders_clean.csv",
        processed_dir,
        raw_dir,
    )

    if orders is None:
        orders = load_csv(
            "orders.csv",
            processed_dir,
            raw_dir,
        )

    weather = load_csv(
        "weather_clean.csv",
        processed_dir,
        raw_dir,
    )

    if weather is None:
        weather = load_csv(
            "weather.csv",
            processed_dir,
            raw_dir,
        )

    gps = load_csv(
        "gps_clean.csv",
        processed_dir,
        raw_dir,
    )

    if gps is None:
        gps = load_csv(
            "gps_tracking.csv",
            processed_dir,
            raw_dir,
        )

    if orders is None:
        raise FileNotFoundError(
            "orders_clean.csv/orders.csv is required for feature engineering."
        )

    print("\n[1/5] Building delivery features...")
    delivery_features = build_delivery_features(
        orders,
        weather,
    )
    delivery_features.to_csv(
        output_dir / "delivery_features.csv",
        index=False,
    )

    features: dict[str, pd.DataFrame] = {
        "delivery_features": delivery_features,
    }

    if gps is not None:
        print("[2/5] Building GPS features...")
        gps_features = build_gps_features(gps)
        gps_features.to_csv(
            output_dir / "gps_features.csv",
            index=False,
        )
        features["gps_features"] = gps_features
    else:
        print("[WARN] GPS data unavailable; skipping GPS features.")

    print("[3/5] Building customer features...")
    customer_features = build_customer_features(orders)
    customer_features.to_csv(
        output_dir / "customer_features.csv",
        index=False,
    )
    features["customer_features"] = customer_features

    print("[4/5] Building driver features...")
    driver_features = build_driver_features(orders)
    driver_features.to_csv(
        output_dir / "driver_features.csv",
        index=False,
    )
    features["driver_features"] = driver_features

    print("[5/5] Building warehouse features...")
    warehouse_features = build_warehouse_features(orders)
    warehouse_features.to_csv(
        output_dir / "warehouse_features.csv",
        index=False,
    )
    features["warehouse_features"] = warehouse_features

    create_feature_summary(
        features,
        output_dir,
    )

    print("\nFeature engineering completed successfully.")
    print(f"Outputs written to: {output_dir.resolve()}")
    for name, df in features.items():
        print(f"  - {name}: {len(df):,} rows x {len(df.columns)} columns")


def main() -> None:
    args = parse_args()

    run_feature_engineering(
        processed_dir=Path(args.processed_dir),
        raw_dir=Path(args.raw_dir),
        output_dir=Path(args.output_dir),
    )


if __name__ == "__main__":
    main()

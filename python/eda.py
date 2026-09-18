"""
EDA - Logistics & Supply Chain Intelligence Platform

Purpose:
    Perform reusable exploratory data analysis (EDA) on the project's raw
    logistics datasets.

Inputs:
    data/raw/
        orders.csv
        customers.csv
        drivers.csv
        vehicles.csv
        warehouses.csv
        gps_tracking.csv
        weather.csv
        traffic.csv
        roads.csv
        delivery_updates.csv

Outputs:
    analytics/output/eda/
        dataset_summary.csv
        missing_values.csv
        numeric_summary.csv
        orders_by_region.csv
        delivery_status_summary.csv
        driver_performance_summary.csv
        warehouse_workload_summary.csv
        weather_summary.csv
        traffic_summary.csv
        charts/*.png

Usage:
    python analytics/python/eda.py

Optional:
    python analytics/python/eda.py --data-dir data/raw --output-dir analytics/output/eda

Notes:
    - The script is designed to run even if one optional dataset is missing.
    - It does not modify the raw datasets.
    - It uses only fields that exist in the project's generated datasets.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


DATASETS = {
    "orders": "orders.csv",
    "customers": "customers.csv",
    "drivers": "drivers.csv",
    "vehicles": "vehicles.csv",
    "warehouses": "warehouses.csv",
    "gps_tracking": "gps_tracking.csv",
    "weather": "weather.csv",
    "traffic": "traffic.csv",
    "roads": "roads.csv",
    "delivery_updates": "delivery_updates.csv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run exploratory data analysis for the logistics project."
    )
    parser.add_argument(
        "--data-dir",
        default="data/raw",
        help="Directory containing raw CSV datasets.",
    )
    parser.add_argument(
        "--output-dir",
        default="analytics/output/eda",
        help="Directory where EDA outputs will be written.",
    )
    return parser.parse_args()


def load_datasets(data_dir: Path) -> dict[str, pd.DataFrame]:
    """Load available CSV datasets without changing their contents."""
    datasets: dict[str, pd.DataFrame] = {}

    for name, filename in DATASETS.items():
        path = data_dir / filename

        if not path.exists():
            print(f"[WARN] Missing dataset: {path}")
            continue

        try:
            df = pd.read_csv(path)
            datasets[name] = df
            print(f"[OK] Loaded {name}: {len(df):,} rows x {len(df.columns)} columns")
        except Exception as exc:
            print(f"[WARN] Could not load {path}: {exc}")

    if not datasets:
        raise FileNotFoundError(
            f"No CSV datasets were found in {data_dir.resolve()}"
        )

    return datasets


def save_csv(df: pd.DataFrame, path: Path) -> None:
    """Save a DataFrame as CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def create_dataset_summary(
    datasets: dict[str, pd.DataFrame],
    output_dir: Path,
) -> None:
    """Create row/column/duplicate summary for every loaded dataset."""
    rows = []

    for name, df in datasets.items():
        rows.append(
            {
                "dataset": name,
                "rows": len(df),
                "columns": len(df.columns),
                "duplicate_rows": int(df.duplicated().sum()),
                "missing_cells": int(df.isna().sum().sum()),
                "missing_cells_pct": round(
                    df.isna().sum().sum()
                    / max(df.shape[0] * df.shape[1], 1)
                    * 100,
                    2,
                ),
            }
        )

    summary = pd.DataFrame(rows).sort_values("rows", ascending=False)
    save_csv(summary, output_dir / "dataset_summary.csv")


def create_missing_values_report(
    datasets: dict[str, pd.DataFrame],
    output_dir: Path,
) -> None:
    """Create column-level missing-value statistics."""
    rows = []

    for dataset_name, df in datasets.items():
        for column in df.columns:
            missing_count = int(df[column].isna().sum())

            rows.append(
                {
                    "dataset": dataset_name,
                    "column": column,
                    "missing_count": missing_count,
                    "missing_pct": round(
                        missing_count / max(len(df), 1) * 100,
                        2,
                    ),
                }
            )

    report = pd.DataFrame(rows)

    if not report.empty:
        report = report.sort_values(
            ["missing_count", "dataset", "column"],
            ascending=[False, True, True],
        )

    save_csv(report, output_dir / "missing_values.csv")


def create_numeric_summary(
    datasets: dict[str, pd.DataFrame],
    output_dir: Path,
) -> None:
    """Create descriptive statistics for numeric columns."""
    rows = []

    for dataset_name, df in datasets.items():
        numeric_df = df.select_dtypes(include="number")

        if numeric_df.empty:
            continue

        summary = numeric_df.describe().T.reset_index()
        summary = summary.rename(columns={"index": "column"})
        summary.insert(0, "dataset", dataset_name)
        rows.append(summary)

    if rows:
        result = pd.concat(rows, ignore_index=True)
    else:
        result = pd.DataFrame()

    save_csv(result, output_dir / "numeric_summary.csv")


def analyze_orders(
    orders: pd.DataFrame,
    output_dir: Path,
    charts_dir: Path,
) -> None:
    """Analyze order volume, value, status, distance, and delivery delay."""
    orders = orders.copy()

    if "order_date" in orders.columns:
        orders["order_date"] = pd.to_datetime(
            orders["order_date"],
            errors="coerce",
        )

    if "order_value" in orders.columns:
        orders["order_value"] = pd.to_numeric(
            orders["order_value"],
            errors="coerce",
        )

    if "delivery_distance_km" in orders.columns:
        orders["delivery_distance_km"] = pd.to_numeric(
            orders["delivery_distance_km"],
            errors="coerce",
        )

    if "actual_delivery_time_min" in orders.columns:
        orders["actual_delivery_time_min"] = pd.to_numeric(
            orders["actual_delivery_time_min"],
            errors="coerce",
        )

    if "promised_delivery_time_min" in orders.columns:
        orders["promised_delivery_time_min"] = pd.to_numeric(
            orders["promised_delivery_time_min"],
            errors="coerce",
        )

    if {
        "actual_delivery_time_min",
        "promised_delivery_time_min",
    }.issubset(orders.columns):
        orders["delivery_delay_min"] = (
            orders["actual_delivery_time_min"]
            - orders["promised_delivery_time_min"]
        )

        orders["is_delayed"] = (
            orders["delivery_delay_min"] > 0
        ).astype(int)

    if "region" in orders.columns:
        region = (
            orders.groupby("region")
            .agg(
                orders=("order_id", "count"),
                total_order_value=("order_value", "sum"),
                average_order_value=("order_value", "mean"),
                average_distance_km=("delivery_distance_km", "mean"),
            )
            .reset_index()
        )

        if "delivery_delay_min" in orders.columns:
            delay = (
                orders.groupby("region")
                .agg(
                    average_delay_min=("delivery_delay_min", "mean"),
                    delayed_orders=("is_delayed", "sum"),
                )
                .reset_index()
            )
            region = region.merge(delay, on="region", how="left")

        save_csv(
            region.sort_values("orders", ascending=False),
            output_dir / "orders_by_region.csv",
        )

        plt.figure(figsize=(10, 6))
        region_sorted = region.sort_values("orders", ascending=True)
        plt.barh(region_sorted["region"], region_sorted["orders"])
        plt.title("Orders by Region")
        plt.xlabel("Number of Orders")
        plt.ylabel("Region")
        plt.tight_layout()
        plt.savefig(charts_dir / "orders_by_region.png", dpi=150)
        plt.close()

    if "delivery_status" in orders.columns:
        status = (
            orders["delivery_status"]
            .value_counts(dropna=False)
            .rename_axis("delivery_status")
            .reset_index(name="orders")
        )

        status["order_share_pct"] = round(
            status["orders"] / max(len(orders), 1) * 100,
            2,
        )

        save_csv(status, output_dir / "delivery_status_summary.csv")

        plt.figure(figsize=(9, 5))
        plt.bar(status["delivery_status"].astype(str), status["orders"])
        plt.title("Delivery Status Distribution")
        plt.xlabel("Delivery Status")
        plt.ylabel("Orders")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        plt.savefig(charts_dir / "delivery_status_distribution.png", dpi=150)
        plt.close()

    if "order_value" in orders.columns:
        plt.figure(figsize=(9, 5))
        plt.hist(
            orders["order_value"].dropna(),
            bins=30,
        )
        plt.title("Order Value Distribution")
        plt.xlabel("Order Value")
        plt.ylabel("Number of Orders")
        plt.tight_layout()
        plt.savefig(charts_dir / "order_value_distribution.png", dpi=150)
        plt.close()

    if {
        "delivery_distance_km",
        "delivery_delay_min",
    }.issubset(orders.columns):
        sample = orders[
            ["delivery_distance_km", "delivery_delay_min"]
        ].dropna()

        if not sample.empty:
            plt.figure(figsize=(9, 6))
            plt.scatter(
                sample["delivery_distance_km"],
                sample["delivery_delay_min"],
                alpha=0.35,
            )
            plt.axhline(0, linewidth=1)
            plt.title("Delivery Distance vs Delay")
            plt.xlabel("Delivery Distance (km)")
            plt.ylabel("Delivery Delay (min)")
            plt.tight_layout()
            plt.savefig(charts_dir / "distance_vs_delay.png", dpi=150)
            plt.close()


def analyze_customers(
    customers: pd.DataFrame,
    orders: pd.DataFrame | None,
    output_dir: Path,
) -> None:
    """Analyze customer distribution and, when orders exist, customer activity."""
    if orders is None or "customer_id" not in orders.columns:
        return

    customer_orders = (
        orders.groupby("customer_id")
        .agg(
            total_orders=("order_id", "count"),
            total_order_value=("order_value", "sum"),
            average_order_value=("order_value", "mean"),
        )
        .reset_index()
        .sort_values("total_order_value", ascending=False)
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
        customer_orders = customer_orders.merge(
            delay,
            on="customer_id",
            how="left",
        )

    save_csv(
        customer_orders,
        output_dir / "customer_activity_summary.csv",
    )

    if "region" in customers.columns:
        customer_region = (
            customers.groupby("region")
            .agg(customers=("customer_id", "nunique"))
            .reset_index()
            .sort_values("customers", ascending=False)
        )

        save_csv(
            customer_region,
            output_dir / "customers_by_region.csv",
        )


def analyze_drivers(
    drivers: pd.DataFrame,
    orders: pd.DataFrame | None,
    output_dir: Path,
) -> None:
    """Analyze driver workload and delivery performance."""
    if orders is None or "driver_id" not in orders.columns:
        return

    metrics = (
        orders.groupby("driver_id")
        .agg(
            total_orders=("order_id", "count"),
            total_order_value=("order_value", "sum"),
            average_order_value=("order_value", "mean"),
        )
        .reset_index()
    )

    if "delivery_delay_min" in orders.columns:
        metrics = metrics.merge(
            orders.groupby("driver_id")
            .agg(
                average_delay_min=("delivery_delay_min", "mean"),
                delayed_orders=("is_delayed", "sum"),
            )
            .reset_index(),
            on="driver_id",
            how="left",
        )

        metrics["delay_rate_pct"] = round(
            metrics["delayed_orders"]
            / metrics["total_orders"]
            * 100,
            2,
        )

        metrics["on_time_rate_pct"] = round(
            100 - metrics["delay_rate_pct"],
            2,
        )

    if "region" in drivers.columns:
        metrics = metrics.merge(
            drivers[["driver_id", "region"]],
            on="driver_id",
            how="left",
        )

    save_csv(
        metrics.sort_values("total_orders", ascending=False),
        output_dir / "driver_performance_summary.csv",
    )


def analyze_warehouses(
    warehouses: pd.DataFrame,
    orders: pd.DataFrame | None,
    output_dir: Path,
) -> None:
    """Analyze warehouse workload using actual order assignments."""
    if orders is None or "warehouse_id" not in orders.columns:
        return

    metrics = (
        orders.groupby("warehouse_id")
        .agg(
            total_orders=("order_id", "count"),
            total_order_value=("order_value", "sum"),
            average_order_value=("order_value", "mean"),
        )
        .reset_index()
    )

    if "delivery_delay_min" in orders.columns:
        metrics = metrics.merge(
            orders.groupby("warehouse_id")
            .agg(
                average_delivery_delay_min=("delivery_delay_min", "mean"),
                delayed_orders=("is_delayed", "sum"),
            )
            .reset_index(),
            on="warehouse_id",
            how="left",
        )

        metrics["delay_rate_pct"] = round(
            metrics["delayed_orders"]
            / metrics["total_orders"]
            * 100,
            2,
        )

    warehouse_columns = [
        column
        for column in [
            "warehouse_id",
            "warehouse_name",
            "region",
            "capacity_units",
        ]
        if column in warehouses.columns
    ]

    if warehouse_columns:
        metrics = metrics.merge(
            warehouses[warehouse_columns],
            on="warehouse_id",
            how="left",
        )

    save_csv(
        metrics.sort_values("total_orders", ascending=False),
        output_dir / "warehouse_workload_summary.csv",
    )


def analyze_weather(
    weather: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Summarize weather conditions and risk indicators."""
    weather = weather.copy()

    numeric_columns = [
        "temperature_c",
        "humidity_pct",
        "rainfall_mm",
    ]

    for column in numeric_columns:
        if column in weather.columns:
            weather[column] = pd.to_numeric(
                weather[column],
                errors="coerce",
            )

    if "weather_condition" in weather.columns:
        summary = (
            weather.groupby("weather_condition")
            .agg(
                observations=("weather_condition", "size"),
                average_temperature_c=("temperature_c", "mean"),
                average_humidity_pct=("humidity_pct", "mean"),
                average_rainfall_mm=("rainfall_mm", "mean"),
            )
            .reset_index()
            .sort_values("observations", ascending=False)
        )

        save_csv(summary, output_dir / "weather_summary.csv")

    if "rainfall_mm" in weather.columns:
        plt.figure(figsize=(10, 5))
        plt.hist(
            weather["rainfall_mm"].dropna(),
            bins=30,
        )
        plt.title("Rainfall Distribution")
        plt.xlabel("Rainfall (mm)")
        plt.ylabel("Observations")
        plt.tight_layout()
        plt.savefig(output_dir / "charts" / "rainfall_distribution.png", dpi=150)
        plt.close()


def analyze_traffic(
    traffic: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Summarize traffic conditions by location and traffic level."""
    traffic = traffic.copy()

    for column in [
        "traffic_score",
        "average_speed_kmph",
    ]:
        if column in traffic.columns:
            traffic[column] = pd.to_numeric(
                traffic[column],
                errors="coerce",
            )

    if "location" in traffic.columns:
        summary = (
            traffic.groupby("location")
            .agg(
                observations=("location", "size"),
                average_traffic_score=("traffic_score", "mean"),
                average_speed_kmph=("average_speed_kmph", "mean"),
            )
            .reset_index()
            .sort_values(
                "average_traffic_score",
                ascending=False,
            )
        )

        save_csv(summary, output_dir / "traffic_summary.csv")

    if "traffic_level" in traffic.columns:
        level = (
            traffic["traffic_level"]
            .value_counts(dropna=False)
            .rename_axis("traffic_level")
            .reset_index(name="observations")
        )

        save_csv(
            level,
            output_dir / "traffic_level_summary.csv",
        )


def analyze_gps(
    gps: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Summarize GPS movement, speed, signal quality, and vehicle activity."""
    gps = gps.copy()

    for column in ["speed_kmph", "signal_quality"]:
        if column in gps.columns:
            gps[column] = pd.to_numeric(
                gps[column],
                errors="coerce",
            )

    if "vehicle_id" not in gps.columns:
        return

    summary = (
        gps.groupby("vehicle_id")
        .agg(
            gps_observations=("vehicle_id", "size"),
            average_speed_kmph=("speed_kmph", "mean"),
            maximum_speed_kmph=("speed_kmph", "max"),
            average_signal_quality=("signal_quality", "mean"),
        )
        .reset_index()
        .sort_values("gps_observations", ascending=False)
    )

    if "vehicle_status" in gps.columns:
        active = (
            gps.assign(
                active_observation=gps["vehicle_status"]
                .astype(str)
                .str.lower()
                .eq("active")
                .astype(int)
            )
            .groupby("vehicle_id")
            .agg(active_observations=("active_observation", "sum"))
            .reset_index()
        )

        summary = summary.merge(
            active,
            on="vehicle_id",
            how="left",
        )

    save_csv(
        summary,
        output_dir / "gps_vehicle_summary.csv",
    )


def analyze_delivery_updates(
    updates: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Summarize operational delivery events and delay minutes."""
    updates = updates.copy()

    if "delay_minutes" in updates.columns:
        updates["delay_minutes"] = pd.to_numeric(
            updates["delay_minutes"],
            errors="coerce",
        )

    if "event_type" in updates.columns:
        event_summary = (
            updates.groupby("event_type")
            .agg(
                events=("event_type", "size"),
                average_delay_minutes=("delay_minutes", "mean"),
            )
            .reset_index()
            .sort_values("events", ascending=False)
        )

        save_csv(
            event_summary,
            output_dir / "delivery_event_summary.csv",
        )

    if "delivery_status" in updates.columns:
        status_summary = (
            updates["delivery_status"]
            .value_counts(dropna=False)
            .rename_axis("delivery_status")
            .reset_index(name="events")
        )

        save_csv(
            status_summary,
            output_dir / "delivery_update_status_summary.csv",
        )


def run_eda(data_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    charts_dir = output_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    datasets = load_datasets(data_dir)

    create_dataset_summary(datasets, output_dir)
    create_missing_values_report(datasets, output_dir)
    create_numeric_summary(datasets, output_dir)

    orders = datasets.get("orders")

    if orders is not None:
        analyze_orders(
            orders,
            output_dir,
            charts_dir,
        )

    analyze_customers(
        datasets["customers"]
        if "customers" in datasets
        else pd.DataFrame(),
        orders,
        output_dir,
    )

    if "drivers" in datasets:
        analyze_drivers(
            datasets["drivers"],
            orders,
            output_dir,
        )

    if "warehouses" in datasets:
        analyze_warehouses(
            datasets["warehouses"],
            orders,
            output_dir,
        )

    if "weather" in datasets:
        analyze_weather(
            datasets["weather"],
            output_dir,
        )

    if "traffic" in datasets:
        analyze_traffic(
            datasets["traffic"],
            output_dir,
        )

    if "gps_tracking" in datasets:
        analyze_gps(
            datasets["gps_tracking"],
            output_dir,
        )

    if "delivery_updates" in datasets:
        analyze_delivery_updates(
            datasets["delivery_updates"],
            output_dir,
        )

    print(f"\nEDA completed successfully.")
    print(f"Outputs written to: {output_dir.resolve()}")


def main() -> None:
    args = parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)

    run_eda(
        data_dir=data_dir,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()

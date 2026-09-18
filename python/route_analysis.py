"""
Route Analysis - Logistics & Supply Chain Intelligence Platform

Purpose:
    Analyze route/network characteristics using the project's GPS, traffic,
    roads, and order datasets.

Inputs:
    Preferred:
        data/processed/gps_clean.csv
        data/processed/orders_clean.csv

    Fallback:
        data/raw/gps_tracking.csv
        data/raw/orders.csv

    Additional:
        data/raw/traffic.csv
        data/raw/roads.csv

Outputs:
    analytics/output/routes/
        gps_route_summary.csv
        traffic_route_summary.csv
        road_network_summary.csv
        region_route_summary.csv
        delivery_distance_summary.csv
        route_risk_summary.csv
        route_analysis_summary.csv
        charts/...

Important modeling note:
    The current project data does not contain a reliable order_id-to-road_id
    or GPS-point-to-road-segment mapping. Therefore this script does NOT
    invent route assignments. It analyzes:
        1. GPS movement behavior,
        2. traffic conditions,
        3. available road-network characteristics,
        4. delivery distance,
        5. region-level operational risk.

    A production route-optimization system would add map matching or a
    routing engine to associate GPS traces/orders with actual road segments.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run route and logistics network analysis."
    )
    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Directory containing cleaned datasets.",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Directory containing raw datasets.",
    )
    parser.add_argument(
        "--output-dir",
        default="analytics/output/routes",
        help="Directory for route analysis outputs.",
    )
    return parser.parse_args()


def load_dataset(
    filename: str,
    processed_dir: Path,
    raw_dir: Path,
) -> pd.DataFrame | None:
    """Load processed data first, then raw data."""
    processed_path = processed_dir / filename
    raw_path = raw_dir / filename

    if processed_path.exists():
        print(f"[OK] Loading processed: {processed_path}")
        return pd.read_csv(processed_path)

    if raw_path.exists():
        print(f"[OK] Loading raw: {raw_path}")
        return pd.read_csv(raw_path)

    print(f"[WARN] Dataset not found: {filename}")
    return None


def load_named_dataset(
    processed_filename: str,
    raw_filename: str,
    processed_dir: Path,
    raw_dir: Path,
) -> pd.DataFrame | None:
    """Load a dataset where processed and raw filenames differ."""
    processed_path = processed_dir / processed_filename
    raw_path = raw_dir / raw_filename

    if processed_path.exists():
        print(f"[OK] Loading processed: {processed_path}")
        return pd.read_csv(processed_path)

    if raw_path.exists():
        print(f"[OK] Loading raw: {raw_path}")
        return pd.read_csv(raw_path)

    print(
        f"[WARN] Dataset not found: "
        f"{processed_filename} / {raw_filename}"
    )
    return None


def numeric(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Convert available columns to numeric."""
    result = df.copy()

    for column in columns:
        if column in result.columns:
            result[column] = pd.to_numeric(
                result[column],
                errors="coerce",
            )

    return result


def save_csv(
    df: pd.DataFrame,
    path: Path,
) -> None:
    """Save a DataFrame as CSV."""
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    df.to_csv(
        path,
        index=False,
    )


def analyze_gps(
    gps: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    """Analyze GPS observations and vehicle movement."""
    df = gps.copy()

    df = numeric(
        df,
        [
            "speed_kmph",
            "heading",
            "signal_quality",
            "latitude",
            "longitude",
        ],
    )

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
        )

    if "speed_kmph" in df.columns:
        df["is_moving"] = (
            df["speed_kmph"].fillna(0) > 3
        ).astype(int)

    if "signal_quality" in df.columns:
        df["is_low_signal"] = (
            df["signal_quality"].fillna(0) < 50
        ).astype(int)

    if "vehicle_id" not in df.columns:
        return pd.DataFrame()

    summary = (
        df.groupby("vehicle_id")
        .agg(
            gps_observations=("vehicle_id", "size"),
            average_speed_kmph=("speed_kmph", "mean"),
            maximum_speed_kmph=("speed_kmph", "max"),
            average_signal_quality=("signal_quality", "mean"),
            moving_observations=("is_moving", "sum"),
            low_signal_observations=("is_low_signal", "sum"),
        )
        .reset_index()
    )

    summary["moving_share_pct"] = round(
        summary["moving_observations"]
        / summary["gps_observations"]
        * 100,
        2,
    )

    summary["low_signal_share_pct"] = round(
        summary["low_signal_observations"]
        / summary["gps_observations"]
        * 100,
        2,
    )

    save_csv(
        summary.sort_values(
            "average_speed_kmph",
            ascending=False,
        ),
        output_dir / "gps_route_summary.csv",
    )

    if "speed_kmph" in df.columns:
        plt.figure(figsize=(9, 5))
        plt.hist(
            df["speed_kmph"].dropna(),
            bins=30,
        )
        plt.title("GPS Speed Distribution")
        plt.xlabel("Speed (km/h)")
        plt.ylabel("GPS Observations")
        plt.tight_layout()
        plt.savefig(
            output_dir / "charts" / "gps_speed_distribution.png",
            dpi=150,
        )
        plt.close()

    if "region" in df.columns:
        region = (
            df.groupby("region")
            .agg(
                gps_observations=("vehicle_id", "size"),
                average_speed_kmph=("speed_kmph", "mean"),
                average_signal_quality=("signal_quality", "mean"),
                moving_observations=("is_moving", "sum"),
            )
            .reset_index()
        )

        region["moving_share_pct"] = round(
            region["moving_observations"]
            / region["gps_observations"]
            * 100,
            2,
        )

        save_csv(
            region.sort_values(
                "average_speed_kmph",
                ascending=False,
            ),
            output_dir / "gps_region_summary.csv",
        )

    return summary


def analyze_traffic(
    traffic: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    """Analyze traffic severity by location."""
    df = traffic.copy()

    df = numeric(
        df,
        [
            "traffic_score",
            "average_speed_kmph",
            "latitude",
            "longitude",
        ],
    )

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
        )

    if "traffic_score" in df.columns:
        df["traffic_risk"] = pd.cut(
            df["traffic_score"],
            bins=[
                -np.inf,
                25,
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

    if "location" not in df.columns:
        return pd.DataFrame()

    aggregations = {
        "observations": ("location", "size"),
    }

    if "traffic_score" in df.columns:
        aggregations["average_traffic_score"] = (
            "traffic_score",
            "mean",
        )

    if "average_speed_kmph" in df.columns:
        aggregations["average_speed_kmph"] = (
            "average_speed_kmph",
            "mean",
        )

    summary = (
        df.groupby("location")
        .agg(**aggregations)
        .reset_index()
    )

    if "traffic_score" in df.columns:
        summary["traffic_level"] = pd.cut(
            summary["average_traffic_score"],
            bins=[
                -np.inf,
                25,
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

    save_csv(
        summary.sort_values(
            "average_traffic_score"
            if "average_traffic_score" in summary.columns
            else "observations",
            ascending=False,
        ),
        output_dir / "traffic_route_summary.csv",
    )

    if "traffic_score" in df.columns:
        plt.figure(figsize=(9, 5))
        plt.hist(
            df["traffic_score"].dropna(),
            bins=30,
        )
        plt.title("Traffic Score Distribution")
        plt.xlabel("Traffic Score")
        plt.ylabel("Observations")
        plt.tight_layout()
        plt.savefig(
            output_dir / "charts" / "traffic_score_distribution.png",
            dpi=150,
        )
        plt.close()

    return summary


def analyze_roads(
    roads: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    """Analyze the available road-network reference data."""
    df = roads.copy()

    df = numeric(
        df,
        [
            "distance_km",
            "lanes",
            "speed_limit_kmph",
        ],
    )

    if "road_id" not in df.columns:
        return pd.DataFrame()

    aggregations = {
        "road_segments": ("road_id", "nunique"),
    }

    if "distance_km" in df.columns:
        aggregations["total_distance_km"] = (
            "distance_km",
            "sum",
        )
        aggregations["average_distance_km"] = (
            "distance_km",
            "mean",
        )

    if "lanes" in df.columns:
        aggregations["average_lanes"] = (
            "lanes",
            "mean",
        )

    if "speed_limit_kmph" in df.columns:
        aggregations["average_speed_limit_kmph"] = (
            "speed_limit_kmph",
            "mean",
        )

    if "road_condition" in df.columns:
        condition_summary = (
            df.groupby("road_condition")
            .agg(
                road_segments=("road_id", "nunique"),
                total_distance_km=("distance_km", "sum"),
                average_speed_limit_kmph=(
                    "speed_limit_kmph",
                    "mean",
                ),
            )
            .reset_index()
            .sort_values(
                "road_segments",
                ascending=False,
            )
        )

        save_csv(
            condition_summary,
            output_dir / "road_condition_summary.csv",
        )

    if "road_type" in df.columns:
        type_summary = (
            df.groupby("road_type")
            .agg(
                road_segments=("road_id", "nunique"),
                total_distance_km=("distance_km", "sum"),
                average_distance_km=("distance_km", "mean"),
                average_speed_limit_kmph=(
                    "speed_limit_kmph",
                    "mean",
                ),
            )
            .reset_index()
            .sort_values(
                "road_segments",
                ascending=False,
            )
        )

        save_csv(
            type_summary,
            output_dir / "road_network_summary.csv",
        )

    summary = (
        df.agg(
            road_segments=("road_id", "nunique"),
            total_distance_km=("distance_km", "sum"),
            average_distance_km=("distance_km", "mean"),
            average_lanes=("lanes", "mean"),
            average_speed_limit_kmph=(
                "speed_limit_kmph",
                "mean",
            ),
        )
        .to_frame()
        .T
    )

    save_csv(
        summary,
        output_dir / "road_network_overview.csv",
    )

    return df


def analyze_delivery_distance(
    orders: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    """Analyze actual delivery-distance characteristics."""
    df = orders.copy()

    df = numeric(
        df,
        [
            "delivery_distance_km",
            "order_value",
            "promised_delivery_time_min",
            "actual_delivery_time_min",
        ],
    )

    if "delivery_distance_km" not in df.columns:
        return pd.DataFrame()

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

    aggregations = {
        "orders": ("order_id", "count"),
        "average_distance_km": (
            "delivery_distance_km",
            "mean",
        ),
    }

    if "order_value" in df.columns:
        aggregations["total_order_value"] = (
            "order_value",
            "sum",
        )
        aggregations["average_order_value"] = (
            "order_value",
            "mean",
        )

    if {
        "actual_delivery_time_min",
        "promised_delivery_time_min",
    }.issubset(df.columns):
        df["delay_minutes"] = (
            df["actual_delivery_time_min"]
            - df["promised_delivery_time_min"]
        )

        aggregations["average_delay_min"] = (
            "delay_minutes",
            "mean",
        )

        aggregations["delayed_orders"] = (
            "delay_minutes",
            lambda series: int((series > 0).sum()),
        )

    summary = (
        df.groupby("distance_band", observed=False)
        .agg(**aggregations)
        .reset_index()
    )

    if "delayed_orders" in summary.columns:
        summary["delay_rate_pct"] = round(
            summary["delayed_orders"]
            / summary["orders"]
            * 100,
            2,
        )

    save_csv(
        summary,
        output_dir / "delivery_distance_summary.csv",
    )

    if {
        "delivery_distance_km",
        "delay_minutes",
    }.issubset(df.columns):
        plot_df = df[
            [
                "delivery_distance_km",
                "delay_minutes",
            ]
        ].dropna()

        if not plot_df.empty:
            plt.figure(figsize=(9, 6))
            plt.scatter(
                plot_df["delivery_distance_km"],
                plot_df["delay_minutes"],
                alpha=0.35,
            )
            plt.axhline(
                0,
                linewidth=1,
            )
            plt.title("Delivery Distance vs Delay")
            plt.xlabel("Delivery Distance (km)")
            plt.ylabel("Delay (minutes)")
            plt.tight_layout()
            plt.savefig(
                output_dir / "charts" / "distance_vs_delay.png",
                dpi=150,
            )
            plt.close()

    return summary


def build_region_route_risk(
    gps: pd.DataFrame | None,
    traffic: pd.DataFrame | None,
    orders: pd.DataFrame | None,
    output_dir: Path,
) -> pd.DataFrame:
    """
    Build a region-level route risk view.

    This combines independently observed signals by region. It does not
    pretend that an individual GPS observation belongs to a particular order.
    """
    region_frames: list[pd.DataFrame] = []

    if gps is not None and "region" in gps.columns:
        gps_df = numeric(
            gps,
            [
                "speed_kmph",
                "signal_quality",
            ],
        )

        gps_region = (
            gps_df.groupby("region")
            .agg(
                gps_observations=("vehicle_id", "size"),
                average_gps_speed_kmph=(
                    "speed_kmph",
                    "mean",
                ),
                average_signal_quality=(
                    "signal_quality",
                    "mean",
                ),
            )
            .reset_index()
        )

        region_frames.append(gps_region)

    if traffic is not None and "location" in traffic.columns:
        traffic_df = numeric(
            traffic,
            [
                "traffic_score",
                "average_speed_kmph",
            ],
        )

        traffic_region = (
            traffic_df.groupby("location")
            .agg(
                traffic_observations=("location", "size"),
                average_traffic_score=(
                    "traffic_score",
                    "mean",
                ),
                average_traffic_speed_kmph=(
                    "average_speed_kmph",
                    "mean",
                ),
            )
            .reset_index()
            .rename(columns={"location": "region"})
        )

        region_frames.append(traffic_region)

    if orders is not None and "region" in orders.columns:
        order_df = orders.copy()

        order_df = numeric(
            order_df,
            [
                "delivery_distance_km",
                "actual_delivery_time_min",
                "promised_delivery_time_min",
            ],
        )

        aggregations = {
            "orders": ("order_id", "count"),
            "average_delivery_distance_km": (
                "delivery_distance_km",
                "mean",
            ),
        }

        if {
            "actual_delivery_time_min",
            "promised_delivery_time_min",
        }.issubset(order_df.columns):
            order_df["delay_minutes"] = (
                order_df["actual_delivery_time_min"]
                - order_df["promised_delivery_time_min"]
            )

            aggregations["average_delivery_delay_min"] = (
                "delay_minutes",
                "mean",
            )

            aggregations["delayed_orders"] = (
                "delay_minutes",
                lambda series: int((series > 0).sum()),
            )

        order_region = (
            order_df.groupby("region")
            .agg(**aggregations)
            .reset_index()
        )

        if "delayed_orders" in order_region.columns:
            order_region["delay_rate_pct"] = round(
                order_region["delayed_orders"]
                / order_region["orders"]
                * 100,
                2,
            )

        region_frames.append(order_region)

    if not region_frames:
        return pd.DataFrame()

    result = region_frames[0]

    for frame in region_frames[1:]:
        result = result.merge(
            frame,
            on="region",
            how="outer",
        )

    # A transparent composite risk score using only available regional
    # indicators. The score is descriptive, not a trained ML risk model.
    result["route_risk_score"] = 0.0

    if "average_traffic_score" in result.columns:
        result["route_risk_score"] += (
            result["average_traffic_score"]
            .fillna(result["average_traffic_score"].median())
            .clip(0, 100)
            * 0.50
        )

    if "average_delivery_delay_min" in result.columns:
        delay_component = (
            result["average_delivery_delay_min"]
            .fillna(0)
            .clip(0, 60)
            / 60
            * 100
        )

        result["route_risk_score"] += (
            delay_component * 0.30
        )

    if "average_signal_quality" in result.columns:
        signal_component = (
            100
            - result["average_signal_quality"]
            .fillna(100)
            .clip(0, 100)
        )

        result["route_risk_score"] += (
            signal_component * 0.20
        )

    result["route_risk_score"] = (
        result["route_risk_score"]
        .clip(0, 100)
        .round(2)
    )

    result["route_risk_category"] = pd.cut(
        result["route_risk_score"],
        bins=[
            -np.inf,
            25,
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

    save_csv(
        result.sort_values(
            "route_risk_score",
            ascending=False,
        ),
        output_dir / "route_risk_summary.csv",
    )

    return result


def create_summary(
    gps_summary: pd.DataFrame,
    traffic_summary: pd.DataFrame,
    roads: pd.DataFrame | None,
    orders: pd.DataFrame | None,
) -> pd.DataFrame:
    """Create a high-level route-analysis summary."""
    row: dict[str, object] = {
        "vehicles_analyzed": (
            int(gps_summary["vehicle_id"].nunique())
            if not gps_summary.empty
            and "vehicle_id" in gps_summary.columns
            else 0
        ),
        "gps_observations": (
            int(gps_summary["gps_observations"].sum())
            if not gps_summary.empty
            else 0
        ),
        "traffic_locations_analyzed": (
            int(traffic_summary["location"].nunique())
            if not traffic_summary.empty
            and "location" in traffic_summary.columns
            else 0
        ),
        "road_segments_available": (
            int(roads["road_id"].nunique())
            if roads is not None
            and "road_id" in roads.columns
            else 0
        ),
        "orders_analyzed": (
            int(len(orders))
            if orders is not None
            else 0
        ),
    }

    return pd.DataFrame([row])


def run_route_analysis(
    processed_dir: Path,
    raw_dir: Path,
    output_dir: Path,
) -> None:
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    (output_dir / "charts").mkdir(
        parents=True,
        exist_ok=True,
    )

    print("[1/5] Loading route-related datasets...")

    gps = load_named_dataset(
        "gps_clean.csv",
        "gps_tracking.csv",
        processed_dir,
        raw_dir,
    )

    traffic = load_dataset(
        "traffic.csv",
        processed_dir,
        raw_dir,
    )

    roads = load_dataset(
        "roads.csv",
        processed_dir,
        raw_dir,
    )

    orders = load_named_dataset(
        "orders_clean.csv",
        "orders.csv",
        processed_dir,
        raw_dir,
    )

    if gps is None and traffic is None and roads is None and orders is None:
        raise FileNotFoundError(
            "No route-related datasets were found."
        )

    print("[2/5] Analyzing GPS movement...")
    if gps is not None:
        gps_summary = analyze_gps(
            gps,
            output_dir,
        )
    else:
        gps_summary = pd.DataFrame()

    print("[3/5] Analyzing traffic and road network...")
    if traffic is not None:
        traffic_summary = analyze_traffic(
            traffic,
            output_dir,
        )
    else:
        traffic_summary = pd.DataFrame()

    if roads is not None:
        analyze_roads(
            roads,
            output_dir,
        )

    print("[4/5] Analyzing delivery distance...")
    if orders is not None:
        analyze_delivery_distance(
            orders,
            output_dir,
        )

    region_risk = build_region_route_risk(
        gps,
        traffic,
        orders,
        output_dir,
    )

    print("[5/5] Creating route analysis summary...")
    summary = create_summary(
        gps_summary,
        traffic_summary,
        roads,
        orders,
    )

    save_csv(
        summary,
        output_dir / "route_analysis_summary.csv",
    )

    if not region_risk.empty:
        save_csv(
            region_risk,
            output_dir / "region_route_summary.csv",
        )

    print("\nRoute analysis completed successfully.")
    print(
        f"Outputs written to: {output_dir.resolve()}"
    )


def main() -> None:
    args = parse_args()

    run_route_analysis(
        processed_dir=Path(args.processed_dir),
        raw_dir=Path(args.raw_dir),
        output_dir=Path(args.output_dir),
    )


if __name__ == "__main__":
    main()

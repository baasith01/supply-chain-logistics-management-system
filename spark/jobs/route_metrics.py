"""
Route Metrics Job
-----------------
Purpose:
    Calculate route and movement intelligence from GPS telemetry, traffic
    observations, and delivery/order data.

Inputs:
    data/processed/delivery_features.csv
    data/processed/gps_clean.csv
    data/raw/traffic.csv
    data/raw/roads.csv

Outputs:
    data/processed/route_metrics.csv
    data/processed/route_region_metrics.csv

Important modeling note:
    The current order dataset does not contain a route_id or GPS event_id.
    Therefore this job does NOT invent an order-to-road mapping. Delivery
    metrics and GPS/traffic metrics are calculated independently and then
    summarized by region where a reliable geographic relationship exists.

Run:
    spark-submit spark/jobs/route_metrics.py

Optional environment variables:
    DELIVERY_FEATURES_PATH
    GPS_PATH
    TRAFFIC_PATH
    ROADS_PATH
    OUTPUT_PATH
"""

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DELIVERY_REQUIRED_COLUMNS = {
    "order_id",
    "region",
    "delivery_distance_km",
    "actual_delivery_time_min",
    "delivery_delay_min",
    "is_delayed",
}

GPS_REQUIRED_COLUMNS = {
    "vehicle_id",
    "timestamp",
    "region",
    "latitude",
    "longitude",
    "speed_kmph",
    "is_moving",
    "is_low_signal",
}

TRAFFIC_REQUIRED_COLUMNS = {
    "timestamp",
    "location",
    "traffic_score",
    "traffic_level",
    "average_speed_kmph",
}

ROADS_REQUIRED_COLUMNS = {
    "road_id",
    "start_location",
    "end_location",
    "road_type",
    "distance_km",
    "lanes",
    "speed_limit_kmph",
    "road_condition",
}


def create_spark_session() -> SparkSession:
    """Create and return the Spark session."""
    return (
        SparkSession.builder
        .appName("LogisticsRouteMetrics")
        .getOrCreate()
    )


def validate_columns(df, required_columns, dataset_name: str) -> None:
    """Fail fast when required columns are missing."""
    missing = required_columns.difference(df.columns)

    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{sorted(missing)}"
        )


def calculate_gps_metrics(gps):
    """Calculate vehicle movement and telemetry KPIs by region."""

    return (
        gps.groupBy("region")
        .agg(
            F.count("*").alias("gps_events"),

            F.countDistinct("vehicle_id")
            .alias("tracked_vehicles"),

            F.round(F.avg("speed_kmph"), 2)
            .alias("avg_speed_kmph"),

            F.round(
                F.avg(
                    F.when(
                        F.col("is_moving") == 1,
                        F.col("speed_kmph")
                    )
                ),
                2
            ).alias("avg_moving_speed_kmph"),

            F.round(F.max("speed_kmph"), 2)
            .alias("max_speed_kmph"),

            F.sum(
                F.when(F.col("is_moving") == 1, 1).otherwise(0)
            ).alias("moving_events"),

            F.sum(
                F.when(F.col("is_low_signal") == 1, 1).otherwise(0)
            ).alias("low_signal_events"),
        )
        .withColumn(
            "moving_event_rate_pct",
            F.round(
                F.when(
                    F.col("gps_events") > 0,
                    F.col("moving_events")
                    / F.col("gps_events") * 100
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "low_signal_rate_pct",
            F.round(
                F.when(
                    F.col("gps_events") > 0,
                    F.col("low_signal_events")
                    / F.col("gps_events") * 100
                ).otherwise(0),
                2
            )
        )
    )


def calculate_traffic_metrics(traffic):
    """Calculate traffic and congestion KPIs by location."""

    return (
        traffic.groupBy("location")
        .agg(
            F.count("*").alias("traffic_events"),

            F.round(F.avg("traffic_score"), 2)
            .alias("avg_traffic_score"),

            F.round(F.max("traffic_score"), 2)
            .alias("max_traffic_score"),

            F.round(F.avg("average_speed_kmph"), 2)
            .alias("avg_traffic_speed_kmph"),

            F.sum(
                F.when(
                    F.upper(F.col("traffic_level"))
                    .isin("HIGH", "SEVERE"),
                    1
                ).otherwise(0)
            ).alias("congested_events"),

            F.sum(
                F.when(
                    F.upper(F.col("traffic_level")) == "SEVERE",
                    1
                ).otherwise(0)
            ).alias("severe_congestion_events"),
        )
        .withColumn(
            "congestion_rate_pct",
            F.round(
                F.when(
                    F.col("traffic_events") > 0,
                    F.col("congested_events")
                    / F.col("traffic_events") * 100
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "severe_congestion_rate_pct",
            F.round(
                F.when(
                    F.col("traffic_events") > 0,
                    F.col("severe_congestion_events")
                    / F.col("traffic_events") * 100
                ).otherwise(0),
                2
            )
        )
        .orderBy(F.desc("avg_traffic_score"))
    )


def calculate_road_metrics(roads):
    """Calculate static road-network characteristics."""

    return (
        roads.groupBy("road_type")
        .agg(
            F.countDistinct("road_id")
            .alias("road_count"),

            F.round(F.sum("distance_km"), 2)
            .alias("total_road_distance_km"),

            F.round(F.avg("distance_km"), 2)
            .alias("avg_road_distance_km"),

            F.round(F.avg("lanes"), 2)
            .alias("avg_lanes"),

            F.round(F.avg("speed_limit_kmph"), 2)
            .alias("avg_speed_limit_kmph"),

            F.sum(
                F.when(
                    F.upper(F.col("road_condition"))
                    .isin("POOR", "CRITICAL"),
                    1
                ).otherwise(0)
            ).alias("poor_condition_roads"),
        )
        .withColumn(
            "poor_condition_rate_pct",
            F.round(
                F.when(
                    F.col("road_count") > 0,
                    F.col("poor_condition_roads")
                    / F.col("road_count") * 100
                ).otherwise(0),
                2
            )
        )
        .orderBy(F.desc("total_road_distance_km"))
    )


def calculate_delivery_route_metrics(deliveries):
    """
    Calculate route-related delivery KPIs by region.

    These are delivery-distance metrics, not GPS-derived route paths.
    """

    return (
        deliveries.groupBy("region")
        .agg(
            F.countDistinct("order_id")
            .alias("delivery_orders"),

            F.round(F.avg("delivery_distance_km"), 2)
            .alias("avg_delivery_distance_km"),

            F.round(F.max("delivery_distance_km"), 2)
            .alias("max_delivery_distance_km"),

            F.round(F.sum("delivery_distance_km"), 2)
            .alias("total_delivery_distance_km"),

            F.round(F.avg("actual_delivery_time_min"), 2)
            .alias("avg_delivery_time_min"),

            F.round(F.avg("delivery_delay_min"), 2)
            .alias("avg_delivery_delay_min"),

            F.countDistinct(
                F.when(
                    F.col("is_delayed") == 1,
                    F.col("order_id")
                )
            ).alias("delayed_orders"),
        )
        .withColumn(
            "delay_rate_pct",
            F.round(
                F.when(
                    F.col("delivery_orders") > 0,
                    F.col("delayed_orders")
                    / F.col("delivery_orders") * 100
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "avg_delivery_minutes_per_km",
            F.round(
                F.when(
                    F.col("avg_delivery_distance_km") > 0,
                    F.col("avg_delivery_time_min")
                    / F.col("avg_delivery_distance_km")
                ).otherwise(0),
                2
            )
        )
    )


def calculate_region_metrics(deliveries, gps, traffic):
    """
    Combine independently reliable regional delivery, GPS, and traffic
    metrics. Traffic location is normalized to the same region naming
    convention where possible.
    """

    delivery_region = calculate_delivery_route_metrics(deliveries)

    gps_region = calculate_gps_metrics(gps)

    traffic_region = (
        traffic.groupBy("location")
        .agg(
            F.count("*").alias("traffic_events"),
            F.round(F.avg("traffic_score"), 2)
            .alias("avg_traffic_score"),
            F.round(F.avg("average_speed_kmph"), 2)
            .alias("avg_traffic_speed_kmph"),
            F.sum(
                F.when(
                    F.upper(F.col("traffic_level"))
                    .isin("HIGH", "SEVERE"),
                    1
                ).otherwise(0)
            ).alias("congested_events"),
        )
        .withColumnRenamed("location", "region")
        .withColumn(
            "congestion_rate_pct",
            F.round(
                F.when(
                    F.col("traffic_events") > 0,
                    F.col("congested_events")
                    / F.col("traffic_events") * 100
                ).otherwise(0),
                2
            )
        )
    )

    result = (
        delivery_region
        .join(gps_region, on="region", how="left")
        .join(traffic_region, on="region", how="left")
        .fillna(
            {
                "gps_events": 0,
                "tracked_vehicles": 0,
                "avg_speed_kmph": 0.0,
                "avg_moving_speed_kmph": 0.0,
                "max_speed_kmph": 0.0,
                "moving_events": 0,
                "low_signal_events": 0,
                "moving_event_rate_pct": 0.0,
                "low_signal_rate_pct": 0.0,
                "traffic_events": 0,
                "avg_traffic_score": 0.0,
                "avg_traffic_speed_kmph": 0.0,
                "congested_events": 0,
                "congestion_rate_pct": 0.0,
            }
        )
        .withColumn(
            "route_risk",
            F.when(
                (F.col("delay_rate_pct") >= 30)
                | (F.col("congestion_rate_pct") >= 50)
                | (F.col("low_signal_rate_pct") >= 20),
                "High"
            )
            .when(
                (F.col("delay_rate_pct") >= 15)
                | (F.col("congestion_rate_pct") >= 25)
                | (F.col("low_signal_rate_pct") >= 10),
                "Medium"
            )
            .otherwise("Low")
        )
        .orderBy(F.desc("delivery_orders"))
    )

    return result


def write_single_csv(df, output_path: str, temp_name: str) -> None:
    """Write a Spark dataframe as one CSV file."""

    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)

    temp_dir = os.path.join(output_dir, temp_name)

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(temp_dir)
    )

    part_files = [
        name
        for name in os.listdir(temp_dir)
        if name.startswith("part-") and name.endswith(".csv")
    ]

    if not part_files:
        raise RuntimeError(
            f"No Spark CSV part file generated in {temp_dir}"
        )

    part_path = os.path.join(temp_dir, part_files[0])

    if os.path.exists(output_path):
        os.remove(output_path)

    shutil.move(part_path, output_path)
    shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> None:
    delivery_path = os.getenv(
        "DELIVERY_FEATURES_PATH",
        "data/processed/delivery_features.csv"
    )
    gps_path = os.getenv(
        "GPS_PATH",
        "data/processed/gps_clean.csv"
    )
    traffic_path = os.getenv(
        "TRAFFIC_PATH",
        "data/raw/traffic.csv"
    )
    roads_path = os.getenv(
        "ROADS_PATH",
        "data/raw/roads.csv"
    )
    output_path = os.getenv(
        "OUTPUT_PATH",
        "data/processed/route_metrics.csv"
    )

    spark = create_spark_session()

    try:
        deliveries = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(delivery_path)
        )

        gps = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(gps_path)
        )

        traffic = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(traffic_path)
        )

        roads = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(roads_path)
        )

        validate_columns(
            deliveries,
            DELIVERY_REQUIRED_COLUMNS,
            "Delivery features"
        )
        validate_columns(
            gps,
            GPS_REQUIRED_COLUMNS,
            "GPS data"
        )
        validate_columns(
            traffic,
            TRAFFIC_REQUIRED_COLUMNS,
            "Traffic data"
        )
        validate_columns(
            roads,
            ROADS_REQUIRED_COLUMNS,
            "Road data"
        )

        deliveries = (
            deliveries
            .withColumn("order_id", F.trim(F.col("order_id")))
            .withColumn("region", F.upper(F.trim(F.col("region"))))
            .withColumn(
                "delivery_distance_km",
                F.col("delivery_distance_km").cast("double")
            )
            .withColumn(
                "actual_delivery_time_min",
                F.col("actual_delivery_time_min").cast("double")
            )
            .withColumn(
                "delivery_delay_min",
                F.col("delivery_delay_min").cast("double")
            )
            .withColumn(
                "is_delayed",
                F.col("is_delayed").cast("int")
            )
            .filter(F.col("order_id").isNotNull())
        )

        gps = (
            gps
            .withColumn("vehicle_id", F.trim(F.col("vehicle_id")))
            .withColumn("region", F.upper(F.trim(F.col("region"))))
            .withColumn(
                "timestamp",
                F.to_timestamp("timestamp")
            )
            .withColumn(
                "speed_kmph",
                F.col("speed_kmph").cast("double")
            )
            .withColumn(
                "is_moving",
                F.col("is_moving").cast("int")
            )
            .withColumn(
                "is_low_signal",
                F.col("is_low_signal").cast("int")
            )
            .filter(
                F.col("vehicle_id").isNotNull()
                & F.col("timestamp").isNotNull()
            )
        )

        traffic = (
            traffic
            .withColumn(
                "location",
                F.upper(F.trim(F.col("location")))
            )
            .withColumn(
                "timestamp",
                F.to_timestamp("timestamp")
            )
            .withColumn(
                "traffic_score",
                F.col("traffic_score").cast("double")
            )
            .withColumn(
                "average_speed_kmph",
                F.col("average_speed_kmph").cast("double")
            )
            .withColumn(
                "traffic_level",
                F.upper(F.trim(F.col("traffic_level")))
            )
            .filter(F.col("location").isNotNull())
        )

        roads = (
            roads
            .withColumn(
                "road_id",
                F.trim(F.col("road_id"))
            )
            .withColumn(
                "road_type",
                F.upper(F.trim(F.col("road_type")))
            )
            .withColumn(
                "road_condition",
                F.upper(F.trim(F.col("road_condition")))
            )
            .withColumn(
                "distance_km",
                F.col("distance_km").cast("double")
            )
            .withColumn(
                "lanes",
                F.col("lanes").cast("double")
            )
            .withColumn(
                "speed_limit_kmph",
                F.col("speed_limit_kmph").cast("double")
            )
            .dropDuplicates(["road_id"])
        )

        region_metrics = calculate_region_metrics(
            deliveries,
            gps,
            traffic
        )

        traffic_metrics = calculate_traffic_metrics(traffic)

        road_metrics = calculate_road_metrics(roads)

        write_single_csv(
            region_metrics,
            output_path,
            ".route_metrics_tmp"
        )

        region_output_path = output_path.replace(
            "route_metrics.csv",
            "route_region_metrics.csv"
        )

        # The region output is the primary combined route intelligence.
        # Keep it separate from static road-type metrics for clarity.
        write_single_csv(
            region_metrics,
            region_output_path,
            ".route_region_metrics_tmp"
        )

        traffic_output_path = output_path.replace(
            "route_metrics.csv",
            "traffic_metrics.csv"
        )

        road_output_path = output_path.replace(
            "route_metrics.csv",
            "road_type_metrics.csv"
        )

        write_single_csv(
            traffic_metrics,
            traffic_output_path,
            ".traffic_metrics_tmp"
        )

        write_single_csv(
            road_metrics,
            road_output_path,
            ".road_type_metrics_tmp"
        )

        print(f"Route metrics written to: {output_path}")
        print(
            f"Region route metrics written to: "
            f"{region_output_path}"
        )
        print(
            f"Traffic metrics written to: "
            f"{traffic_output_path}"
        )
        print(
            f"Road type metrics written to: "
            f"{road_output_path}"
        )
        print(f"Region rows: {region_metrics.count()}")
        print(f"Traffic rows: {traffic_metrics.count()}")
        print(f"Road type rows: {road_metrics.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

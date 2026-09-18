"""
Driver Metrics Job
------------------
Purpose:
    Calculate driver-level performance KPIs from the processed delivery
    features and driver master data.

Inputs:
    data/processed/delivery_features.csv
    data/raw/drivers.csv

Output:
    data/processed/driver_metrics.csv

Run:
    spark-submit spark/jobs/driver_metrics.py

Optional environment variables:
    DELIVERY_FEATURES_PATH
    DRIVERS_PATH
    OUTPUT_PATH
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DELIVERY_REQUIRED_COLUMNS = {
    "order_id",
    "driver_id",
    "region",
    "delivery_status",
    "order_date",
    "order_value",
    "delivery_distance_km",
    "actual_delivery_time_min",
    "delivery_delay_min",
    "is_delayed",
}

DRIVER_REQUIRED_COLUMNS = {
    "driver_id",
    "driver_name",
    "region",
    "experience_years",
    "rating",
    "employment_status",
    "license_type",
}


def create_spark_session() -> SparkSession:
    """Create and return the Spark session."""
    return (
        SparkSession.builder
        .appName("LogisticsDriverMetrics")
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


def calculate_driver_metrics(deliveries, drivers):
    """Calculate driver-level operational and business KPIs."""

    driver_delivery = (
        deliveries
        .groupBy("driver_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),

            F.countDistinct(
                F.when(
                    F.upper(F.col("delivery_status")) == "DELIVERED",
                    F.col("order_id")
                )
            ).alias("delivered_orders"),

            F.countDistinct(
                F.when(
                    F.col("is_delayed") == 1,
                    F.col("order_id")
                )
            ).alias("delayed_orders"),

            F.round(F.avg("delivery_delay_min"), 2)
            .alias("avg_delay_min"),

            F.round(
                F.avg(
                    F.when(
                        F.col("delivery_delay_min") > 0,
                        F.col("delivery_delay_min")
                    )
                ),
                2
            ).alias("avg_positive_delay_min"),

            F.round(F.avg("actual_delivery_time_min"), 2)
            .alias("avg_delivery_time_min"),

            F.round(F.sum("delivery_distance_km"), 2)
            .alias("total_distance_km"),

            F.round(F.avg("delivery_distance_km"), 2)
            .alias("avg_distance_km"),

            F.round(F.sum("order_value"), 2)
            .alias("total_order_value"),

            F.round(F.avg("order_value"), 2)
            .alias("avg_order_value"),
        )
    )

    result = (
        drivers
        .join(driver_delivery, on="driver_id", how="left")
        .fillna(
            {
                "total_orders": 0,
                "delivered_orders": 0,
                "delayed_orders": 0,
                "avg_delay_min": 0.0,
                "avg_positive_delay_min": 0.0,
                "avg_delivery_time_min": 0.0,
                "total_distance_km": 0.0,
                "avg_distance_km": 0.0,
                "total_order_value": 0.0,
                "avg_order_value": 0.0,
            }
        )
        .withColumn(
            "on_time_orders",
            F.col("delivered_orders") - F.col("delayed_orders")
        )
        .withColumn(
            "on_time_delivery_rate_pct",
            F.round(
                F.when(
                    F.col("delivered_orders") > 0,
                    F.col("on_time_orders")
                    / F.col("delivered_orders") * 100
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "delay_rate_pct",
            F.round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("delayed_orders")
                    / F.col("total_orders") * 100
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "orders_per_experience_year",
            F.round(
                F.when(
                    F.col("experience_years") > 0,
                    F.col("total_orders")
                    / F.col("experience_years")
                ).otherwise(F.col("total_orders")),
                2
            )
        )
        .withColumn(
            "revenue_per_km",
            F.round(
                F.when(
                    F.col("total_distance_km") > 0,
                    F.col("total_order_value")
                    / F.col("total_distance_km")
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "performance_category",
            F.when(
                (F.col("delivered_orders") >= 20)
                & (F.col("on_time_delivery_rate_pct") >= 90)
                & (F.col("rating") >= 4.5),
                "Top Performer"
            )
            .when(
                (F.col("delivered_orders") >= 10)
                & (F.col("on_time_delivery_rate_pct") >= 80)
                & (F.col("rating") >= 4.0),
                "Strong Performer"
            )
            .when(
                (F.col("total_orders") == 0),
                "No Delivery Data"
            )
            .when(
                (F.col("delay_rate_pct") >= 30)
                | (F.col("rating") < 3.5),
                "Needs Attention"
            )
            .otherwise("Average")
        )
        .withColumn(
            "risk_flag",
            F.when(
                (F.col("delay_rate_pct") >= 30)
                | (F.col("rating") < 3.5),
                "High"
            )
            .when(
                (F.col("delay_rate_pct") >= 15)
                | (F.col("rating") < 4.0),
                "Medium"
            )
            .otherwise("Low")
        )
        .select(
            "driver_id",
            "driver_name",
            "region",
            "experience_years",
            "rating",
            "employment_status",
            "license_type",
            "total_orders",
            "delivered_orders",
            "delayed_orders",
            "on_time_orders",
            "on_time_delivery_rate_pct",
            "delay_rate_pct",
            "avg_delay_min",
            "avg_positive_delay_min",
            "avg_delivery_time_min",
            "total_distance_km",
            "avg_distance_km",
            "total_order_value",
            "avg_order_value",
            "revenue_per_km",
            "orders_per_experience_year",
            "performance_category",
            "risk_flag",
        )
        .orderBy(
            F.desc("on_time_delivery_rate_pct"),
            F.desc("rating")
        )
    )

    return result


def calculate_region_driver_metrics(deliveries):
    """Calculate aggregated driver KPIs by region."""

    return (
        deliveries
        .groupBy("region")
        .agg(
            F.countDistinct("driver_id").alias("active_drivers"),
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct(
                F.when(
                    F.col("is_delayed") == 1,
                    F.col("order_id")
                )
            ).alias("delayed_orders"),
            F.round(F.avg("delivery_delay_min"), 2)
            .alias("avg_delay_min"),
            F.round(F.avg("actual_delivery_time_min"), 2)
            .alias("avg_delivery_time_min"),
            F.round(F.sum("order_value"), 2)
            .alias("total_order_value"),
        )
        .withColumn(
            "delay_rate_pct",
            F.round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("delayed_orders")
                    / F.col("total_orders") * 100
                ).otherwise(0),
                2
            )
        )
        .orderBy(F.desc("total_orders"))
    )


def write_single_csv(df, output_path: str) -> None:
    """Write a Spark dataframe as one CSV file."""

    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)

    temp_dir = os.path.join(
        output_dir,
        ".driver_metrics_tmp"
    )

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
    drivers_path = os.getenv(
        "DRIVERS_PATH",
        "data/raw/drivers.csv"
    )
    output_path = os.getenv(
        "OUTPUT_PATH",
        "data/processed/driver_metrics.csv"
    )

    spark = create_spark_session()

    try:
        deliveries = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(delivery_path)
        )

        drivers = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(drivers_path)
        )

        validate_columns(
            deliveries,
            DELIVERY_REQUIRED_COLUMNS,
            "Delivery features"
        )
        validate_columns(
            drivers,
            DRIVER_REQUIRED_COLUMNS,
            "Drivers"
        )

        deliveries = (
            deliveries
            .withColumn("driver_id", F.trim(F.col("driver_id")))
            .withColumn("region", F.upper(F.trim(F.col("region"))))
            .withColumn(
                "delivery_status",
                F.upper(F.trim(F.col("delivery_status")))
            )
            .withColumn(
                "is_delayed",
                F.col("is_delayed").cast("int")
            )
            .withColumn(
                "delivery_delay_min",
                F.col("delivery_delay_min").cast("double")
            )
            .withColumn(
                "actual_delivery_time_min",
                F.col("actual_delivery_time_min").cast("double")
            )
            .withColumn(
                "delivery_distance_km",
                F.col("delivery_distance_km").cast("double")
            )
            .withColumn(
                "order_value",
                F.col("order_value").cast("double")
            )
            .filter(
                F.col("order_id").isNotNull()
                & F.col("driver_id").isNotNull()
            )
        )

        drivers = (
            drivers
            .withColumn("driver_id", F.trim(F.col("driver_id")))
            .withColumn("region", F.upper(F.trim(F.col("region"))))
            .withColumn(
                "experience_years",
                F.col("experience_years").cast("double")
            )
            .withColumn(
                "rating",
                F.col("rating").cast("double")
            )
            .dropDuplicates(["driver_id"])
        )

        driver_metrics = calculate_driver_metrics(
            deliveries,
            drivers
        )

        region_metrics = calculate_region_driver_metrics(
            deliveries
        )

        write_single_csv(driver_metrics, output_path)

        region_output_path = output_path.replace(
            "driver_metrics.csv",
            "driver_region_metrics.csv"
        )
        write_single_csv(region_metrics, region_output_path)

        print(f"Driver metrics written to: {output_path}")
        print(
            f"Region driver metrics written to: "
            f"{region_output_path}"
        )
        print(f"Driver rows: {driver_metrics.count()}")
        print(f"Region rows: {region_metrics.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

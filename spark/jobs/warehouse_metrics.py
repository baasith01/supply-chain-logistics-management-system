"""
Warehouse Metrics Job
---------------------
Purpose:
    Calculate warehouse-level operational and business KPIs by combining
    warehouse master data with processed delivery/order features.

Inputs:
    data/processed/delivery_features.csv
    data/raw/warehouses.csv

Outputs:
    data/processed/warehouse_metrics.csv
    data/processed/warehouse_region_metrics.csv

Run:
    spark-submit spark/jobs/warehouse_metrics.py

Optional environment variables:
    DELIVERY_FEATURES_PATH
    WAREHOUSES_PATH
    OUTPUT_PATH
"""

import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DELIVERY_REQUIRED_COLUMNS = {
    "order_id",
    "warehouse_id",
    "region",
    "delivery_status",
    "order_date",
    "order_value",
    "delivery_distance_km",
    "actual_delivery_time_min",
    "delivery_delay_min",
    "is_delayed",
}

WAREHOUSE_REQUIRED_COLUMNS = {
    "warehouse_id",
    "warehouse_name",
    "region",
    "capacity_units",
    "latitude",
    "longitude",
}


def create_spark_session() -> SparkSession:
    """Create and return the Spark session."""
    return (
        SparkSession.builder
        .appName("LogisticsWarehouseMetrics")
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


def calculate_warehouse_metrics(deliveries, warehouses):
    """
    Calculate warehouse-level KPIs.

    Note:
        This project does not contain inventory snapshots. Therefore,
        order volume is used as an operational workload indicator, not
        as a claim of true inventory utilization.
    """

    warehouse_orders = (
        deliveries
        .groupBy("warehouse_id")
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

            F.round(F.avg("actual_delivery_time_min"), 2)
            .alias("avg_delivery_time_min"),

            F.round(F.avg("delivery_distance_km"), 2)
            .alias("avg_delivery_distance_km"),

            F.round(F.sum("delivery_distance_km"), 2)
            .alias("total_delivery_distance_km"),

            F.round(F.sum("order_value"), 2)
            .alias("total_order_value"),

            F.round(F.avg("order_value"), 2)
            .alias("avg_order_value"),

            F.countDistinct("driver_id")
            .alias("active_drivers")
            if "driver_id" in deliveries.columns
            else F.lit(0).cast("long").alias("active_drivers"),
        )
    )

    result = (
        warehouses
        .join(
            warehouse_orders,
            on="warehouse_id",
            how="left"
        )
        .fillna(
            {
                "total_orders": 0,
                "delivered_orders": 0,
                "delayed_orders": 0,
                "avg_delay_min": 0.0,
                "avg_delivery_time_min": 0.0,
                "avg_delivery_distance_km": 0.0,
                "total_delivery_distance_km": 0.0,
                "total_order_value": 0.0,
                "avg_order_value": 0.0,
                "active_drivers": 0,
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
            "orders_per_driver",
            F.round(
                F.when(
                    F.col("active_drivers") > 0,
                    F.col("total_orders")
                    / F.col("active_drivers")
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "orders_per_capacity_unit",
            F.round(
                F.when(
                    F.col("capacity_units") > 0,
                    F.col("total_orders")
                    / F.col("capacity_units")
                ).otherwise(0),
                4
            )
        )
        .withColumn(
            "revenue_per_order",
            F.round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("total_order_value")
                    / F.col("total_orders")
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "workload_category",
            F.when(
                F.col("total_orders") == 0,
                "No Activity"
            )
            .when(
                F.col("orders_per_capacity_unit") >= 1.0,
                "Very High"
            )
            .when(
                F.col("orders_per_capacity_unit") >= 0.75,
                "High"
            )
            .when(
                F.col("orders_per_capacity_unit") >= 0.40,
                "Moderate"
            )
            .otherwise("Low")
        )
        .withColumn(
            "performance_category",
            F.when(
                F.col("total_orders") == 0,
                "No Activity"
            )
            .when(
                (F.col("on_time_delivery_rate_pct") >= 90)
                & (F.col("delay_rate_pct") < 10),
                "High Performing"
            )
            .when(
                (F.col("on_time_delivery_rate_pct") >= 80)
                & (F.col("delay_rate_pct") < 20),
                "Stable"
            )
            .when(
                F.col("delay_rate_pct") >= 30,
                "Needs Attention"
            )
            .otherwise("Watch")
        )
        .withColumn(
            "risk_flag",
            F.when(
                F.col("delay_rate_pct") >= 30,
                "High"
            )
            .when(
                (F.col("delay_rate_pct") >= 15)
                | (F.col("on_time_delivery_rate_pct") < 80),
                "Medium"
            )
            .otherwise("Low")
        )
        .select(
            "warehouse_id",
            "warehouse_name",
            "region",
            "capacity_units",
            "latitude",
            "longitude",
            "total_orders",
            "delivered_orders",
            "delayed_orders",
            "on_time_orders",
            "on_time_delivery_rate_pct",
            "delay_rate_pct",
            "avg_delay_min",
            "avg_delivery_time_min",
            "avg_delivery_distance_km",
            "total_delivery_distance_km",
            "total_order_value",
            "avg_order_value",
            "revenue_per_order",
            "active_drivers",
            "orders_per_driver",
            "orders_per_capacity_unit",
            "workload_category",
            "performance_category",
            "risk_flag",
        )
        .orderBy(
            F.desc("total_orders"),
            F.desc("on_time_delivery_rate_pct")
        )
    )

    return result


def calculate_region_metrics(deliveries, warehouses):
    """
    Calculate regional warehouse workload and delivery KPIs.

    This aggregates the warehouses serving each region. It is not an
    inventory-utilization calculation because inventory snapshots are absent.
    """

    warehouse_region = (
        warehouses
        .groupBy("region")
        .agg(
            F.countDistinct("warehouse_id")
            .alias("total_warehouses"),

            F.round(F.sum("capacity_units"), 2)
            .alias("total_capacity_units"),
        )
    )

    delivery_region = (
        deliveries
        .groupBy("region")
        .agg(
            F.countDistinct("order_id")
            .alias("total_orders"),

            F.countDistinct(
                F.when(
                    F.col("delivery_status") == "DELIVERED",
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

            F.round(F.avg("actual_delivery_time_min"), 2)
            .alias("avg_delivery_time_min"),

            F.round(F.sum("order_value"), 2)
            .alias("total_order_value"),

            F.countDistinct("warehouse_id")
            .alias("active_warehouses"),
        )
    )

    result = (
        warehouse_region
        .join(
            delivery_region,
            on="region",
            how="left"
        )
        .fillna(
            {
                "total_orders": 0,
                "delivered_orders": 0,
                "delayed_orders": 0,
                "avg_delay_min": 0.0,
                "avg_delivery_time_min": 0.0,
                "total_order_value": 0.0,
                "active_warehouses": 0,
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
            "orders_per_capacity_unit",
            F.round(
                F.when(
                    F.col("total_capacity_units") > 0,
                    F.col("total_orders")
                    / F.col("total_capacity_units")
                ).otherwise(0),
                4
            )
        )
        .withColumn(
            "orders_per_warehouse",
            F.round(
                F.when(
                    F.col("active_warehouses") > 0,
                    F.col("total_orders")
                    / F.col("active_warehouses")
                ).otherwise(0),
                2
            )
        )
        .orderBy(F.desc("total_orders"))
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
    warehouses_path = os.getenv(
        "WAREHOUSES_PATH",
        "data/raw/warehouses.csv"
    )
    output_path = os.getenv(
        "OUTPUT_PATH",
        "data/processed/warehouse_metrics.csv"
    )

    spark = create_spark_session()

    try:
        deliveries = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(delivery_path)
        )

        warehouses = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(warehouses_path)
        )

        validate_columns(
            deliveries,
            DELIVERY_REQUIRED_COLUMNS,
            "Delivery features"
        )

        validate_columns(
            warehouses,
            WAREHOUSE_REQUIRED_COLUMNS,
            "Warehouses"
        )

        deliveries = (
            deliveries
            .withColumn(
                "order_id",
                F.trim(F.col("order_id"))
            )
            .withColumn(
                "warehouse_id",
                F.trim(F.col("warehouse_id"))
            )
            .withColumn(
                "region",
                F.upper(F.trim(F.col("region")))
            )
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
                & F.col("warehouse_id").isNotNull()
            )
        )

        warehouses = (
            warehouses
            .withColumn(
                "warehouse_id",
                F.trim(F.col("warehouse_id"))
            )
            .withColumn(
                "region",
                F.upper(F.trim(F.col("region")))
            )
            .withColumn(
                "capacity_units",
                F.col("capacity_units").cast("double")
            )
            .withColumn(
                "latitude",
                F.col("latitude").cast("double")
            )
            .withColumn(
                "longitude",
                F.col("longitude").cast("double")
            )
            .dropDuplicates(["warehouse_id"])
        )

        warehouse_metrics = calculate_warehouse_metrics(
            deliveries,
            warehouses
        )

        region_metrics = calculate_region_metrics(
            deliveries,
            warehouses
        )

        write_single_csv(
            warehouse_metrics,
            output_path,
            ".warehouse_metrics_tmp"
        )

        region_output_path = output_path.replace(
            "warehouse_metrics.csv",
            "warehouse_region_metrics.csv"
        )

        write_single_csv(
            region_metrics,
            region_output_path,
            ".warehouse_region_metrics_tmp"
        )

        print(
            f"Warehouse metrics written to: {output_path}"
        )
        print(
            f"Warehouse region metrics written to: "
            f"{region_output_path}"
        )
        print(
            f"Warehouse rows: {warehouse_metrics.count()}"
        )
        print(
            f"Region rows: {region_metrics.count()}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

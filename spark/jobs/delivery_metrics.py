"""
Delivery Metrics Job
--------------------
Purpose:
    Calculate business-ready delivery KPIs from the processed delivery feature
    dataset and write a compact metrics dataset for downstream analytics.

Input:
    data/processed/delivery_features.csv

Output:
    data/processed/delivery_metrics.csv

The job is intentionally batch-oriented. It can be executed with PySpark:

    spark-submit spark/jobs/delivery_metrics.py

Configuration:
    INPUT_PATH   - input CSV path
    OUTPUT_PATH  - output CSV path

Example:
    INPUT_PATH=data/processed/delivery_features.csv \
    OUTPUT_PATH=data/processed/delivery_metrics.csv \
    spark-submit spark/jobs/delivery_metrics.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


REQUIRED_COLUMNS = {
    "order_id",
    "region",
    "order_status",
    "delivery_status",
    "order_date",
    "order_value",
    "delivery_distance_km",
    "promised_delivery_time_min",
    "actual_delivery_time_min",
    "delivery_delay_min",
    "is_delayed",
}


def create_spark_session() -> SparkSession:
    """Create and return the Spark session."""
    return (
        SparkSession.builder
        .appName("LogisticsDeliveryMetrics")
        .getOrCreate()
    )


def validate_columns(df) -> None:
    """Fail fast when required columns are missing."""
    missing = REQUIRED_COLUMNS.difference(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )


def calculate_metrics(df):
    """
    Calculate overall delivery KPIs.

    Metrics:
        - total orders
        - delivered orders
        - delayed orders
        - on-time delivery rate
        - average delivery time
        - average promised delivery time
        - average delay
        - maximum delay
        - total order value
        - average order value
        - total delivery distance
        - average delivery distance
    """
    return df.agg(
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

        F.round(
            F.avg(
                F.when(
                    F.upper(F.col("delivery_status")) == "DELIVERED",
                    F.when(F.col("is_delayed") == 0, 1).otherwise(0)
                )
            ) * 100,
            2
        ).alias("on_time_delivery_rate_pct"),

        F.round(F.avg("actual_delivery_time_min"), 2)
        .alias("avg_delivery_time_min"),

        F.round(F.avg("promised_delivery_time_min"), 2)
        .alias("avg_promised_delivery_time_min"),

        F.round(
            F.avg(F.when(F.col("delivery_delay_min") > 0,
                         F.col("delivery_delay_min")))
        , 2).alias("avg_positive_delay_min"),

        F.round(F.max("delivery_delay_min"), 2)
        .alias("max_delay_min"),

        F.round(F.sum("order_value"), 2)
        .alias("total_order_value"),

        F.round(F.avg("order_value"), 2)
        .alias("avg_order_value"),

        F.round(F.sum("delivery_distance_km"), 2)
        .alias("total_delivery_distance_km"),

        F.round(F.avg("delivery_distance_km"), 2)
        .alias("avg_delivery_distance_km"),
    )


def calculate_region_metrics(df):
    """Calculate delivery KPIs by region."""
    return (
        df.groupBy("region")
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

            F.round(
                F.avg(
                    F.when(
                        F.upper(F.col("delivery_status")) == "DELIVERED",
                        F.when(F.col("is_delayed") == 0, 1).otherwise(0)
                    )
                ) * 100,
                2
            ).alias("on_time_delivery_rate_pct"),

            F.round(F.avg("actual_delivery_time_min"), 2)
            .alias("avg_delivery_time_min"),

            F.round(F.avg("delivery_delay_min"), 2)
            .alias("avg_delay_min"),

            F.round(F.sum("order_value"), 2)
            .alias("total_order_value"),

            F.round(F.avg("delivery_distance_km"), 2)
            .alias("avg_delivery_distance_km"),
        )
        .withColumn(
            "delay_rate_pct",
            F.round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("delayed_orders") /
                    F.col("total_orders") * 100
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "revenue_per_order",
            F.round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("total_order_value") /
                    F.col("total_orders")
                ).otherwise(0),
                2
            )
        )
        .orderBy(F.desc("total_orders"))
    )


def calculate_daily_metrics(df):
    """Calculate delivery KPIs by order date."""
    return (
        df.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),

            F.countDistinct(
                F.when(
                    F.col("is_delayed") == 1,
                    F.col("order_id")
                )
            ).alias("delayed_orders"),

            F.round(F.avg("actual_delivery_time_min"), 2)
            .alias("avg_delivery_time_min"),

            F.round(F.avg("delivery_delay_min"), 2)
            .alias("avg_delay_min"),

            F.round(F.sum("order_value"), 2)
            .alias("total_order_value"),
        )
        .withColumn(
            "delay_rate_pct",
            F.round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("delayed_orders") /
                    F.col("total_orders") * 100
                ).otherwise(0),
                2
            )
        )
        .orderBy("order_date")
    )


def calculate_status_metrics(df):
    """Calculate order volume and value by delivery status."""
    return (
        df.groupBy("delivery_status")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("order_value"), 2).alias("total_order_value"),
            F.round(F.avg("delivery_delay_min"), 2)
            .alias("avg_delay_min"),
        )
        .orderBy(F.desc("total_orders"))
    )


def main() -> None:
    input_path = os.getenv(
        "INPUT_PATH",
        "data/processed/delivery_features.csv"
    )
    output_path = os.getenv(
        "OUTPUT_PATH",
        "data/processed/delivery_metrics.csv"
    )

    spark = create_spark_session()

    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(input_path)
        )

        validate_columns(df)

        # Standardize important fields before aggregation.
        df = (
            df.withColumn(
                "order_date",
                F.to_date("order_date")
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
        )

        # Remove records that cannot contribute to reliable order-level KPIs.
        df = df.filter(
            F.col("order_id").isNotNull()
            & F.col("order_date").isNotNull()
            & F.col("region").isNotNull()
        )

        overall = (
            calculate_metrics(df)
            .withColumn("metric_level", F.lit("overall"))
            .withColumn("metric_key", F.lit("ALL"))
        )

        regional = (
            calculate_region_metrics(df)
            .withColumn("metric_level", F.lit("region"))
            .withColumn("metric_key", F.col("region"))
        )

        daily = (
            calculate_daily_metrics(df)
            .withColumn("metric_level", F.lit("daily"))
            .withColumn(
                "metric_key",
                F.date_format(F.col("order_date"), "yyyy-MM-dd")
            )
        )

        # Keep the primary output compact and dashboard-friendly.
        metrics = (
            overall.select(
                "metric_level",
                "metric_key",
                "total_orders",
                "delivered_orders",
                "delayed_orders",
                "on_time_delivery_rate_pct",
                "avg_delivery_time_min",
                "avg_promised_delivery_time_min",
                "avg_positive_delay_min",
                "max_delay_min",
                "total_order_value",
                "avg_order_value",
                "total_delivery_distance_km",
                "avg_delivery_distance_km",
            )
            .unionByName(
                regional.select(
                    "metric_level",
                    "metric_key",
                    "total_orders",
                    "delivered_orders",
                    "delayed_orders",
                    "on_time_delivery_rate_pct",
                    "avg_delivery_time_min",
                    F.lit(None).cast("double").alias(
                        "avg_promised_delivery_time_min"
                    ),
                    F.lit(None).cast("double").alias(
                        "avg_positive_delay_min"
                    ),
                    F.lit(None).cast("double").alias("max_delay_min"),
                    "total_order_value",
                    F.col("revenue_per_order").alias("avg_order_value"),
                    F.lit(None).cast("double").alias(
                        "total_delivery_distance_km"
                    ),
                    "avg_delivery_distance_km",
                )
            )
            .unionByName(
                daily.select(
                    "metric_level",
                    "metric_key",
                    "total_orders",
                    F.lit(None).cast("long").alias("delivered_orders"),
                    "delayed_orders",
                    F.lit(None).cast("double").alias(
                        "on_time_delivery_rate_pct"
                    ),
                    "avg_delivery_time_min",
                    F.lit(None).cast("double").alias(
                        "avg_promised_delivery_time_min"
                    ),
                    F.lit(None).cast("double").alias(
                        "avg_positive_delay_min"
                    ),
                    F.lit(None).cast("double").alias("max_delay_min"),
                    "total_order_value",
                    F.lit(None).cast("double").alias("avg_order_value"),
                    F.lit(None).cast("double").alias(
                        "total_delivery_distance_km"
                    ),
                    F.lit(None).cast("double").alias(
                        "avg_delivery_distance_km"
                    ),
                )
            )
        )

        # CSV output is convenient for inspection and downstream prototypes.
        # A single output file is created to match the project's current layout.
        output_dir = os.path.dirname(output_path) or "."
        temp_dir = f"{output_dir}/.delivery_metrics_tmp"

        (
            metrics
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(temp_dir)
        )

        part_files = [
            name for name in os.listdir(temp_dir)
            if name.startswith("part-") and name.endswith(".csv")
        ]

        if not part_files:
            raise RuntimeError(
                f"No Spark CSV part file was generated in {temp_dir}"
            )

        os.makedirs(output_dir, exist_ok=True)

        final_path = output_path
        part_path = os.path.join(temp_dir, part_files[0])

        # Spark may run locally or on a mounted filesystem. The final rename
        # is performed through Python only after Spark has completed the write.
        import shutil

        if os.path.exists(final_path):
            os.remove(final_path)

        shutil.move(part_path, final_path)
        shutil.rmtree(temp_dir, ignore_errors=True)

        print(f"Delivery metrics written to: {final_path}")
        print(f"Rows written: {metrics.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

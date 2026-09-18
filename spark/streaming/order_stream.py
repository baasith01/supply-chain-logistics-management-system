"""
Spark Structured Streaming job: order_stream.py

Purpose:
    Consume order events from Kafka, parse and validate JSON messages,
    create operational features, and persist the streaming output.

Kafka topic:
    orders

Input format:
    JSON

Run:
    spark-submit spark/streaming/order_stream.py

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS   Default: localhost:9092
    KAFKA_ORDER_TOPIC         Default: orders
    CHECKPOINT_LOCATION       Optional checkpoint directory
    ORDER_STREAM_OUTPUT       Optional output directory

The job requires a running Kafka broker and the Spark Kafka connector
at execution time.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_ORDER_TOPIC",
    "orders",
)

CHECKPOINT_LOCATION = os.getenv(
    "ORDER_STREAM_CHECKPOINT",
    str(PROJECT_ROOT / "spark_checkpoints" / "order_stream"),
)

OUTPUT_PATH = os.getenv(
    "ORDER_STREAM_OUTPUT",
    str(PROJECT_ROOT / "data" / "processed" / "order_stream"),
)


ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("region", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("delivery_status", StringType(), True),
        StructField("order_value", DoubleType(), True),
        StructField("delivery_distance_km", DoubleType(), True),
        StructField("promised_delivery_time_min", IntegerType(), True),
        StructField("actual_delivery_time_min", IntegerType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    """Create the Spark session for Structured Streaming."""
    return (
        SparkSession.builder
        .appName("Logistics-Order-Stream")
        .getOrCreate()
    )


def read_order_stream(spark: SparkSession) -> DataFrame:
    """Read order events from the Kafka orders topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_order_stream(kafka_df: DataFrame) -> DataFrame:
    """Parse order JSON messages and create operational features."""

    parsed = (
        kafka_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("json_value"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "data",
            F.from_json(F.col("json_value"), ORDER_SCHEMA),
        )
        .select(
            "kafka_key",
            "kafka_timestamp",
            "data.*",
        )
    )

    transformed = (
        parsed
        .withColumn("region", F.upper(F.trim("region")))
        .withColumn("order_status", F.upper(F.trim("order_status")))
        .withColumn("delivery_status", F.upper(F.trim("delivery_status")))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn(
            "delivery_delay_min",
            F.when(
                F.col("actual_delivery_time_min").isNotNull()
                & F.col("promised_delivery_time_min").isNotNull(),
                F.col("actual_delivery_time_min")
                - F.col("promised_delivery_time_min"),
            ),
        )
        .withColumn(
            "is_delayed",
            F.when(F.col("delivery_delay_min") > 0, 1).otherwise(0),
        )
        .withColumn(
            "delivery_per_km_value",
            F.when(
                F.col("delivery_distance_km") > 0,
                F.round(
                    F.col("order_value") / F.col("delivery_distance_km"),
                    2,
                ),
            ),
        )
        .withColumn(
            "distance_bucket",
            F.when(F.col("delivery_distance_km") < 5, "0-5 km")
            .when(F.col("delivery_distance_km") < 10, "5-10 km")
            .when(F.col("delivery_distance_km") < 20, "10-20 km")
            .when(F.col("delivery_distance_km") < 50, "20-50 km")
            .otherwise("50+ km"),
        )
        .withColumn(
            "is_valid_order",
            F.when(
                F.col("order_id").isNotNull()
                & (F.trim(F.col("order_id")) != "")
                & F.col("customer_id").isNotNull()
                & F.col("warehouse_id").isNotNull()
                & (F.col("order_value") >= 0)
                & (F.col("delivery_distance_km") >= 0),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "processing_delay_sec",
            F.round(
                F.unix_timestamp("kafka_timestamp")
                - F.unix_timestamp(
                    F.to_timestamp("order_date")
                ),
                2,
            ),
        )
    )

    return transformed


def write_stream(df: DataFrame):
    """Write streaming order data to a checkpointed JSON sink."""
    return (
        df
        .writeStream
        .format("json")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )


def main() -> int:
    """Run the order Structured Streaming pipeline."""
    spark = create_spark_session()

    try:
        print("Starting order Kafka stream...")
        print(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Kafka topic:   {KAFKA_TOPIC}")
        print(f"Output path:   {OUTPUT_PATH}")
        print(f"Checkpoint:    {CHECKPOINT_LOCATION}")

        Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
        Path(CHECKPOINT_LOCATION).parent.mkdir(parents=True, exist_ok=True)

        kafka_df = read_order_stream(spark)
        order_df = transform_order_stream(kafka_df)

        query = write_stream(order_df)

        print("Order streaming query started.")
        print("Waiting for Kafka order events...")

        query.awaitTermination()

        return 0

    except KeyboardInterrupt:
        print("Order streaming job stopped by user.")
        return 0

    except Exception as exc:
        print(f"Order streaming failed: {exc}", file=sys.stderr)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

"""
Spark Structured Streaming job: realtime_delivery.py

Purpose:
    Combine real-time order events with GPS telemetry to produce a
    delivery-operations stream. The job identifies active deliveries,
    vehicle movement, delay risk, and operational status.

Kafka topics:
    orders
    gps_tracking

Run:
    spark-submit spark/streaming/realtime_delivery.py

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS   Default: localhost:9092
    KAFKA_ORDER_TOPIC         Default: orders
    KAFKA_GPS_TOPIC           Default: gps_tracking
    CHECKPOINT_LOCATION       Optional checkpoint directory
    REALTIME_OUTPUT            Optional output directory

Important:
    This implementation demonstrates the streaming architecture without
    inventing a persistent production state store. Order events and GPS
    events are consumed independently and written as real-time operational
    records. A production implementation can add stateful stream-stream
    joins or a serving database for active-delivery state.
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
    TimestampType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

ORDER_TOPIC = os.getenv(
    "KAFKA_ORDER_TOPIC",
    "orders",
)

GPS_TOPIC = os.getenv(
    "KAFKA_GPS_TOPIC",
    "gps_tracking",
)

CHECKPOINT_LOCATION = os.getenv(
    "REALTIME_CHECKPOINT",
    str(PROJECT_ROOT / "spark_checkpoints" / "realtime_delivery"),
)

OUTPUT_PATH = os.getenv(
    "REALTIME_OUTPUT",
    str(PROJECT_ROOT / "data" / "processed" / "realtime_delivery"),
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

GPS_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("region", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed_kmph", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("vehicle_status", StringType(), True),
        StructField("ignition_status", StringType(), True),
        StructField("signal_quality", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    """Create the Spark Structured Streaming session."""
    return (
        SparkSession.builder
        .appName("Logistics-Realtime-Delivery")
        .getOrCreate()
    )


def read_kafka_stream(
    spark: SparkSession,
    topic: str,
) -> DataFrame:
    """Read a Kafka topic as a Spark streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_orders(kafka_df: DataFrame) -> DataFrame:
    """Parse order events from Kafka."""
    return (
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
            "active_delivery",
            F.when(
                F.col("delivery_status").isin(
                    "ASSIGNED",
                    "PICKED_UP",
                    "IN_TRANSIT",
                    "OUT_FOR_DELIVERY",
                ),
                1,
            ).otherwise(0),
        )
    )


def parse_gps(kafka_df: DataFrame) -> DataFrame:
    """Parse GPS events from Kafka."""
    return (
        kafka_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("json_value"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "data",
            F.from_json(F.col("json_value"), GPS_SCHEMA),
        )
        .select(
            "kafka_key",
            "kafka_timestamp",
            "data.*",
        )
        .withColumn("region", F.upper(F.trim("region")))
        .withColumn("vehicle_status", F.upper(F.trim("vehicle_status")))
        .withColumn("ignition_status", F.upper(F.trim("ignition_status")))
        .withColumn("signal_quality", F.upper(F.trim("signal_quality")))
        .withColumn(
            "is_moving",
            F.when(F.col("speed_kmph") > 0, 1).otherwise(0),
        )
        .withColumn(
            "is_low_signal",
            F.when(
                F.col("signal_quality").isin("LOW", "POOR"),
                1,
            ).otherwise(0),
        )
    )


def build_realtime_delivery_stream(
    orders_df: DataFrame,
    gps_df: DataFrame,
) -> DataFrame:
    """
    Create a unified operational stream.

    The streams are unioned into a common event model rather than performing
    an unbounded stream-stream join. This avoids incorrectly matching GPS
    telemetry to orders without a time-bounded state model.
    """

    order_events = orders_df.select(
        F.col("kafka_timestamp").alias("event_ingest_time"),
        F.col("order_date").cast("timestamp").alias("event_time"),
        F.lit("ORDER").alias("event_source"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("warehouse_id"),
        F.col("driver_id"),
        F.col("vehicle_id"),
        F.col("region"),
        F.col("delivery_status").alias("status"),
        F.col("delivery_delay_min"),
        F.col("is_delayed"),
        F.col("active_delivery"),
        F.lit(None).cast("double").alias("latitude"),
        F.lit(None).cast("double").alias("longitude"),
        F.lit(None).cast("double").alias("speed_kmph"),
        F.lit(None).cast("string").alias("signal_quality"),
        F.lit(None).cast("int").alias("is_moving"),
        F.lit(None).cast("int").alias("is_low_signal"),
    )

    gps_events = gps_df.select(
        F.col("kafka_timestamp").alias("event_ingest_time"),
        F.col("timestamp").alias("event_time"),
        F.lit("GPS").alias("event_source"),
        F.lit(None).cast("string").alias("order_id"),
        F.lit(None).cast("string").alias("customer_id"),
        F.lit(None).cast("string").alias("warehouse_id"),
        F.lit(None).cast("string").alias("driver_id"),
        F.col("vehicle_id"),
        F.col("region"),
        F.col("vehicle_status").alias("status"),
        F.lit(None).cast("double").alias("delivery_delay_min"),
        F.lit(None).cast("int").alias("is_delayed"),
        F.lit(None).cast("int").alias("active_delivery"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("speed_kmph"),
        F.col("signal_quality"),
        F.col("is_moving"),
        F.col("is_low_signal"),
    )

    unified = (
        order_events
        .unionByName(gps_events)
        .withColumn(
            "operational_risk",
            F.when(
                (F.col("event_source") == "ORDER")
                & (F.col("is_delayed") == 1),
                "HIGH",
            )
            .when(
                (F.col("event_source") == "GPS")
                & (F.col("is_low_signal") == 1),
                "MEDIUM",
            )
            .otherwise("LOW"),
        )
    )

    return unified


def write_stream(df: DataFrame):
    """Write the unified real-time delivery stream."""
    return (
        df.writeStream
        .format("json")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )


def main() -> int:
    """Run the real-time delivery streaming pipeline."""
    spark = create_spark_session()

    try:
        print("Starting real-time delivery stream...")
        print(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Order topic:   {ORDER_TOPIC}")
        print(f"GPS topic:     {GPS_TOPIC}")
        print(f"Output path:   {OUTPUT_PATH}")
        print(f"Checkpoint:    {CHECKPOINT_LOCATION}")

        Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
        Path(CHECKPOINT_LOCATION).parent.mkdir(parents=True, exist_ok=True)

        order_kafka_df = read_kafka_stream(spark, ORDER_TOPIC)
        gps_kafka_df = read_kafka_stream(spark, GPS_TOPIC)

        orders_df = parse_orders(order_kafka_df)
        gps_df = parse_gps(gps_kafka_df)

        realtime_df = build_realtime_delivery_stream(
            orders_df,
            gps_df,
        )

        query = write_stream(realtime_df)

        print("Real-time delivery query started.")
        print("Waiting for order and GPS events...")

        query.awaitTermination()

        return 0

    except KeyboardInterrupt:
        print("Real-time delivery job stopped by user.")
        return 0

    except Exception as exc:
        print(
            f"Real-time delivery streaming failed: {exc}",
            file=sys.stderr,
        )
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

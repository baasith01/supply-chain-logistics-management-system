"""
Spark Structured Streaming job: gps_stream.py

Purpose:
    Consume vehicle GPS events from Kafka, validate/transform the events,
    and write the streaming output to a checkpointed sink.

Kafka topic:
    gps_tracking

Input format:
    JSON

Run:
    spark-submit spark/streaming/gps_stream.py

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS   Default: localhost:9092
    KAFKA_GPS_TOPIC           Default: gps_tracking
    CHECKPOINT_LOCATION       Optional checkpoint directory

Important:
    This job is a real streaming implementation, but it requires a running
    Kafka broker and the Spark Kafka connector at execution time.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
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
KAFKA_TOPIC = os.getenv(
    "KAFKA_GPS_TOPIC",
    "gps_tracking",
)

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    str(PROJECT_ROOT / "spark_checkpoints" / "gps_stream"),
)

OUTPUT_PATH = os.getenv(
    "GPS_STREAM_OUTPUT",
    str(PROJECT_ROOT / "data" / "processed" / "gps_stream"),
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
    """Create the Spark session for Structured Streaming."""
    return (
        SparkSession.builder
        .appName("Logistics-GPS-Stream")
        .getOrCreate()
    )


def read_gps_stream(spark: SparkSession) -> DataFrame:
    """Read GPS events from the Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_gps_stream(kafka_df: DataFrame) -> DataFrame:
    """Parse Kafka JSON messages and create streaming GPS features."""

    parsed = (
        kafka_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("json_value"),
            F.timestamp("timestamp").alias("kafka_timestamp"),
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
    )

    transformed = (
        parsed
        .withColumn("region", F.upper(F.trim("region")))
        .withColumn("vehicle_status", F.upper(F.trim("vehicle_status")))
        .withColumn("ignition_status", F.upper(F.trim("ignition_status")))
        .withColumn("signal_quality", F.upper(F.trim("signal_quality")))
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_hour", F.hour("timestamp"))
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
        .withColumn(
            "is_valid_coordinates",
            F.when(
                F.col("latitude").between(-90, 90)
                & F.col("longitude").between(-180, 180),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "processing_delay_sec",
            F.round(
                F.unix_timestamp("kafka_timestamp")
                - F.unix_timestamp("timestamp"),
                2,
            ),
        )
    )

    return transformed


def write_stream(df: DataFrame):
    """Write streaming GPS data to a checkpointed file sink."""
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
    """Run the GPS Structured Streaming pipeline."""
    spark = create_spark_session()

    try:
        print("Starting GPS Kafka stream...")
        print(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Kafka topic:   {KAFKA_TOPIC}")
        print(f"Output path:   {OUTPUT_PATH}")
        print(f"Checkpoint:    {CHECKPOINT_LOCATION}")

        Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
        Path(CHECKPOINT_LOCATION).parent.mkdir(parents=True, exist_ok=True)

        kafka_df = read_gps_stream(spark)
        gps_df = transform_gps_stream(kafka_df)

        query = write_stream(gps_df)

        print("GPS streaming query started.")
        print("Waiting for Kafka GPS events...")

        query.awaitTermination()

        return 0

    except KeyboardInterrupt:
        print("GPS streaming job stopped by user.")
        return 0

    except Exception as exc:
        print(f"GPS streaming failed: {exc}", file=sys.stderr)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

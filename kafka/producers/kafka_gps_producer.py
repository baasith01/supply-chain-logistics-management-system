"""
Kafka GPS producer.

Purpose:
    Publish GPS tracking events from the raw GPS dataset to the Kafka
    `gps_tracking` topic.

This module is the Kafka-layer producer. The corresponding ingestion-layer
producer is located at ingestion/streaming/gps_producer.py. Keeping this
implementation here makes the kafka/ directory independently usable.

Flow:
    data/raw/gps_tracking.csv
            |
            v
    Kafka GPS Producer
            |
            v
    Kafka topic: gps_tracking
            |
            v
    gps_consumer.py / Spark Structured Streaming
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError


load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "gps_tracking"


def create_producer() -> KafkaProducer:
    """Create a Kafka producer using the centralized environment settings."""
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_BOOTSTRAP_SERVERS,
    )

    return KafkaProducer(
        bootstrap_servers=[
            server.strip()
            for server in bootstrap_servers.split(",")
            if server.strip()
        ],
        key_serializer=lambda key: (
            key.encode("utf-8") if key is not None else None
        ),
        value_serializer=lambda value: json.dumps(
            value,
            default=str,
        ).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
        compression_type="gzip",
    )


def make_json_safe(value: Any) -> Optional[Any]:
    """Convert pandas/numpy values into JSON-safe Python values."""
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass

    return value


def row_to_event(row: pd.Series) -> Dict[str, Any]:
    """Convert one GPS DataFrame row into a JSON event."""
    return {
        key: make_json_safe(value)
        for key, value in row.to_dict().items()
    }


def publish_gps_events(
    source_path: str,
    limit: int = 100,
    delay_seconds: float = 0.1,
) -> int:
    """
    Publish GPS events to Kafka.

    Args:
        source_path: Raw GPS CSV path.
        limit: Maximum number of events. 0 publishes all rows.
        delay_seconds: Delay between events to simulate a live stream.

    Returns:
        Number of successfully published events.
    """
    source = Path(source_path)

    if not source.exists():
        raise FileNotFoundError(f"GPS source not found: {source}")

    df = pd.read_csv(source)

    if df.empty:
        logger.warning("GPS source contains no records.")
        return 0

    if limit > 0:
        df = df.head(limit)

    topic = os.getenv("KAFKA_GPS_TOPIC", DEFAULT_TOPIC)
    producer = create_producer()
    published = 0

    try:
        for _, row in df.iterrows():
            event = row_to_event(row)
            vehicle_id = event.get("vehicle_id")

            try:
                future = producer.send(
                    topic,
                    key=str(vehicle_id) if vehicle_id is not None else None,
                    value=event,
                )

                # Wait for broker acknowledgement so failures are visible.
                future.get(timeout=10)
                published += 1

                logger.info(
                    "GPS event published | vehicle=%s | timestamp=%s",
                    vehicle_id,
                    event.get("timestamp"),
                )

                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            except KafkaError as exc:
                logger.error(
                    "Failed to publish GPS event for vehicle %s: %s",
                    vehicle_id,
                    exc,
                )

    finally:
        producer.flush()
        producer.close()

    return published


def main(source_path: str, limit: int, delay_seconds: float) -> None:
    """Run the GPS Kafka producer."""
    try:
        count = publish_gps_events(
            source_path=source_path,
            limit=limit,
            delay_seconds=delay_seconds,
        )

        print("Kafka GPS producer completed successfully.")
        print(f"Topic: {os.getenv('KAFKA_GPS_TOPIC', DEFAULT_TOPIC)}")
        print(f"Messages published: {count:,}")

    except (FileNotFoundError, ValueError, KafkaError) as exc:
        logger.error("GPS producer failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Publish GPS tracking events to Kafka."
    )

    parser.add_argument(
        "--source",
        default="data/raw/gps_tracking.csv",
        help="Path to the raw GPS CSV.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum events to publish. Use 0 for all events.",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay in seconds between published events.",
    )

    args = parser.parse_args()

    main(
        source_path=args.source,
        limit=args.limit,
        delay_seconds=args.delay,
    )

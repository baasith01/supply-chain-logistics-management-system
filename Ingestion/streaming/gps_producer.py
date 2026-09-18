"""
Kafka GPS producer.

Purpose:
    Read GPS tracking events and publish them to the `gps_tracking` Kafka
    topic as JSON messages.

Pipeline position:
    data/raw/gps_tracking.csv -> Kafka Producer -> gps_tracking topic
    -> Kafka Consumer / Spark Structured Streaming

Configuration:
    KAFKA_BOOTSTRAP_SERVERS is read from the environment.
    KAFKA_GPS_TOPIC defaults to gps_tracking.

For local development, the producer can publish a limited number of rows
using --limit.
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError


load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "gps_tracking"


def create_producer() -> KafkaProducer:
    """
    Create and configure a Kafka producer.
    """
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_BOOTSTRAP_SERVERS,
    )

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=3,
        acks="all",
    )


def row_to_event(row: pd.Series) -> Dict:
    """
    Convert a DataFrame row into a JSON-serializable GPS event.
    """
    event = row.to_dict()

    for key, value in event.items():
        if pd.isna(value):
            event[key] = None
        elif isinstance(value, pd.Timestamp):
            event[key] = value.isoformat()
        else:
            event[key] = value

    return event


def publish_gps_data(
    source_path: str,
    limit: int = 0,
    delay_seconds: float = 0.1,
) -> int:
    """
    Read GPS data and publish each record to Kafka.

    Args:
        source_path: CSV containing GPS tracking events.
        limit: Maximum rows to publish. 0 means all rows.
        delay_seconds: Delay between messages to simulate streaming.

    Returns:
        Number of successfully submitted messages.
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

    sent = 0

    try:
        for _, row in df.iterrows():
            event = row_to_event(row)

            try:
                future = producer.send(topic, value=event)
                future.get(timeout=10)
                sent += 1

                logger.info(
                    "Published GPS event | vehicle=%s | timestamp=%s",
                    event.get("vehicle_id"),
                    event.get("timestamp"),
                )

                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            except KafkaError as exc:
                logger.error("Failed to publish GPS event: %s", exc)

    finally:
        producer.flush()
        producer.close()

    return sent


def main(source_path: str, limit: int, delay_seconds: float) -> None:
    """Run the GPS Kafka producer."""
    try:
        count = publish_gps_data(
            source_path=source_path,
            limit=limit,
            delay_seconds=delay_seconds,
        )

        print("GPS Kafka producer completed.")
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
        description="Publish GPS tracking data to Kafka."
    )

    parser.add_argument(
        "--source",
        default="data/raw/gps_tracking.csv",
        help="GPS source CSV path.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum rows to publish. Use 0 for all rows.",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay in seconds between messages.",
    )

    args = parser.parse_args()

    main(
        source_path=args.source,
        limit=args.limit,
        delay_seconds=args.delay,
    )

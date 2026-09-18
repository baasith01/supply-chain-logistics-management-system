"""
Kafka order producer.

Purpose:
    Publish order events from the raw order dataset to the Kafka `orders`
    topic.

This is the Kafka-layer implementation for:
    data/raw/orders.csv -> Kafka -> orders -> consumers/Spark

The implementation uses `order_id` as the Kafka message key so events for
the same order can be routed consistently to the same partition.
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
DEFAULT_TOPIC = "orders"


def create_producer() -> KafkaProducer:
    """Create a configured Kafka producer."""
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_BOOTSTRAP_SERVERS,
    )

    servers = [
        server.strip()
        for server in bootstrap_servers.split(",")
        if server.strip()
    ]

    return KafkaProducer(
        bootstrap_servers=servers,
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
    """Convert an order DataFrame row into a JSON event."""
    return {
        key: make_json_safe(value)
        for key, value in row.to_dict().items()
    }


def publish_order_events(
    source_path: str,
    limit: int = 100,
    delay_seconds: float = 0.1,
) -> int:
    """
    Publish order events to the configured Kafka topic.

    Args:
        source_path: Raw orders CSV path.
        limit: Maximum events to publish. 0 means all rows.
        delay_seconds: Delay between messages for stream simulation.

    Returns:
        Number of successfully published events.
    """
    source = Path(source_path)

    if not source.exists():
        raise FileNotFoundError(f"Order source not found: {source}")

    df = pd.read_csv(source)

    if df.empty:
        logger.warning("Order source contains no records.")
        return 0

    if limit > 0:
        df = df.head(limit)

    topic = os.getenv("KAFKA_ORDERS_TOPIC", DEFAULT_TOPIC)
    producer = create_producer()
    published = 0

    try:
        for _, row in df.iterrows():
            event = row_to_event(row)
            order_id = event.get("order_id")

            try:
                future = producer.send(
                    topic,
                    key=str(order_id) if order_id is not None else None,
                    value=event,
                )

                # Wait for acknowledgement so failed messages are visible.
                future.get(timeout=10)
                published += 1

                logger.info(
                    "Order event published | order_id=%s | status=%s",
                    order_id,
                    event.get("delivery_status"),
                )

                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            except KafkaError as exc:
                logger.error(
                    "Failed to publish order %s: %s",
                    order_id,
                    exc,
                )

    finally:
        producer.flush()
        producer.close()

    return published


def main(source_path: str, limit: int, delay_seconds: float) -> None:
    """Run the order Kafka producer."""
    try:
        count = publish_order_events(
            source_path=source_path,
            limit=limit,
            delay_seconds=delay_seconds,
        )

        print("Kafka order producer completed successfully.")
        print(f"Topic: {os.getenv('KAFKA_ORDERS_TOPIC', DEFAULT_TOPIC)}")
        print(f"Messages published: {count:,}")

    except (FileNotFoundError, ValueError, KafkaError) as exc:
        logger.error("Order producer failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Publish order events to Kafka."
    )

    parser.add_argument(
        "--source",
        default="data/raw/orders.csv",
        help="Path to the raw orders CSV.",
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

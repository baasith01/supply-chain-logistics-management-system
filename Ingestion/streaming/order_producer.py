"""
Kafka order producer.

Purpose:
    Read order events from a CSV file and publish them as JSON messages to
    the `orders` Kafka topic.

Pipeline position:
    data/raw/orders.csv -> Kafka Producer -> orders topic
    -> Kafka Consumer / Spark Structured Streaming

Configuration:
    KAFKA_BOOTSTRAP_SERVERS is read from the environment.
    KAFKA_ORDERS_TOPIC defaults to orders.

For local development, --limit controls how many events are published.
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError


load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "orders"


def create_producer() -> KafkaProducer:
    """
    Create and configure the Kafka producer.
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


def make_json_safe(value: Any) -> Any:
    """
    Convert pandas/numpy values into JSON-serializable Python values.
    """
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    # Convert numpy scalar values such as int64/float64.
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass

    return value


def row_to_event(row: pd.Series) -> Dict[str, Any]:
    """
    Convert one order row into a JSON-ready event.
    """
    return {
        key: make_json_safe(value)
        for key, value in row.to_dict().items()
    }


def publish_order_data(
    source_path: str,
    limit: int = 0,
    delay_seconds: float = 0.1,
) -> int:
    """
    Publish order records to Kafka.

    Args:
        source_path: Order source CSV.
        limit: Maximum rows to publish. 0 means all rows.
        delay_seconds: Delay between messages to simulate streaming.

    Returns:
        Number of successfully published messages.
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

    sent = 0

    try:
        for _, row in df.iterrows():
            event = row_to_event(row)

            try:
                # Use order_id as the Kafka message key when available.
                key = str(event["order_id"]).encode("utf-8")                     if event.get("order_id") is not None else None

                future = producer.send(
                    topic,
                    key=key,
                    value=event,
                )

                # Wait for acknowledgement so failures are visible during demos.
                future.get(timeout=10)
                sent += 1

                logger.info(
                    "Published order event | order_id=%s | status=%s",
                    event.get("order_id"),
                    event.get("delivery_status"),
                )

                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            except KafkaError as exc:
                logger.error(
                    "Failed to publish order %s: %s",
                    event.get("order_id"),
                    exc,
                )

    finally:
        producer.flush()
        producer.close()

    return sent


def main(source_path: str, limit: int, delay_seconds: float) -> None:
    """Run the order Kafka producer."""
    try:
        count = publish_order_data(
            source_path=source_path,
            limit=limit,
            delay_seconds=delay_seconds,
        )

        print("Order Kafka producer completed.")
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
        help="Order source CSV path.",
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

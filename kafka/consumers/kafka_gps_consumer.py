"""
Kafka GPS consumer.

Purpose:
    Consume GPS tracking events from the `gps_tracking` Kafka topic,
    validate/normalize the incoming JSON messages, and persist consumed
    events to a local CSV sink for downstream processing.

Flow:
    Kafka topic: gps_tracking
            |
            v
    gps_consumer.py
            |
            v
    data/processed/gps_stream_output.csv
            |
            v
    Spark / Analytics

The CSV sink is useful for local development and demonstrations. In a
production architecture, the consumer can be replaced or extended to write
to HDFS, S3, a database, or a streaming-processing framework.
"""

import argparse
import csv
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError


load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "gps_tracking"
DEFAULT_GROUP = "logistics-gps-consumer"

OUTPUT_COLUMNS = [
    "vehicle_id",
    "timestamp",
    "region",
    "latitude",
    "longitude",
    "speed_kmph",
    "heading",
    "vehicle_status",
    "ignition_status",
    "signal_quality",
]


def create_consumer(
    topic: Optional[str] = None,
    group_id: Optional[str] = None,
) -> KafkaConsumer:
    """Create a Kafka consumer for GPS events."""
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_BOOTSTRAP_SERVERS,
    )

    topic = topic or os.getenv(
        "KAFKA_GPS_TOPIC",
        DEFAULT_TOPIC,
    )

    group_id = group_id or os.getenv(
        "KAFKA_GPS_CONSUMER_GROUP",
        DEFAULT_GROUP,
    )

    return KafkaConsumer(
        topic,
        bootstrap_servers=[
            server.strip()
            for server in bootstrap_servers.split(",")
            if server.strip()
        ],
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
    )


def validate_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and normalize one GPS event.

    Required fields:
        vehicle_id, timestamp, latitude, longitude
    """
    required = [
        "vehicle_id",
        "timestamp",
        "latitude",
        "longitude",
    ]

    missing = [
        field
        for field in required
        if event.get(field) in (None, "")
    ]

    if missing:
        raise ValueError(
            f"GPS event missing required fields: {', '.join(missing)}"
        )

    normalized = {
        column: event.get(column)
        for column in OUTPUT_COLUMNS
    }

    # Convert numeric values to predictable types.
    for column in [
        "latitude",
        "longitude",
        "speed_kmph",
        "heading",
    ]:
        if normalized[column] is not None:
            normalized[column] = float(normalized[column])

    return normalized


def append_event(
    output_path: str,
    event: Dict[str, Any],
) -> None:
    """Append a validated GPS event to a CSV sink."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    file_exists = output.exists() and output.stat().st_size > 0

    with output.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=OUTPUT_COLUMNS,
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(event)


def consume_gps_events(
    output_path: str = "data/processed/gps_stream_output.csv",
    max_messages: int = 100,
) -> int:
    """
    Consume GPS events and write them to the local CSV sink.

    Args:
        output_path: Local output CSV.
        max_messages: Number of messages to consume. 0 means continuous mode.

    Returns:
        Number of successfully processed events.
    """
    consumer = create_consumer()
    processed = 0

    try:
        for message in consumer:
            try:
                event = validate_event(message.value)
                append_event(output_path, event)

                consumer.commit()
                processed += 1

                logger.info(
                    "GPS event consumed | vehicle=%s | partition=%s | offset=%s",
                    event["vehicle_id"],
                    message.partition,
                    message.offset,
                )

                if max_messages > 0 and processed >= max_messages:
                    break

            except (ValueError, TypeError, KeyError) as exc:
                logger.error(
                    "Invalid GPS event at partition=%s offset=%s: %s",
                    message.partition,
                    message.offset,
                    exc,
                )

    except KafkaError as exc:
        logger.error("Kafka consumer error: %s", exc)
        raise

    finally:
        consumer.close()

    return processed


def main(output_path: str, max_messages: int) -> None:
    """Run the GPS Kafka consumer."""
    try:
        count = consume_gps_events(
            output_path=output_path,
            max_messages=max_messages,
        )

        print("Kafka GPS consumer completed successfully.")
        print(f"Messages processed: {count:,}")
        print(f"Output: {output_path}")

    except (KafkaError, OSError, ValueError) as exc:
        logger.error("GPS consumer failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Consume GPS tracking events from Kafka."
    )

    parser.add_argument(
        "--output",
        default="data/processed/gps_stream_output.csv",
        help="Output CSV path.",
    )

    parser.add_argument(
        "--max-messages",
        type=int,
        default=100,
        help="Messages to consume. Use 0 for continuous mode.",
    )

    args = parser.parse_args()

    main(
        output_path=args.output,
        max_messages=args.max_messages,
    )

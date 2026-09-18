"""
Central Kafka configuration for the logistics pipeline.

Purpose:
    Keep Kafka connection settings, topic names, and common producer/consumer
    configuration in one place so all Kafka producers and consumers use the
    same configuration.

Environment variables are loaded from .env when available.
"""

import os
from typing import Dict, List

from dotenv import load_dotenv


load_dotenv()


# ---------------------------------------------------------------------------
# Kafka connection
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

BOOTSTRAP_SERVERS: List[str] = [
    server.strip()
    for server in KAFKA_BOOTSTRAP_SERVERS.split(",")
    if server.strip()
]


# ---------------------------------------------------------------------------
# Kafka topics
# ---------------------------------------------------------------------------

KAFKA_TOPICS: Dict[str, str] = {
    "orders": os.getenv("KAFKA_ORDERS_TOPIC", "orders"),
    "gps_tracking": os.getenv("KAFKA_GPS_TOPIC", "gps_tracking"),
    "delivery_updates": os.getenv(
        "KAFKA_DELIVERY_TOPIC",
        "delivery_updates",
    ),
    "traffic_updates": os.getenv(
        "KAFKA_TRAFFIC_TOPIC",
        "traffic_updates",
    ),
    "weather_updates": os.getenv(
        "KAFKA_WEATHER_TOPIC",
        "weather_updates",
    ),
}


# ---------------------------------------------------------------------------
# Producer configuration
# ---------------------------------------------------------------------------

PRODUCER_CONFIG = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "linger_ms": 10,
    "compression_type": "gzip",
}


# ---------------------------------------------------------------------------
# Consumer configuration
# ---------------------------------------------------------------------------

CONSUMER_CONFIG = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,
}


# ---------------------------------------------------------------------------
# Consumer groups
# ---------------------------------------------------------------------------

CONSUMER_GROUPS: Dict[str, str] = {
    "gps": "logistics-gps-consumer",
    "orders": "logistics-orders-consumer",
    "delivery": "logistics-delivery-consumer",
}


def get_topic(topic_name: str) -> str:
    """
    Return a configured Kafka topic by logical name.

    Example:
        get_topic("orders") -> "orders"
    """
    if topic_name not in KAFKA_TOPICS:
        valid_topics = ", ".join(KAFKA_TOPICS.keys())
        raise KeyError(
            f"Unknown Kafka topic '{topic_name}'. "
            f"Valid topics: {valid_topics}"
        )

    return KAFKA_TOPICS[topic_name]


def print_config() -> None:
    """
    Print the active Kafka configuration for local debugging.
    """
    print("Kafka Configuration")
    print("-" * 30)
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print("Topics:")

    for logical_name, topic in KAFKA_TOPICS.items():
        print(f"  {logical_name}: {topic}")

    print("Consumer groups:")

    for name, group in CONSUMER_GROUPS.items():
        print(f"  {name}: {group}")


if __name__ == "__main__":
    print_config()

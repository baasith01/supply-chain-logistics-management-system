"""
Batch ingestion module for orders.

Purpose:
    Read order data from a CSV source, perform basic ingestion-time
    validation, and write the ingested data to the project's raw-data layer.

Pipeline position:
    Source CSV -> orders_ingestion.py -> data/raw/orders.csv
    -> Spark / Hive / dbt / Analytics

The module is intentionally lightweight: detailed transformation and
feature engineering are handled later in data/processed and Spark/dbt.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd


REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "warehouse_id",
    "driver_id",
    "vehicle_id",
    "order_date",
    "region",
    "order_status",
    "delivery_status",
    "order_value",
    "delivery_distance_km",
    "promised_delivery_time_min",
    "actual_delivery_time_min",
]

logger = logging.getLogger(__name__)


def validate_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the minimum structure required for order ingestion.

    Returns:
        The validated DataFrame.

    Raises:
        ValueError: If required columns are missing or no valid records exist.
    """
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]

    if missing:
        raise ValueError(
            f"Missing required order columns: {', '.join(missing)}"
        )

    if df.empty:
        raise ValueError("Order source contains no records.")

    # Remove exact duplicate records during ingestion.
    df = df.drop_duplicates().copy()

    # Normalize column names and basic string values.
    df.columns = [column.strip().lower() for column in df.columns]

    text_columns = [
        "order_id",
        "customer_id",
        "warehouse_id",
        "driver_id",
        "vehicle_id",
        "region",
        "order_status",
        "delivery_status",
    ]

    for column in text_columns:
        df[column] = df[column].astype("string").str.strip()

    # Parse dates and numeric fields so downstream jobs receive predictable types.
    df["order_date"] = pd.to_datetime(
        df["order_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    numeric_columns = [
        "order_value",
        "delivery_distance_km",
        "promised_delivery_time_min",
        "actual_delivery_time_min",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Reject records without the primary business identifier or date.
    before = len(df)
    df = df.dropna(subset=["order_id", "order_date"]).copy()

    if len(df) < before:
        logger.warning(
            "Dropped %d records with missing order_id/order_date.",
            before - len(df),
        )

    # Keep one record per order ID at the ingestion layer.
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"]).copy()

    if len(df) < before:
        logger.info(
            "Removed %d duplicate order IDs.",
            before - len(df),
        )

    if df.empty:
        raise ValueError("No valid orders remain after validation.")

    return df


def ingest_orders(
    source_path: str,
    destination_path: str = "data/raw/orders.csv",
) -> pd.DataFrame:
    """
    Read, validate, and store order data.

    Args:
        source_path: Path to the source CSV.
        destination_path: Path in the raw-data layer.

    Returns:
        The ingested DataFrame.
    """
    source = Path(source_path)
    destination = Path(destination_path)

    if not source.exists():
        raise FileNotFoundError(f"Order source not found: {source}")

    logger.info("Reading orders from %s", source)
    df = pd.read_csv(source)

    df = validate_orders(df)

    destination.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destination, index=False)

    logger.info(
        "Order ingestion completed: %d records written to %s",
        len(df),
        destination,
    )

    return df


def main(source_path: str, destination_path: str) -> None:
    """Run the order batch-ingestion job."""
    try:
        df = ingest_orders(source_path, destination_path)

        print("Order ingestion completed successfully.")
        print(f"Records ingested: {len(df):,}")
        print(f"Output: {destination_path}")

    except (FileNotFoundError, ValueError, pd.errors.ParserError) as exc:
        logger.error("Order ingestion failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Batch ingest logistics order data."
    )

    parser.add_argument(
        "--source",
        default="data/source/orders.csv",
        help="Source order CSV path.",
    )

    parser.add_argument(
        "--destination",
        default="data/raw/orders.csv",
        help="Destination path in the raw data layer.",
    )

    args = parser.parse_args()

    main(
        source_path=args.source,
        destination_path=args.destination,
    )

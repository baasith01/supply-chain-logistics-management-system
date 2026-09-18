"""
Batch ingestion module for customers.

Purpose:
    Read customer master data from a CSV source, validate its structure,
    remove duplicate customer records, standardize basic fields, and write
    the result to the raw-data layer.

Pipeline position:
    Source CSV -> customer_ingestion.py -> data/raw/customers.csv
    -> Spark / Hive / dbt / Analytics
"""

import argparse
import logging
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = [
    "customer_id",
    "customer_name",
    "region",
    "customer_type",
    "registration_date",
]

logger = logging.getLogger(__name__)


def validate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and standardize customer master data.
    """
    # Normalize column names before checking the schema.
    df.columns = [column.strip().lower() for column in df.columns]

    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]

    if missing:
        raise ValueError(
            f"Missing required customer columns: {', '.join(missing)}"
        )

    if df.empty:
        raise ValueError("Customer source contains no records.")

    df = df.drop_duplicates().copy()

    # Standardize text fields.
    text_columns = [
        "customer_id",
        "customer_name",
        "region",
        "customer_type",
    ]

    for column in text_columns:
        df[column] = df[column].astype("string").str.strip()

    df["region"] = df["region"].str.title()
    df["customer_type"] = df["customer_type"].str.title()

    # Standardize registration date.
    df["registration_date"] = pd.to_datetime(
        df["registration_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # Customer ID is the primary business key.
    before = len(df)
    df = df.dropna(subset=["customer_id"]).copy()

    if len(df) < before:
        logger.warning(
            "Dropped %d records with missing customer_id.",
            before - len(df),
        )

    before = len(df)
    df = df.drop_duplicates(subset=["customer_id"]).copy()

    if len(df) < before:
        logger.info(
            "Removed %d duplicate customer IDs.",
            before - len(df),
        )

    if df.empty:
        raise ValueError("No valid customers remain after validation.")

    return df


def ingest_customers(
    source_path: str,
    destination_path: str = "data/raw/customers.csv",
) -> pd.DataFrame:
    """
    Read, validate, and store customer data.

    Args:
        source_path: Path to the source CSV.
        destination_path: Destination in the raw-data layer.

    Returns:
        The ingested customer DataFrame.
    """
    source = Path(source_path)
    destination = Path(destination_path)

    if not source.exists():
        raise FileNotFoundError(f"Customer source not found: {source}")

    logger.info("Reading customers from %s", source)

    df = pd.read_csv(source)
    df = validate_customers(df)

    destination.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destination, index=False)

    logger.info(
        "Customer ingestion completed: %d records written to %s",
        len(df),
        destination,
    )

    return df


def main(source_path: str, destination_path: str) -> None:
    """Run the customer batch-ingestion job."""
    try:
        df = ingest_customers(source_path, destination_path)

        print("Customer ingestion completed successfully.")
        print(f"Records ingested: {len(df):,}")
        print(f"Output: {destination_path}")

    except (FileNotFoundError, ValueError, pd.errors.ParserError) as exc:
        logger.error("Customer ingestion failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Batch ingest logistics customer data."
    )

    parser.add_argument(
        "--source",
        default="data/source/customers.csv",
        help="Source customer CSV path.",
    )

    parser.add_argument(
        "--destination",
        default="data/raw/customers.csv",
        help="Destination path in the raw data layer.",
    )

    args = parser.parse_args()

    main(
        source_path=args.source,
        destination_path=args.destination,
    )

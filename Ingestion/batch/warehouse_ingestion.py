"""
Batch ingestion module for warehouses.

Purpose:
    Read warehouse master data from a CSV source, validate geographic and
    operational fields, remove duplicate warehouse records, standardize
    values, and write the result to the raw-data layer.

Pipeline position:
    Source CSV -> warehouse_ingestion.py -> data/raw/warehouses.csv
    -> Spark / Hive / dbt / Analytics
"""

import argparse
import logging
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = [
    "warehouse_id",
    "warehouse_name",
    "region",
    "capacity_units",
    "latitude",
    "longitude",
]

logger = logging.getLogger(__name__)


def validate_warehouses(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and standardize warehouse master data.
    """
    # Normalize column names before schema validation.
    df.columns = [column.strip().lower() for column in df.columns]

    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]

    if missing:
        raise ValueError(
            f"Missing required warehouse columns: {', '.join(missing)}"
        )

    if df.empty:
        raise ValueError("Warehouse source contains no records.")

    df = df.drop_duplicates().copy()

    # Standardize text fields.
    text_columns = [
        "warehouse_id",
        "warehouse_name",
        "region",
    ]

    for column in text_columns:
        df[column] = df[column].astype("string").str.strip()

    df["region"] = df["region"].str.title()

    # Convert operational and geographic fields.
    numeric_columns = [
        "capacity_units",
        "latitude",
        "longitude",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Validate coordinates.
    df.loc[~df["latitude"].between(-90, 90), "latitude"] = pd.NA
    df.loc[~df["longitude"].between(-180, 180), "longitude"] = pd.NA

    # Capacity cannot be negative.
    df.loc[df["capacity_units"] < 0, "capacity_units"] = pd.NA

    # Required business key and location fields.
    before = len(df)
    df = df.dropna(
        subset=[
            "warehouse_id",
            "warehouse_name",
            "region",
            "capacity_units",
            "latitude",
            "longitude",
        ]
    ).copy()

    if len(df) < before:
        logger.warning(
            "Dropped %d invalid warehouse records.",
            before - len(df),
        )

    # One master record per warehouse.
    before = len(df)
    df = df.drop_duplicates(subset=["warehouse_id"]).copy()

    if len(df) < before:
        logger.info(
            "Removed %d duplicate warehouse IDs.",
            before - len(df),
        )

    # Clean precision for storage.
    df["capacity_units"] = df["capacity_units"].round().astype("int64")
    df["latitude"] = df["latitude"].round(6)
    df["longitude"] = df["longitude"].round(6)

    if df.empty:
        raise ValueError("No valid warehouses remain after validation.")

    return df


def ingest_warehouses(
    source_path: str,
    destination_path: str = "data/raw/warehouses.csv",
) -> pd.DataFrame:
    """
    Read, validate, and store warehouse master data.

    Args:
        source_path: Path to the source CSV.
        destination_path: Destination in the raw-data layer.

    Returns:
        The ingested warehouse DataFrame.
    """
    source = Path(source_path)
    destination = Path(destination_path)

    if not source.exists():
        raise FileNotFoundError(f"Warehouse source not found: {source}")

    logger.info("Reading warehouses from %s", source)

    df = pd.read_csv(source)
    df = validate_warehouses(df)

    destination.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destination, index=False)

    logger.info(
        "Warehouse ingestion completed: %d records written to %s",
        len(df),
        destination,
    )

    return df


def main(source_path: str, destination_path: str) -> None:
    """Run the warehouse batch-ingestion job."""
    try:
        df = ingest_warehouses(source_path, destination_path)

        print("Warehouse ingestion completed successfully.")
        print(f"Records ingested: {len(df):,}")
        print(f"Output: {destination_path}")

    except (FileNotFoundError, ValueError, pd.errors.ParserError) as exc:
        logger.error("Warehouse ingestion failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Batch ingest logistics warehouse data."
    )

    parser.add_argument(
        "--source",
        default="data/source/warehouses.csv",
        help="Source warehouse CSV path.",
    )

    parser.add_argument(
        "--destination",
        default="data/raw/warehouses.csv",
        help="Destination path in the raw data layer.",
    )

    args = parser.parse_args()

    main(
        source_path=args.source,
        destination_path=args.destination,
    )

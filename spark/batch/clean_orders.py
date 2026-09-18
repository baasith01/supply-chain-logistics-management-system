"""
Spark batch job: clean_orders.py

Cleans data/raw/orders.csv and writes an analytical dataset to
data/processed/orders_clean.csv.

Run:
    spark-submit spark/batch/clean_orders.py
"""

from pathlib import Path
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "orders.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "orders_clean.csv"

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


def create_spark_session() -> SparkSession:
    """Create the local Spark session."""
    return (
        SparkSession.builder
        .appName("Logistics-Clean-Orders")
        .master("local[*]")
        .getOrCreate()
    )


def validate_columns(df: DataFrame) -> None:
    """Ensure the source contains all expected columns."""
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns: " + ", ".join(missing)
        )


def clean_orders(df: DataFrame) -> DataFrame:
    """Clean, standardize, deduplicate, and enrich order data."""

    string_columns = [
        "order_id",
        "customer_id",
        "warehouse_id",
        "driver_id",
        "vehicle_id",
        "region",
        "order_status",
        "delivery_status",
    ]

    numeric_columns = [
        "order_value",
        "delivery_distance_km",
        "promised_delivery_time_min",
        "actual_delivery_time_min",
    ]

    cleaned = df

    for column in string_columns:
        cleaned = cleaned.withColumn(column, F.trim(F.col(column)))

    cleaned = (
        cleaned
        .withColumn("region", F.upper(F.col("region")))
        .withColumn("order_status", F.upper(F.col("order_status")))
        .withColumn("delivery_status", F.upper(F.col("delivery_status")))
        .withColumn("order_date", F.to_date(F.col("order_date")))
    )

    for column in numeric_columns:
        cleaned = cleaned.withColumn(
            column,
            F.col(column).cast("double")
        )

    cleaned = (
        cleaned
        .dropDuplicates(["order_id"])
        .withColumn(
            "delivery_delay_min",
            F.round(
                F.col("actual_delivery_time_min")
                - F.col("promised_delivery_time_min"),
                2,
            ),
        )
        .withColumn(
            "is_delayed",
            F.when(F.col("delivery_delay_min") > 0, 1).otherwise(0),
        )
        .withColumn(
            "delivery_per_km_value",
            F.when(
                F.col("delivery_distance_km") > 0,
                F.round(
                    F.col("order_value") / F.col("delivery_distance_km"),
                    2,
                ),
            ),
        )
    )

    return cleaned


def validate_data(df: DataFrame) -> None:
    """Run basic data-quality checks."""
    invalid_ids = df.filter(
        F.col("order_id").isNull() | (F.col("order_id") == "")
    ).count()

    invalid_dates = df.filter(
        F.col("order_date").isNull()
    ).count()

    negative_values = df.filter(
        (F.col("order_value") < 0)
        | (F.col("delivery_distance_km") < 0)
        | (F.col("promised_delivery_time_min") < 0)
        | (F.col("actual_delivery_time_min") < 0)
    ).count()

    if invalid_ids:
        raise ValueError(f"Invalid order IDs found: {invalid_ids}")

    if invalid_dates:
        raise ValueError(f"Invalid order dates found: {invalid_dates}")

    if negative_values:
        raise ValueError(
            f"Rows with negative numeric values found: {negative_values}"
        )


def main() -> int:
    """Run the batch cleaning job."""
    spark = create_spark_session()

    try:
        if not INPUT_PATH.exists():
            raise FileNotFoundError(
                f"Input file not found: {INPUT_PATH}"
            )

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        print(f"Reading source: {INPUT_PATH}")

        raw_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(INPUT_PATH))
        )

        validate_columns(raw_df)

        print(f"Raw rows: {raw_df.count():,}")

        cleaned_df = clean_orders(raw_df)

        validate_data(cleaned_df)

        print(f"Cleaned rows: {cleaned_df.count():,}")

        # Spark CSV writes to a directory rather than a single file.
        # coalesce(1) keeps this demo-sized project easy to inspect.
        temp_output = OUTPUT_PATH.parent / "_orders_clean_spark"

        (
            cleaned_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(str(temp_output))
        )

        part_files = list(temp_output.glob("part-*.csv"))

        if not part_files:
            raise RuntimeError(
                "Spark completed but no output CSV part file was created."
            )

        if OUTPUT_PATH.exists():
            OUTPUT_PATH.unlink()

        part_files[0].replace(OUTPUT_PATH)

        for file_path in temp_output.glob("*"):
            if file_path.is_file():
                file_path.unlink()

        temp_output.rmdir()

        print(f"Output written to: {OUTPUT_PATH}")
        print("Orders cleaning completed successfully.")

        return 0

    except Exception as exc:
        print(f"Orders cleaning failed: {exc}", file=sys.stderr)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

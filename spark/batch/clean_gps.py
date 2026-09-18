"""
Spark batch job: clean_gps.py

Cleans vehicle GPS telemetry and creates movement/quality features.

Input:
    data/raw/gps_tracking.csv

Output:
    data/processed/gps_clean.csv

Run:
    spark-submit spark/batch/clean_gps.py
"""

from pathlib import Path
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "gps_tracking.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "gps_clean.csv"

REQUIRED_COLUMNS = [
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


def create_spark_session() -> SparkSession:
    """Create the local Spark session."""
    return (
        SparkSession.builder
        .appName("Logistics-Clean-GPS")
        .master("local[*]")
        .getOrCreate()
    )


def validate_columns(df: DataFrame) -> None:
    """Ensure the GPS source contains all expected columns."""
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]

    if missing:
        raise ValueError(
            "Missing required columns: " + ", ".join(missing)
        )


def clean_gps(df: DataFrame) -> DataFrame:
    """Clean GPS data and create telemetry-quality features."""

    string_columns = [
        "vehicle_id",
        "region",
        "vehicle_status",
        "ignition_status",
        "signal_quality",
    ]

    numeric_columns = [
        "latitude",
        "longitude",
        "speed_kmph",
        "heading",
    ]

    cleaned = df

    for column in string_columns:
        cleaned = cleaned.withColumn(column, F.trim(F.col(column)))

    cleaned = (
        cleaned
        .withColumn("region", F.upper(F.col("region")))
        .withColumn("vehicle_status", F.upper(F.col("vehicle_status")))
        .withColumn("ignition_status", F.upper(F.col("ignition_status")))
        .withColumn("signal_quality", F.upper(F.col("signal_quality")))
        .withColumn(
            "timestamp",
            F.to_timestamp(F.col("timestamp"))
        )
    )

    for column in numeric_columns:
        cleaned = cleaned.withColumn(
            column,
            F.col(column).cast("double")
        )

    cleaned = (
        cleaned
        .dropDuplicates(["vehicle_id", "timestamp"])
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("day_of_week", F.date_format("timestamp", "E"))
        .withColumn(
            "is_moving",
            F.when(F.col("speed_kmph") > 0, 1).otherwise(0),
        )
        .withColumn(
            "is_low_signal",
            F.when(
                F.col("signal_quality").isin("LOW", "POOR"),
                1,
            ).otherwise(0),
        )
    )

    return cleaned


def validate_data(df: DataFrame) -> None:
    """Run basic GPS data-quality checks."""

    invalid_vehicle_ids = df.filter(
        F.col("vehicle_id").isNull() | (F.col("vehicle_id") == "")
    ).count()

    invalid_timestamps = df.filter(
        F.col("timestamp").isNull()
    ).count()

    invalid_coordinates = df.filter(
        F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | (F.col("latitude") < -90)
        | (F.col("latitude") > 90)
        | (F.col("longitude") < -180)
        | (F.col("longitude") > 180)
    ).count()

    invalid_speed = df.filter(
        F.col("speed_kmph") < 0
    ).count()

    invalid_heading = df.filter(
        (F.col("heading") < 0) | (F.col("heading") > 360)
    ).count()

    if invalid_vehicle_ids:
        raise ValueError(
            f"Invalid vehicle IDs found: {invalid_vehicle_ids}"
        )

    if invalid_timestamps:
        raise ValueError(
            f"Invalid timestamps found: {invalid_timestamps}"
        )

    if invalid_coordinates:
        raise ValueError(
            f"Invalid GPS coordinates found: {invalid_coordinates}"
        )

    if invalid_speed:
        raise ValueError(
            f"Negative speed values found: {invalid_speed}"
        )

    if invalid_heading:
        raise ValueError(
            f"Invalid heading values found: {invalid_heading}"
        )


def main() -> int:
    """Run the GPS batch-cleaning job."""
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

        cleaned_df = clean_gps(raw_df)

        validate_data(cleaned_df)

        print(f"Cleaned rows: {cleaned_df.count():,}")

        # Spark writes CSV output as a directory. For this demo-sized
        # project, coalesce(1) keeps the generated output easy to inspect.
        temp_output = OUTPUT_PATH.parent / "_gps_clean_spark"

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
        print("GPS cleaning completed successfully.")

        return 0

    except Exception as exc:
        print(f"GPS cleaning failed: {exc}", file=sys.stderr)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

"""
Spark batch job: clean_weather.py

Cleans weather observations and creates weather-risk features.

Input:
    data/raw/weather.csv

Output:
    data/processed/weather_clean.csv

Run:
    spark-submit spark/batch/clean_weather.py
"""

from pathlib import Path
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "weather.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "weather_clean.csv"

REQUIRED_COLUMNS = [
    "date",
    "location",
    "latitude",
    "longitude",
    "temperature_c",
    "humidity_pct",
    "rainfall_mm",
    "weather_condition",
]


def create_spark_session() -> SparkSession:
    """Create the local Spark session."""
    return (
        SparkSession.builder
        .appName("Logistics-Clean-Weather")
        .master("local[*]")
        .getOrCreate()
    )


def validate_columns(df: DataFrame) -> None:
    """Ensure the source contains all expected weather columns."""
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]

    if missing:
        raise ValueError(
            "Missing required columns: " + ", ".join(missing)
        )


def clean_weather(df: DataFrame) -> DataFrame:
    """Clean weather data and create analytical risk features."""

    cleaned = (
        df
        .withColumn("location", F.trim(F.col("location")))
        .withColumn(
            "weather_condition",
            F.upper(F.trim(F.col("weather_condition"))),
        )
        .withColumn("date", F.to_date(F.col("date")))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("temperature_c", F.col("temperature_c").cast("double"))
        .withColumn("humidity_pct", F.col("humidity_pct").cast("double"))
        .withColumn("rainfall_mm", F.col("rainfall_mm").cast("double"))
        .dropDuplicates(["date", "location"])
        .withColumn(
            "is_rainy",
            F.when(
                (F.col("rainfall_mm") > 0)
                | F.col("weather_condition").rlike("RAIN|DRIZZLE|STORM"),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "heavy_rain_flag",
            F.when(F.col("rainfall_mm") >= 10, 1).otherwise(0),
        )
        .withColumn(
            "high_humidity_flag",
            F.when(F.col("humidity_pct") >= 80, 1).otherwise(0),
        )
        .withColumn(
            "weather_risk",
            F.when(
                (F.col("rainfall_mm") >= 20)
                | F.col("weather_condition").rlike("STORM|THUNDER"),
                "HIGH",
            )
            .when(
                (F.col("rainfall_mm") >= 5)
                | (F.col("humidity_pct") >= 80)
                | F.col("weather_condition").rlike("RAIN|DRIZZLE"),
                "MEDIUM",
            )
            .otherwise("LOW"),
        )
    )

    return cleaned


def validate_data(df: DataFrame) -> None:
    """Run basic weather data-quality checks."""

    invalid_dates = df.filter(F.col("date").isNull()).count()

    invalid_coordinates = df.filter(
        F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | (F.col("latitude") < -90)
        | (F.col("latitude") > 90)
        | (F.col("longitude") < -180)
        | (F.col("longitude") > 180)
    ).count()

    invalid_weather_values = df.filter(
        (F.col("humidity_pct") < 0)
        | (F.col("humidity_pct") > 100)
        | (F.col("rainfall_mm") < 0)
    ).count()

    if invalid_dates:
        raise ValueError(
            f"Invalid weather dates found: {invalid_dates}"
        )

    if invalid_coordinates:
        raise ValueError(
            f"Invalid weather coordinates found: {invalid_coordinates}"
        )

    if invalid_weather_values:
        raise ValueError(
            f"Invalid humidity/rainfall values found: {invalid_weather_values}"
        )


def main() -> int:
    """Run the weather batch-cleaning job."""
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

        cleaned_df = clean_weather(raw_df)

        validate_data(cleaned_df)

        print(f"Cleaned rows: {cleaned_df.count():,}")

        # Spark writes CSV output as a directory. coalesce(1) keeps the
        # demo-sized project output easy to inspect.
        temp_output = OUTPUT_PATH.parent / "_weather_clean_spark"

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
        print("Weather cleaning completed successfully.")

        return 0

    except Exception as exc:
        print(f"Weather cleaning failed: {exc}", file=sys.stderr)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

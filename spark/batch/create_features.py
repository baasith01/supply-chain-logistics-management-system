"""
Spark batch job: create_features.py

Creates an integrated delivery-feature dataset by combining cleaned order
data with cleaned weather data.

Inputs:
    data/processed/orders_clean.csv
    data/processed/weather_clean.csv

Output:
    data/processed/delivery_features.csv

Run:
    spark-submit spark/batch/create_features.py
"""

from pathlib import Path
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]

ORDERS_PATH = PROJECT_ROOT / "data" / "processed" / "orders_clean.csv"
WEATHER_PATH = PROJECT_ROOT / "data" / "processed" / "weather_clean.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "delivery_features.csv"


def create_spark_session() -> SparkSession:
    """Create the local Spark session."""
    return (
        SparkSession.builder
        .appName("Logistics-Create-Delivery-Features")
        .master("local[*]")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Read a CSV dataset with a header."""
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(path))
    )


def create_features(
    orders_df: DataFrame,
    weather_df: DataFrame,
) -> DataFrame:
    """Join orders with weather and create analytical features."""

    # Weather is daily and location-based, so aggregate to one record per
    # date/location before joining to avoid multiplying order rows.
    weather_daily = (
        weather_df
        .groupBy("date", "location")
        .agg(
            F.round(F.avg("temperature_c"), 2).alias("temperature_c"),
            F.round(F.avg("humidity_pct"), 2).alias("humidity_pct"),
            F.round(F.sum("rainfall_mm"), 2).alias("rainfall_mm"),
            F.max("is_rainy").alias("is_rainy"),
            F.max("heavy_rain_flag").alias("heavy_rain_flag"),
            F.max("high_humidity_flag").alias("high_humidity_flag"),
            F.max(
                F.when(F.col("weather_risk") == "HIGH", 3)
                .when(F.col("weather_risk") == "MEDIUM", 2)
                .otherwise(1)
            ).alias("weather_risk_level"),
        )
        .withColumn(
            "weather_risk",
            F.when(F.col("weather_risk_level") == 3, "HIGH")
            .when(F.col("weather_risk_level") == 2, "MEDIUM")
            .otherwise("LOW"),
        )
        .drop("weather_risk_level")
    )

    weather_daily = weather_daily.withColumnRenamed(
        "location",
        "weather_location",
    )

    orders = (
        orders_df
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("region", F.upper(F.trim("region")))
    )

    features = (
        orders.alias("o")
        .join(
            weather_daily.alias("w"),
            (F.col("o.order_date") == F.col("w.date"))
            & (F.col("o.region") == F.col("w.weather_location")),
            "left",
        )
        .select(
            "o.*",
            F.col("w.temperature_c"),
            F.col("w.humidity_pct"),
            F.col("w.rainfall_mm"),
            F.col("w.is_rainy"),
            F.col("w.heavy_rain_flag"),
            F.col("w.high_humidity_flag"),
            F.col("w.weather_risk"),
        )
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
        .withColumn("order_day_of_week", F.dayofweek("order_date"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("order_date").isin(1, 7), 1).otherwise(0),
        )
        .withColumn(
            "delay_ratio",
            F.when(
                F.col("promised_delivery_time_min") > 0,
                F.round(
                    F.col("delivery_delay_min")
                    / F.col("promised_delivery_time_min"),
                    4,
                ),
            ),
        )
        .withColumn(
            "delivery_efficiency",
            F.when(
                F.col("actual_delivery_time_min") > 0,
                F.round(
                    F.col("delivery_distance_km")
                    / F.col("actual_delivery_time_min"),
                    4,
                ),
            ),
        )
        .withColumn(
            "distance_bucket",
            F.when(F.col("delivery_distance_km") < 5, "0-5 km")
            .when(F.col("delivery_distance_km") < 10, "5-10 km")
            .when(F.col("delivery_distance_km") < 20, "10-20 km")
            .when(F.col("delivery_distance_km") < 50, "20-50 km")
            .otherwise("50+ km"),
        )
        .withColumn(
            "order_value_bucket",
            F.when(F.col("order_value") < 500, "Low")
            .when(F.col("order_value") < 1500, "Medium")
            .otherwise("High"),
        )
        .withColumn(
            "delivery_risk",
            F.when(
                (F.col("is_delayed") == 1)
                & (
                    (F.col("delivery_delay_min") >= 30)
                    | (F.col("heavy_rain_flag") == 1)
                ),
                "HIGH",
            )
            .when(
                (F.col("is_delayed") == 1)
                | (F.col("is_rainy") == 1),
                "MEDIUM",
            )
            .otherwise("LOW"),
        )
        .withColumn(
            "weather_delay_risk",
            F.when(
                (F.col("is_delayed") == 1)
                & (F.col("is_rainy") == 1),
                1,
            ).otherwise(0),
        )
    )

    return features


def validate_features(df: DataFrame) -> None:
    """Run basic checks on the generated feature dataset."""

    if df.filter(
        F.col("order_id").isNull() | (F.col("order_id") == "")
    ).count():
        raise ValueError("Feature dataset contains invalid order IDs.")

    if df.filter(
        F.col("delivery_distance_km") < 0
    ).count():
        raise ValueError(
            "Feature dataset contains negative delivery distances."
        )

    if df.filter(
        F.col("order_value") < 0
    ).count():
        raise ValueError(
            "Feature dataset contains negative order values."
        )


def main() -> int:
    """Run the feature-engineering batch job."""
    spark = create_spark_session()

    try:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        print(f"Reading orders:  {ORDERS_PATH}")
        print(f"Reading weather: {WEATHER_PATH}")

        orders_df = read_csv(spark, ORDERS_PATH)
        weather_df = read_csv(spark, WEATHER_PATH)

        print(f"Orders rows:  {orders_df.count():,}")
        print(f"Weather rows: {weather_df.count():,}")

        features_df = create_features(orders_df, weather_df)

        validate_features(features_df)

        print(f"Feature rows: {features_df.count():,}")

        # Spark writes CSV output as a directory. For this demo-sized
        # project, coalesce(1) keeps the result easy to inspect.
        temp_output = OUTPUT_PATH.parent / "_delivery_features_spark"

        (
            features_df
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
        print("Feature creation completed successfully.")

        return 0

    except Exception as exc:
        print(f"Feature creation failed: {exc}", file=sys.stderr)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

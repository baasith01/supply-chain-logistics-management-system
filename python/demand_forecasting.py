"""
Demand Forecasting - Logistics & Supply Chain Intelligence Platform

Purpose:
    Forecast daily order demand using historical order volume.

Model:
    Linear Regression with calendar features.

Inputs:
    Preferred:
        data/processed/orders_clean.csv

    Fallback:
        data/raw/orders.csv

Outputs:
    analytics/output/forecasting/
        daily_demand.csv
        demand_forecast.csv
        model_metrics.csv
        forecast_summary.csv
        charts/demand_forecast.png

Usage:
    python analytics/python/demand_forecasting.py

Optional:
    python analytics/python/demand_forecasting.py \
        --processed-dir data/processed \
        --raw-dir data/raw \
        --output-dir analytics/output/forecasting \
        --forecast-days 30

Important:
    This is a baseline forecasting model intended for the project and
    interview demonstration. It is not presented as a production-grade
    time-series model. A production implementation could evaluate models
    such as Prophet, ARIMA/SARIMA, gradient boosting, or dedicated
    forecasting platforms.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Forecast daily logistics order demand."
    )
    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Directory containing cleaned datasets.",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Fallback directory containing raw datasets.",
    )
    parser.add_argument(
        "--output-dir",
        default="analytics/output/forecasting",
        help="Directory for forecast outputs.",
    )
    parser.add_argument(
        "--forecast-days",
        type=int,
        default=30,
        help="Number of future days to forecast.",
    )
    return parser.parse_args()


def load_orders(
    processed_dir: Path,
    raw_dir: Path,
) -> pd.DataFrame:
    """Load orders, preferring the cleaned dataset."""
    processed_path = processed_dir / "orders_clean.csv"
    raw_path = raw_dir / "orders.csv"

    if processed_path.exists():
        print(f"[OK] Loading processed orders: {processed_path}")
        return pd.read_csv(processed_path)

    if raw_path.exists():
        print(f"[WARN] Processed orders missing; using raw orders: {raw_path}")
        return pd.read_csv(raw_path)

    raise FileNotFoundError(
        "Neither data/processed/orders_clean.csv nor "
        "data/raw/orders.csv was found."
    )


def prepare_daily_demand(
    orders: pd.DataFrame,
) -> pd.DataFrame:
    """Convert order records into one row per calendar day."""
    required = {"order_id", "order_date"}

    missing = required - set(orders.columns)
    if missing:
        raise ValueError(
            f"Orders dataset is missing required columns: {sorted(missing)}"
        )

    df = orders.copy()

    df["order_date"] = pd.to_datetime(
        df["order_date"],
        errors="coerce",
    )

    df = df.dropna(subset=["order_date"])

    # Normalize to calendar date.
    df["date"] = df["order_date"].dt.normalize()

    daily = (
        df.groupby("date")
        .agg(
            order_count=("order_id", "count"),
        )
        .reset_index()
        .sort_values("date")
    )

    if daily.empty:
        raise ValueError("No valid order dates were available for forecasting.")

    # Fill missing calendar days so the model sees continuous time.
    full_dates = pd.date_range(
        daily["date"].min(),
        daily["date"].max(),
        freq="D",
    )

    daily = (
        daily.set_index("date")
        .reindex(full_dates, fill_value=0)
        .rename_axis("date")
        .reset_index()
    )

    daily["order_count"] = daily["order_count"].astype(int)

    return daily


def add_time_features(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
) -> pd.DataFrame:
    """Create calendar and trend features."""
    result = df.copy()

    result["day_index"] = (
        result["date"] - start_date
    ).dt.days

    result["day_of_week"] = result["date"].dt.dayofweek
    result["day_of_month"] = result["date"].dt.day
    result["month"] = result["date"].dt.month
    result["quarter"] = result["date"].dt.quarter
    result["week_of_year"] = (
        result["date"].dt.isocalendar().week.astype(int)
    )
    result["is_weekend"] = (
        result["day_of_week"] >= 5
    ).astype(int)

    # Cyclical encoding helps represent weekly seasonality.
    result["dow_sin"] = np.sin(
        2 * np.pi * result["day_of_week"] / 7
    )
    result["dow_cos"] = np.cos(
        2 * np.pi * result["day_of_week"] / 7
    )

    result["month_sin"] = np.sin(
        2 * np.pi * result["month"] / 12
    )
    result["month_cos"] = np.cos(
        2 * np.pi * result["month"] / 12
    )

    return result


def create_train_test_split(
    data: pd.DataFrame,
    test_ratio: float = 0.20,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create a chronological train/test split.

    Time-series data must not be randomly shuffled because that can leak
    future information into the training set.
    """
    if len(data) < 10:
        raise ValueError(
            "At least 10 daily observations are recommended for forecasting."
        )

    test_size = max(
        1,
        int(np.ceil(len(data) * test_ratio)),
    )

    if test_size >= len(data):
        test_size = 1

    train = data.iloc[:-test_size].copy()
    test = data.iloc[-test_size:].copy()

    return train, test


def train_model(
    train: pd.DataFrame,
    feature_columns: list[str],
) -> LinearRegression:
    """Train the baseline linear regression model."""
    model = LinearRegression()

    X_train = train[feature_columns]
    y_train = train["order_count"]

    model.fit(
        X_train,
        y_train,
    )

    return model


def evaluate_model(
    model: LinearRegression,
    test: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Generate predictions and evaluation metrics."""
    X_test = test[feature_columns]
    y_test = test["order_count"]

    predictions = model.predict(X_test)

    result = test[
        ["date", "order_count"]
    ].copy()

    result["predicted_orders"] = predictions.clip(
        lower=0
    ).round(2)

    mae = mean_absolute_error(
        y_test,
        predictions,
    )

    rmse = np.sqrt(
        mean_squared_error(
            y_test,
            predictions,
        )
    )

    r2 = r2_score(
        y_test,
        predictions,
    )

    metrics = {
        "mae": float(mae),
        "rmse": float(rmse),
        "r2": float(r2),
        "test_days": int(len(test)),
    }

    return result, metrics


def build_future_dates(
    last_date: pd.Timestamp,
    forecast_days: int,
) -> pd.DataFrame:
    """Create future calendar dates."""
    if forecast_days <= 0:
        raise ValueError("--forecast-days must be greater than zero.")

    dates = pd.date_range(
        last_date + pd.Timedelta(days=1),
        periods=forecast_days,
        freq="D",
    )

    return pd.DataFrame(
        {
            "date": dates,
        }
    )


def create_forecast(
    model: LinearRegression,
    future: pd.DataFrame,
    feature_columns: list[str],
    start_date: pd.Timestamp,
) -> pd.DataFrame:
    """Generate future demand predictions."""
    future = add_time_features(
        future,
        start_date,
    )

    predictions = model.predict(
        future[feature_columns]
    )

    forecast = future[
        [
            "date",
            "day_of_week",
            "month",
            "is_weekend",
        ]
    ].copy()

    forecast["predicted_orders"] = (
        np.maximum(predictions, 0)
        .round()
        .astype(int)
    )

    forecast["demand_level"] = pd.cut(
        forecast["predicted_orders"],
        bins=[
            -np.inf,
            100,
            200,
            400,
            np.inf,
        ],
        labels=[
            "Low",
            "Moderate",
            "High",
            "Very High",
        ],
    )

    return forecast


def create_summary(
    daily: pd.DataFrame,
    forecast: pd.DataFrame,
    metrics: dict[str, float],
) -> pd.DataFrame:
    """Create executive-friendly forecasting summary."""
    historical_average = float(
        daily["order_count"].mean()
    )

    historical_peak = int(
        daily["order_count"].max()
    )

    forecast_average = float(
        forecast["predicted_orders"].mean()
    )

    forecast_peak = int(
        forecast["predicted_orders"].max()
    )

    summary = pd.DataFrame(
        [
            {
                "historical_days": len(daily),
                "historical_total_orders": int(
                    daily["order_count"].sum()
                ),
                "historical_average_daily_orders": round(
                    historical_average,
                    2,
                ),
                "historical_peak_daily_orders": historical_peak,
                "forecast_days": len(forecast),
                "forecast_total_orders": int(
                    forecast["predicted_orders"].sum()
                ),
                "forecast_average_daily_orders": round(
                    forecast_average,
                    2,
                ),
                "forecast_peak_daily_orders": forecast_peak,
                "forecast_mae": round(metrics["mae"], 2),
                "forecast_rmse": round(metrics["rmse"], 2),
                "forecast_r2": round(metrics["r2"], 4),
            }
        ]
    )

    return summary


def save_forecast_chart(
    daily: pd.DataFrame,
    test_predictions: pd.DataFrame,
    forecast: pd.DataFrame,
    charts_dir: Path,
) -> None:
    """Save historical/test/forecast demand visualization."""
    plt.figure(figsize=(12, 6))

    plt.plot(
        daily["date"],
        daily["order_count"],
        label="Historical Orders",
    )

    if not test_predictions.empty:
        plt.plot(
            test_predictions["date"],
            test_predictions["predicted_orders"],
            label="Test Predictions",
        )

    plt.plot(
        forecast["date"],
        forecast["predicted_orders"],
        label="Future Forecast",
        linestyle="--",
    )

    plt.title("Logistics Daily Demand Forecast")
    plt.xlabel("Date")
    plt.ylabel("Orders")
    plt.legend()
    plt.xticks(rotation=30)
    plt.tight_layout()

    plt.savefig(
        charts_dir / "demand_forecast.png",
        dpi=150,
    )
    plt.close()


def run_forecasting(
    processed_dir: Path,
    raw_dir: Path,
    output_dir: Path,
    forecast_days: int,
) -> None:
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    charts_dir = output_dir / "charts"
    charts_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    orders = load_orders(
        processed_dir,
        raw_dir,
    )

    print("[1/5] Preparing daily demand...")
    daily = prepare_daily_demand(orders)

    daily = add_time_features(
        daily,
        daily["date"].min(),
    )

    daily.to_csv(
        output_dir / "daily_demand.csv",
        index=False,
    )

    # Keep the model deliberately small and interpretable.
    feature_columns = [
        "day_index",
        "day_of_week",
        "day_of_month",
        "month",
        "quarter",
        "week_of_year",
        "is_weekend",
        "dow_sin",
        "dow_cos",
        "month_sin",
        "month_cos",
    ]

    print("[2/5] Creating chronological train/test split...")
    train, test = create_train_test_split(daily)

    print("[3/5] Training Linear Regression baseline...")
    model = train_model(
        train,
        feature_columns,
    )

    test_predictions, metrics = evaluate_model(
        model,
        test,
        feature_columns,
    )

    test_predictions.to_csv(
        output_dir / "test_predictions.csv",
        index=False,
    )

    metrics_df = pd.DataFrame(
        [
            {
                "metric": key,
                "value": value,
            }
            for key, value in metrics.items()
        ]
    )

    metrics_df.to_csv(
        output_dir / "model_metrics.csv",
        index=False,
    )

    print("[4/5] Training final model on all historical data...")
    final_model = train_model(
        daily,
        feature_columns,
    )

    future_dates = build_future_dates(
        daily["date"].max(),
        forecast_days,
    )

    forecast = create_forecast(
        final_model,
        future_dates,
        feature_columns,
        daily["date"].min(),
    )

    forecast.to_csv(
        output_dir / "demand_forecast.csv",
        index=False,
    )

    summary = create_summary(
        daily,
        forecast,
        metrics,
    )

    summary.to_csv(
        output_dir / "forecast_summary.csv",
        index=False,
    )

    save_forecast_chart(
        daily,
        test_predictions,
        forecast,
        charts_dir,
    )

    print("[5/5] Forecasting completed.")
    print(f"Historical days: {len(daily):,}")
    print(f"Forecast days: {len(forecast):,}")
    print(f"MAE: {metrics['mae']:.2f}")
    print(f"RMSE: {metrics['rmse']:.2f}")
    print(f"R²: {metrics['r2']:.4f}")
    print(f"Outputs written to: {output_dir.resolve()}")


def main() -> None:
    args = parse_args()

    run_forecasting(
        processed_dir=Path(args.processed_dir),
        raw_dir=Path(args.raw_dir),
        output_dir=Path(args.output_dir),
        forecast_days=args.forecast_days,
    )


if __name__ == "__main__":
    main()

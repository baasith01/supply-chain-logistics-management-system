"""
Delivery Delay Prediction - Logistics & Supply Chain Intelligence Platform

Purpose:
    Predict whether an order is likely to be delayed using information that
    can reasonably be available before or around dispatch.

Target:
    is_delayed
        1 = delayed delivery
        0 = on-time delivery

Model:
    Logistic Regression with preprocessing.

Preferred input:
    analytics/output/features/delivery_features.csv

Fallback inputs:
    data/processed/delivery_features.csv
    data/processed/orders_clean.csv
    data/raw/orders.csv

Outputs:
    analytics/output/prediction/
        delivery_predictions.csv
        model_metrics.csv
        classification_report.csv
        feature_coefficients.csv
        prediction_summary.csv
        charts/confusion_matrix.png
        charts/roc_curve.png

Usage:
    python analytics/python/delivery_prediction.py

Optional:
    python analytics/python/delivery_prediction.py \
        --features-dir analytics/output/features \
        --processed-dir data/processed \
        --raw-dir data/raw \
        --output-dir analytics/output/prediction

Important:
    The model intentionally excludes post-delivery outcome fields such as
    actual_delivery_time_min, delivery_delay_min, is_delayed, and
    delay_severity from predictive features to reduce target leakage.

    This is a project/interview baseline rather than a production-grade
    real-time prediction service.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


TARGET_COLUMN = "is_delayed"

# Fields known only after delivery or directly derived from the target.
LEAKAGE_COLUMNS = {
    "actual_delivery_time_min",
    "delivery_delay_min",
    "is_delayed",
    "delay_severity",
    "delivery_risk",
    "weather_delay_risk",
}

# Identifiers are retained in the final prediction output but not used
# as model features.
IDENTIFIER_COLUMNS = {
    "order_id",
    "customer_id",
    "warehouse_id",
    "driver_id",
    "vehicle_id",
}

# Free-text or raw coordinate fields are intentionally not included in this
# baseline model. They can be engineered separately in a production system.
EXCLUDED_RAW_COLUMNS = {
    "customer_name",
    "driver_name",
    "region",
    "weather_condition",
    "weather_risk_category",
    "order_status",
    "delivery_status",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a baseline model to predict delivery delays."
    )
    parser.add_argument(
        "--features-dir",
        default="analytics/output/features",
        help="Directory containing generated feature datasets.",
    )
    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Directory containing processed datasets.",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Fallback directory containing raw datasets.",
    )
    parser.add_argument(
        "--output-dir",
        default="analytics/output/prediction",
        help="Directory for prediction outputs.",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.20,
        help="Fraction of data reserved for testing.",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducible train/test split.",
    )
    return parser.parse_args()


def load_features(
    features_dir: Path,
    processed_dir: Path,
    raw_dir: Path,
) -> pd.DataFrame:
    """Load the best available delivery feature dataset."""
    candidates = [
        features_dir / "delivery_features.csv",
        processed_dir / "delivery_features.csv",
        processed_dir / "orders_clean.csv",
        raw_dir / "orders.csv",
    ]

    for path in candidates:
        if path.exists():
            print(f"[OK] Loading: {path}")
            return pd.read_csv(path)

    raise FileNotFoundError(
        "No delivery feature dataset was found. Expected one of: "
        "analytics/output/features/delivery_features.csv, "
        "data/processed/delivery_features.csv, "
        "data/processed/orders_clean.csv, "
        "data/raw/orders.csv."
    )


def prepare_target(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Create the binary delay target when it is not already present."""
    result = df.copy()

    if TARGET_COLUMN not in result.columns:
        required = {
            "actual_delivery_time_min",
            "promised_delivery_time_min",
        }

        missing = required - set(result.columns)

        if missing:
            raise ValueError(
                "Cannot create the target because required columns are missing: "
                f"{sorted(missing)}"
            )

        actual = pd.to_numeric(
            result["actual_delivery_time_min"],
            errors="coerce",
        )
        promised = pd.to_numeric(
            result["promised_delivery_time_min"],
            errors="coerce",
        )

        result[TARGET_COLUMN] = (
            actual - promised > 0
        ).astype("Int64")

    result = result.dropna(
        subset=[TARGET_COLUMN]
    )

    result[TARGET_COLUMN] = pd.to_numeric(
        result[TARGET_COLUMN],
        errors="coerce",
    )

    result = result.dropna(
        subset=[TARGET_COLUMN]
    )

    result[TARGET_COLUMN] = (
        result[TARGET_COLUMN]
        .astype(int)
        .clip(0, 1)
    )

    if result[TARGET_COLUMN].nunique() < 2:
        raise ValueError(
            "The target contains only one class. Both delayed and on-time "
            "orders are required for classification."
        )

    return result


def add_safe_calendar_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Create model-safe calendar features from order_date."""
    result = df.copy()

    if "order_date" not in result.columns:
        return result

    date = pd.to_datetime(
        result["order_date"],
        errors="coerce",
    )

    result["order_year"] = date.dt.year
    result["order_month"] = date.dt.month
    result["order_day_of_week"] = date.dt.dayofweek
    result["order_day_of_month"] = date.dt.day
    result["order_week_of_year"] = (
        date.dt.isocalendar().week.astype(float)
    )
    result["order_is_weekend"] = (
        date.dt.dayofweek >= 5
    ).astype(float)

    result.drop(
        columns=["order_date"],
        inplace=True,
    )

    return result


def prepare_features(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    """
    Build the feature matrix while explicitly removing target leakage.

    Returns:
        X, y, identifiers
    """
    result = prepare_target(df)
    result = add_safe_calendar_features(result)

    identifiers = [
        column
        for column in IDENTIFIER_COLUMNS
        if column in result.columns
    ]

    feature_columns = [
        column
        for column in result.columns
        if column not in LEAKAGE_COLUMNS
        and column not in IDENTIFIER_COLUMNS
        and column not in EXCLUDED_RAW_COLUMNS
    ]

    # Remove columns that are not useful in this baseline:
    # raw timestamps, free-form categorical labels, or high-cardinality text.
    for column in list(feature_columns):
        if (
            result[column].dtype == "object"
            and result[column].nunique(dropna=True) > 50
        ):
            feature_columns.remove(column)

    if not feature_columns:
        raise ValueError(
            "No usable predictive features remain after leakage prevention."
        )

    X = result[feature_columns].copy()
    y = result[TARGET_COLUMN].copy()

    # Convert boolean columns into numeric form.
    for column in X.columns:
        if pd.api.types.is_bool_dtype(X[column]):
            X[column] = X[column].astype(float)

    return X, y, identifiers


def build_preprocessor(
    X: pd.DataFrame,
) -> ColumnTransformer:
    """Build numeric/categorical preprocessing."""
    numeric_columns = X.select_dtypes(
        include=["number", "bool"]
    ).columns.tolist()

    categorical_columns = X.select_dtypes(
        include=["object", "category"]
    ).columns.tolist()

    numeric_pipeline = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(strategy="median"),
            ),
            (
                "scaler",
                StandardScaler(),
            ),
        ]
    )

    categorical_pipeline = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(strategy="most_frequent"),
            ),
            (
                "onehot",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=False,
                ),
            ),
        ]
    )

    transformers = []

    if numeric_columns:
        transformers.append(
            (
                "numeric",
                numeric_pipeline,
                numeric_columns,
            )
        )

    if categorical_columns:
        transformers.append(
            (
                "categorical",
                categorical_pipeline,
                categorical_columns,
            )
        )

    return ColumnTransformer(
        transformers=transformers,
        remainder="drop",
    )


def create_model_pipeline(
    X_train: pd.DataFrame,
) -> Pipeline:
    """Create preprocessing + logistic regression pipeline."""
    preprocessor = build_preprocessor(
        X_train
    )

    classifier = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        random_state=42,
    )

    return Pipeline(
        steps=[
            (
                "preprocessor",
                preprocessor,
            ),
            (
                "classifier",
                classifier,
            ),
        ]
    )


def evaluate_model(
    model: Pipeline,
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Evaluate predictions using classification metrics."""
    predictions = model.predict(X_test)

    probabilities = model.predict_proba(
        X_test
    )[:, 1]

    metrics = {
        "accuracy": float(
            accuracy_score(
                y_test,
                predictions,
            )
        ),
        "precision": float(
            precision_score(
                y_test,
                predictions,
                zero_division=0,
            )
        ),
        "recall": float(
            recall_score(
                y_test,
                predictions,
                zero_division=0,
            )
        ),
        "f1_score": float(
            f1_score(
                y_test,
                predictions,
                zero_division=0,
            )
        ),
        "roc_auc": float(
            roc_auc_score(
                y_test,
                probabilities,
            )
        ),
        "test_rows": int(len(y_test)),
    }

    prediction_df = pd.DataFrame(
        {
            "actual_is_delayed": y_test.to_numpy(),
            "predicted_is_delayed": predictions,
            "delay_probability": probabilities.round(4),
        }
    )

    return prediction_df, metrics


def get_feature_coefficients(
    model: Pipeline,
) -> pd.DataFrame:
    """Extract logistic-regression coefficients after preprocessing."""
    preprocessor = model.named_steps["preprocessor"]
    classifier = model.named_steps["classifier"]

    try:
        feature_names = preprocessor.get_feature_names_out()
        coefficients = classifier.coef_[0]

        result = pd.DataFrame(
            {
                "feature": feature_names,
                "coefficient": coefficients,
                "absolute_coefficient": np.abs(coefficients),
            }
        )

        result["direction"] = np.where(
            result["coefficient"] >= 0,
            "Higher delay risk",
            "Lower delay risk",
        )

        return result.sort_values(
            "absolute_coefficient",
            ascending=False,
        )
    except Exception as exc:
        print(
            f"[WARN] Could not extract feature coefficients: {exc}"
        )
        return pd.DataFrame(
            columns=[
                "feature",
                "coefficient",
                "absolute_coefficient",
                "direction",
            ]
        )


def save_confusion_matrix(
    y_test: pd.Series,
    predictions: np.ndarray,
    charts_dir: Path,
) -> None:
    """Save a confusion matrix chart."""
    matrix = confusion_matrix(
        y_test,
        predictions,
    )

    plt.figure(figsize=(6, 5))
    plt.imshow(matrix)
    plt.title("Delivery Delay Prediction - Confusion Matrix")
    plt.xlabel("Predicted Class")
    plt.ylabel("Actual Class")

    plt.xticks(
        [0, 1],
        ["On Time", "Delayed"],
    )
    plt.yticks(
        [0, 1],
        ["On Time", "Delayed"],
    )

    for row in range(matrix.shape[0]):
        for column in range(matrix.shape[1]):
            plt.text(
                column,
                row,
                str(matrix[row, column]),
                ha="center",
                va="center",
            )

    plt.colorbar()
    plt.tight_layout()
    plt.savefig(
        charts_dir / "confusion_matrix.png",
        dpi=150,
    )
    plt.close()


def save_roc_curve(
    y_test: pd.Series,
    probabilities: np.ndarray,
    charts_dir: Path,
) -> None:
    """Save ROC curve chart."""
    false_positive_rate, true_positive_rate, _ = roc_curve(
        y_test,
        probabilities,
    )

    auc = roc_auc_score(
        y_test,
        probabilities,
    )

    plt.figure(figsize=(7, 6))
    plt.plot(
        false_positive_rate,
        true_positive_rate,
        label=f"ROC AUC = {auc:.3f}",
    )
    plt.plot(
        [0, 1],
        [0, 1],
        linestyle="--",
        label="Random Baseline",
    )
    plt.title("Delivery Delay Prediction - ROC Curve")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        charts_dir / "roc_curve.png",
        dpi=150,
    )
    plt.close()


def create_prediction_summary(
    prediction_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create a concise summary for analytics/dashboard use."""
    return pd.DataFrame(
        [
            {
                "prediction_rows": len(prediction_df),
                "predicted_delayed_orders": int(
                    prediction_df["predicted_is_delayed"].sum()
                ),
                "predicted_on_time_orders": int(
                    (
                        prediction_df["predicted_is_delayed"] == 0
                    ).sum()
                ),
                "average_delay_probability_pct": round(
                    prediction_df["delay_probability"].mean()
                    * 100,
                    2,
                ),
                "high_risk_predictions_pct": round(
                    (
                        prediction_df["delay_probability"] >= 0.70
                    ).mean()
                    * 100,
                    2,
                ),
            }
        ]
    )


def run_prediction(
    features_dir: Path,
    processed_dir: Path,
    raw_dir: Path,
    output_dir: Path,
    test_size: float,
    random_state: int,
) -> None:
    if not 0 < test_size < 1:
        raise ValueError(
            "--test-size must be between 0 and 1."
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    charts_dir = output_dir / "charts"
    charts_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("[1/6] Loading delivery features...")
    df = load_features(
        features_dir,
        processed_dir,
        raw_dir,
    )

    print("[2/6] Preparing target and leakage-safe features...")
    X, y, identifiers = prepare_features(df)

    print(f"Feature rows: {len(X):,}")
    print(f"Predictive features: {len(X.columns)}")
    print(f"Target distribution:\n{y.value_counts(normalize=True).sort_index()}")

    stratify_target = y if y.value_counts().min() >= 2 else None

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=stratify_target,
    )

    print("[3/6] Training Logistic Regression model...")
    model = create_model_pipeline(
        X_train
    )

    model.fit(
        X_train,
        y_train,
    )

    print("[4/6] Evaluating model...")
    test_predictions, metrics = evaluate_model(
        model,
        X_test,
        y_test,
    )

    # Restore identifiers in the prediction output when available.
    test_indices = X_test.index

    for identifier in identifiers:
        test_predictions.insert(
            0,
            identifier,
            df.loc[
                test_indices,
                identifier,
            ].to_numpy(),
        )

    test_predictions.to_csv(
        output_dir / "delivery_predictions.csv",
        index=False,
    )

    metrics_df = pd.DataFrame(
        [
            {
                "metric": metric,
                "value": value,
            }
            for metric, value in metrics.items()
        ]
    )

    metrics_df.to_csv(
        output_dir / "model_metrics.csv",
        index=False,
    )

    report = classification_report(
        y_test,
        test_predictions["predicted_is_delayed"],
        target_names=[
            "On Time",
            "Delayed",
        ],
        output_dict=True,
        zero_division=0,
    )

    report_df = (
        pd.DataFrame(report)
        .transpose()
        .reset_index()
        .rename(columns={"index": "class"})
    )

    report_df.to_csv(
        output_dir / "classification_report.csv",
        index=False,
    )

    coefficients = get_feature_coefficients(
        model
    )

    coefficients.to_csv(
        output_dir / "feature_coefficients.csv",
        index=False,
    )

    print("[5/6] Saving prediction charts...")
    save_confusion_matrix(
        y_test,
        test_predictions["predicted_is_delayed"].to_numpy(),
        charts_dir,
    )

    save_roc_curve(
        y_test,
        test_predictions["delay_probability"].to_numpy(),
        charts_dir,
    )

    summary = create_prediction_summary(
        test_predictions
    )

    summary.to_csv(
        output_dir / "prediction_summary.csv",
        index=False,
    )

    print("[6/6] Prediction pipeline completed.")
    print(f"Accuracy:  {metrics['accuracy']:.4f}")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall:    {metrics['recall']:.4f}")
    print(f"F1 Score:  {metrics['f1_score']:.4f}")
    print(f"ROC AUC:   {metrics['roc_auc']:.4f}")
    print(f"Outputs written to: {output_dir.resolve()}")


def main() -> None:
    args = parse_args()

    run_prediction(
        features_dir=Path(args.features_dir),
        processed_dir=Path(args.processed_dir),
        raw_dir=Path(args.raw_dir),
        output_dir=Path(args.output_dir),
        test_size=args.test_size,
        random_state=args.random_state,
    )


if __name__ == "__main__":
    main()

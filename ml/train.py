import argparse
import json
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer


DEFAULT_DB = "data/analytics.duckdb"
DEFAULT_TABLE = "mart_flight_features"


def load_data(db_path: str, table: str) -> pd.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(f"select * from {table}").fetchdf()
    finally:
        con.close()
    return df


def split_by_time(df: pd.DataFrame, time_col: str, test_days: int):
    if time_col not in df.columns:
        raise ValueError(f"Colonne temps manquante: {time_col}")
    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    df = df.dropna(subset=[time_col])
    cutoff = df[time_col].max() - pd.Timedelta(days=test_days)
    train_df = df[df[time_col] < cutoff]
    test_df = df[df[time_col] >= cutoff]
    if train_df.empty or test_df.empty:
        train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
    return train_df, test_df


def build_pipeline(target_type: str, numeric_features, categorical_features):
    cat_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    num_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", cat_pipe, categorical_features),
            ("num", num_pipe, numeric_features),
        ]
    )

    if target_type == "classification":
        model = RandomForestClassifier(
            n_estimators=200, random_state=42, n_jobs=-1, class_weight="balanced"
        )
    else:
        model = RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)

    return Pipeline(steps=[("preprocess", preprocessor), ("model", model)])


def evaluate(y_true, y_pred, target_type: str):
    if target_type == "classification":
        return {
            "accuracy": float(accuracy_score(y_true, y_pred)),
            "f1": float(f1_score(y_true, y_pred, average="weighted")),
        }
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(mean_squared_error(y_true, y_pred, squared=False)),
        "r2": float(r2_score(y_true, y_pred)),
    }


def main():
    parser = argparse.ArgumentParser(description="Train baseline ML model")
    parser.add_argument("--db-path", default=DEFAULT_DB)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--target", required=True)
    parser.add_argument(
        "--target-type",
        choices=["regression", "classification"],
        default="regression",
    )
    parser.add_argument("--time-col", default="event_hour_utc")
    parser.add_argument("--test-days", type=int, default=30)
    parser.add_argument("--output-dir", default="ml/artifacts")
    args = parser.parse_args()

    df = load_data(args.db_path, args.table)
    if args.target not in df.columns:
        raise ValueError(
            f"Colonne cible introuvable: {args.target}. "
            "Ajoute-la dans mart_flight_features."
        )

    df = df.dropna(subset=[args.target])
    train_df, test_df = split_by_time(df, args.time_col, args.test_days)

    categorical_features = [
        "flight_type",
        "airport_icao",
        "est_departure_airport",
        "est_arrival_airport",
        "route_code",
        "airport_country_code",
    ]
    numeric_features = [
        "flight_duration_min",
        "hour_of_day_utc",
        "day_of_week_utc",
        "month_utc",
        "week_of_year_utc",
        "day_of_year_utc",
        "temperature_2m",
        "precipitation",
        "wind_speed_10m",
        "cloud_cover",
    ]

    categorical_used = [c for c in categorical_features if c in df.columns]
    numeric_used = [c for c in numeric_features if c in df.columns]
    feature_cols = categorical_used + numeric_used
    X_train = train_df[feature_cols]
    y_train = train_df[args.target]
    X_test = test_df[feature_cols]
    y_test = test_df[args.target]

    pipeline = build_pipeline(args.target_type, numeric_used, categorical_used)
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)

    metrics = evaluate(y_test, y_pred, args.target_type)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, output_dir / "model.joblib")
    with (output_dir / "features.json").open("w", encoding="utf-8") as f:
        json.dump(
            {
                "target": args.target,
                "target_type": args.target_type,
                "time_col": args.time_col,
                "categorical_features": categorical_used,
                "numeric_features": numeric_used,
            },
            f,
            indent=2,
        )
    with (output_dir / "metrics.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print("Metrics:", metrics)


if __name__ == "__main__":
    main()

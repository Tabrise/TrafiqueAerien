import io
import json
from functools import lru_cache
from pathlib import Path

import duckdb
import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse


DEFAULT_DB = "data/analytics.duckdb"
DEFAULT_TABLE = "mart_flight_features"
DEFAULT_MODEL = "ml/artifacts/model.joblib"
DEFAULT_FEATURES = "ml/artifacts/features.json"

app = FastAPI(title="Open Data Air Traffic API")


@lru_cache(maxsize=1)
def load_model(model_path: str):
    path = Path(model_path)
    if not path.exists():
        raise FileNotFoundError(model_path)
    return joblib.load(path)


def load_feature_config(features_path: str) -> dict:
    path = Path(features_path)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_data(db_path: str, table: str, limit: int) -> pd.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            f"""
            select *
            from {table}
            order by event_hour_utc desc
            limit {limit}
            """
        ).fetchdf()
    finally:
        con.close()
    return df


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/predictions")
def predictions(
    limit: int = Query(100, ge=1, le=10000),
    db_path: str = DEFAULT_DB,
    table: str = DEFAULT_TABLE,
    model_path: str = DEFAULT_MODEL,
    features_path: str = DEFAULT_FEATURES,
):
    try:
        model = load_model(model_path)
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="Model not found. Train it first.")

    df = load_data(db_path, table, limit)
    if df.empty:
        return {"count": 0, "predictions": []}

    feature_cfg = load_feature_config(features_path)
    categorical = feature_cfg.get("categorical_features", [])
    numeric = feature_cfg.get("numeric_features", [])
    feature_cols = [c for c in (categorical + numeric) if c in df.columns]
    if not feature_cols:
        raise HTTPException(status_code=400, detail="No feature columns available.")

    X = df[feature_cols]
    preds = model.predict(X)

    out = df[
        [
            "icao24",
            "callsign",
            "flight_type",
            "airport_icao",
            "event_hour_utc",
            "route_code",
        ]
    ].copy()
    out["prediction"] = preds
    out["event_hour_utc"] = pd.to_datetime(out["event_hour_utc"]).astype(str)

    return {"count": len(out), "predictions": out.to_dict(orient="records")}


@app.post("/predict")
def predict(
    payload: dict,
    model_path: str = DEFAULT_MODEL,
    features_path: str = DEFAULT_FEATURES,
):
    try:
        model = load_model(model_path)
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="Model not found. Train it first.")

    records = payload.get("records")
    if not isinstance(records, list) or not records:
        raise HTTPException(status_code=400, detail="Payload must include non-empty 'records' list.")

    df = pd.DataFrame.from_records(records)
    feature_cfg = load_feature_config(features_path)
    categorical = feature_cfg.get("categorical_features", [])
    numeric = feature_cfg.get("numeric_features", [])
    feature_cols = [c for c in (categorical + numeric) if c in df.columns]
    if not feature_cols:
        raise HTTPException(status_code=400, detail="No feature columns available in payload.")

    preds = model.predict(df[feature_cols])
    return {"count": len(df), "predictions": preds.tolist()}


@app.get("/predictions.csv")
def predictions_csv(
    limit: int = Query(100, ge=1, le=10000),
    db_path: str = DEFAULT_DB,
    table: str = DEFAULT_TABLE,
    model_path: str = DEFAULT_MODEL,
    features_path: str = DEFAULT_FEATURES,
):
    try:
        model = load_model(model_path)
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="Model not found. Train it first.")

    df = load_data(db_path, table, limit)
    if df.empty:
        raise HTTPException(status_code=404, detail="No data available.")

    feature_cfg = load_feature_config(features_path)
    categorical = feature_cfg.get("categorical_features", [])
    numeric = feature_cfg.get("numeric_features", [])
    feature_cols = [c for c in (categorical + numeric) if c in df.columns]
    if not feature_cols:
        raise HTTPException(status_code=400, detail="No feature columns available.")

    preds = model.predict(df[feature_cols])
    out = df.copy()
    out["prediction"] = preds
    if "event_hour_utc" in out.columns:
        out["event_hour_utc"] = pd.to_datetime(out["event_hour_utc"]).astype(str)

    buffer = io.StringIO()
    out.to_csv(buffer, index=False)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=predictions.csv"},
    )


@app.get("/flights")
def flights(
    limit: int = Query(100, ge=1, le=10000),
    airport_icao: str | None = None,
    flight_type: str | None = Query(None, pattern="^(arrival|departure)$"),
    db_path: str = DEFAULT_DB,
    table: str = DEFAULT_TABLE,
):
    where = []
    if airport_icao:
        where.append(f"airport_icao = '{airport_icao.upper()}'")
    if flight_type:
        where.append(f"flight_type = '{flight_type}'")
    where_clause = f"where {' and '.join(where)}" if where else ""

    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            f"""
            select
                icao24,
                callsign,
                flight_type,
                airport_icao,
                est_departure_airport,
                est_arrival_airport,
                event_hour_utc,
                route_code,
                flight_duration_min
            from {table}
            {where_clause}
            order by event_hour_utc desc
            limit {limit}
            """
        ).fetchdf()
    finally:
        con.close()

    df["event_hour_utc"] = pd.to_datetime(df["event_hour_utc"]).astype(str)
    return {"count": len(df), "flights": df.to_dict(orient="records")}


@app.get("/flights/{icao24}")
def flights_by_icao24(
    icao24: str,
    limit: int = Query(100, ge=1, le=10000),
    db_path: str = DEFAULT_DB,
    table: str = DEFAULT_TABLE,
):
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            f"""
            select
                icao24,
                callsign,
                flight_type,
                airport_icao,
                est_departure_airport,
                est_arrival_airport,
                event_hour_utc,
                route_code,
                flight_duration_min
            from {table}
            where icao24 = '{icao24.lower()}'
            order by event_hour_utc desc
            limit {limit}
            """
        ).fetchdf()
    finally:
        con.close()

    if df.empty:
        return {"count": 0, "flights": []}

    df["event_hour_utc"] = pd.to_datetime(df["event_hour_utc"]).astype(str)
    return {"count": len(df), "flights": df.to_dict(orient="records")}


@app.get("/airports")
def airports(
    limit: int = Query(200, ge=1, le=5000),
    db_path: str = DEFAULT_DB,
    table: str = DEFAULT_TABLE,
):
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            f"""
            select
                airport_icao,
                count(*) as flights_count,
                min(event_hour_utc) as first_seen,
                max(event_hour_utc) as last_seen
            from {table}
            group by 1
            order by flights_count desc
            limit {limit}
            """
        ).fetchdf()
    finally:
        con.close()

    df["first_seen"] = pd.to_datetime(df["first_seen"]).astype(str)
    df["last_seen"] = pd.to_datetime(df["last_seen"]).astype(str)
    return {"count": len(df), "airports": df.to_dict(orient="records")}

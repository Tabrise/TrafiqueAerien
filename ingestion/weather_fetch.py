import argparse
import json
import time
from pathlib import Path

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from utils import build_date_args, ensure_dir, load_config, resolve_dates


OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "cloud_cover",
]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _get_json(params: dict):
    response = requests.get(OPEN_METEO_ARCHIVE, params=params, timeout=30)
    if response.status_code >= 500:
        response.raise_for_status()
    if response.status_code == 429:
        raise RuntimeError("Rate limited by Open-Meteo (429)")
    response.raise_for_status()
    return response.json()


def read_airports(airports_file: str) -> pd.DataFrame:
    df = pd.read_csv(airports_file)
    required = {"icao", "latitude", "longitude"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError("Colonnes manquantes dans airports_eu.csv: {}".format(sorted(missing)))
    return df.dropna(subset=["icao", "latitude", "longitude"]).drop_duplicates(subset=["icao"])


def write_json(payload, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)


def main():
    parser = argparse.ArgumentParser(description="Fetch Open-Meteo archive by airport")
    build_date_args(parser)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--sleep", type=float, default=1.0, help="Delay between calls")
    args = parser.parse_args()

    config = load_config(args.config)
    airports_file = config["airports"]["file"]
    raw_dir = Path(config["storage"]["raw_dir"]) / "weather"
    timezone_default = "UTC"

    airports_df = read_airports(airports_file)
    start, end = resolve_dates(args, config)

    for day in pd.date_range(start, end, freq="D"):
        day_date = day.date().strftime("%Y-%m-%d")
        day_dir = ensure_dir(raw_dir / day_date)

        for _, row in airports_df.iterrows():
            airport = row["icao"]
            params = {
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "start_date": day_date,
                "end_date": day_date,
                "hourly": ",".join(HOURLY_VARS),
                "timezone": timezone_default,
            }
            payload = _get_json(params)
            payload["airport_icao"] = airport

            output_path = Path(day_dir) / f"{airport}.json"
            write_json(payload, output_path)
            time.sleep(args.sleep)


if __name__ == "__main__":
    main()

import argparse
import json
import time
from pathlib import Path

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from utils import build_date_args, ensure_dir, load_config, resolve_dates


OPENSKY_BASE = "https://opensky-network.org/api"
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def _get_json(url: str, headers: dict, params: dict):
    response = requests.get(url, headers=headers, params=params, timeout=30)
    if response.status_code == 401:
        raise RuntimeError(
            "OpenSky 401: identifiants invalides ou absents. "
            "Verifiez config.yaml (opensky.oauth ou opensky.username/password) "
            "et votre compte."
        )
    if response.status_code == 404:
        return []
    if response.status_code >= 500:
        response.raise_for_status()
    if response.status_code == 429:
        retry_after = response.headers.get("X-Rate-Limit-Retry-After-Seconds")
        if retry_after:
            time.sleep(int(retry_after))
        raise RuntimeError("Rate limited by OpenSky (429)")
    response.raise_for_status()
    return response.json()


def build_auth_headers(config: dict) -> dict:
    oauth_cfg = config.get("opensky", {}).get("oauth", {})
    client_id = oauth_cfg.get("client_id")
    client_secret = oauth_cfg.get("client_secret")
    if client_id and client_secret:
        token = fetch_oauth_token(client_id, client_secret)
        return {"Authorization": f"Bearer {token}"}

    username = config["opensky"].get("username")
    password = config["opensky"].get("password")
    if not username or not password:
        raise ValueError("Identifiants OpenSky manquants dans config.yaml")
    return {"Authorization": "Basic " + requests.auth._basic_auth_str(username, password)}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_oauth_token(client_id: str, client_secret: str) -> str:
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    response = requests.post(OPENSKY_TOKEN_URL, data=data, timeout=30)
    response.raise_for_status()
    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("Token OAuth2 OpenSky manquant dans la reponse")
    return token


def read_airports(airports_file: str) -> pd.DataFrame:
    df = pd.read_csv(airports_file)
    if "icao" not in df.columns:
        raise ValueError("airports_eu.csv doit contenir la colonne 'icao'")
    return df.dropna(subset=["icao"]).drop_duplicates(subset=["icao"])


def epoch_range_for_date(day):
    begin = int(pd.Timestamp(day, tz="UTC").timestamp())
    end = int(pd.Timestamp(day, tz="UTC").replace(hour=23, minute=59, second=59).timestamp())
    return begin, end


def write_jsonl(records, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for row in records:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


def fetch_for_airport(headers, airport: str, begin: int, end: int, flight_type: str):
    if flight_type == "departure":
        endpoint = f"{OPENSKY_BASE}/flights/departure"
    elif flight_type == "arrival":
        endpoint = f"{OPENSKY_BASE}/flights/arrival"
    else:
        raise ValueError("flight_type invalide: {}".format(flight_type))

    params = {"airport": airport, "begin": begin, "end": end}
    return _get_json(endpoint, headers=headers, params=params)


def main():
    parser = argparse.ArgumentParser(description="Fetch OpenSky flights by airport")
    build_date_args(parser)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--sleep", type=float, default=1.0, help="Delay between calls")
    args = parser.parse_args()

    config = load_config(args.config)
    airports_file = config["airports"]["file"]
    raw_dir = Path(config["storage"]["raw_dir"]) / "opensky"
    headers = build_auth_headers(config)

    airports_df = read_airports(airports_file)
    start, end = resolve_dates(args, config)

    for day in pd.date_range(start, end, freq="D"):
        day_date = day.date()
        begin, end_ts = epoch_range_for_date(day_date)
        day_dir = ensure_dir(raw_dir / day_date.strftime("%Y-%m-%d"))

        for _, row in airports_df.iterrows():
            airport = row["icao"]
            for flight_type in ("departure", "arrival"):
                records = fetch_for_airport(headers, airport, begin, end_ts, flight_type)
                for record in records:
                    record["flight_type"] = flight_type
                    record["airport_icao"] = airport

                output_path = Path(day_dir) / f"{airport}_{flight_type}.jsonl"
                write_jsonl(records, output_path)
                time.sleep(args.sleep)


if __name__ == "__main__":
    main()

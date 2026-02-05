import argparse
from pathlib import Path

import pandas as pd
import requests

from utils import ensure_dir


OURAIRPORTS_URL = "https://ourairports.com/data/airports.csv"


def download_csv(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return pd.read_csv(pd.io.common.StringIO(response.text))


def filter_europe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df[df["continent"] == "EU"]
    df = df[df["icao_code"].notna()]
    df = df[df["latitude_deg"].notna() & df["longitude_deg"].notna()]
    if "timezone" in df.columns:
        df = df[df["timezone"].notna()]
    return df


def to_reference_schema(df: pd.DataFrame) -> pd.DataFrame:
    if "timezone" not in df.columns:
        df = df.assign(timezone=None)
    if "iso_country" not in df.columns:
        df = df.assign(iso_country=None)
    return df.rename(
        columns={
            "icao_code": "icao",
            "latitude_deg": "latitude",
            "longitude_deg": "longitude",
            "iso_country": "country_code",
        }
    )[
        ["icao", "latitude", "longitude", "timezone", "country_code"]
    ].drop_duplicates(subset=["icao"])


def main():
    parser = argparse.ArgumentParser(description="Generate EU airports reference file")
    parser.add_argument(
        "--output",
        default="data/reference/airports_eu.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    df = download_csv(OURAIRPORTS_URL)
    df = filter_europe(df)
    out_df = to_reference_schema(df)

    output_path = Path(args.output)
    ensure_dir(output_path.parent)
    out_df.to_csv(output_path, index=False)
    print("Wrote {}".format(output_path))


if __name__ == "__main__":
    main()

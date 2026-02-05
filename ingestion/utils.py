import argparse
import datetime as dt
import os
from pathlib import Path

import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            "Config introuvable: {}. Copiez config.example.yaml en config.yaml.".format(
                config_path
            )
        )
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def date_range(start: dt.date, end: dt.date):
    current = start
    while current <= end:
        yield current
        current = current + dt.timedelta(days=1)


def ensure_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def build_date_args(parser: argparse.ArgumentParser):
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--date", type=parse_date, help="YYYY-MM-DD")
    group = parser.add_argument_group("date range")
    group.add_argument("--start", type=parse_date, help="YYYY-MM-DD")
    group.add_argument("--end", type=parse_date, help="YYYY-MM-DD")


def resolve_dates(args, config: dict):
    if args.date:
        return args.date, args.date
    if args.start and args.end:
        return args.start, args.end

    run_cfg = config.get("run", {})
    start = parse_date(run_cfg.get("start_date"))
    end = parse_date(run_cfg.get("end_date"))
    return start, end

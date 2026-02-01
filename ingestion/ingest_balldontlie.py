import argparse
import datetime as dt
import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional

import boto3  # type: ignore
import requests
from botocore.exceptions import ClientError  # type: ignore

DEFAULT_PER_PAGE = 25


def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing required env var: {name}")
    return value


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=get_env("MINIO_ENDPOINT"),
        aws_access_key_id=get_env("MINIO_ROOT_USER"),
        aws_secret_access_key=get_env("MINIO_ROOT_PASSWORD"),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def put_json(s3, bucket: str, key: str, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def fetch_paginated(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    api_key: str,
) -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    cursor = None
    retries = 0
    while True:
        page_params = dict(params)
        if cursor is not None:
            page_params["cursor"] = cursor
        page_params["per_page"] = DEFAULT_PER_PAGE
        response = session.get(
            url,
            params=page_params,
            headers={"Authorization": api_key},
            timeout=30,
        )
        if response.status_code == 429:
            if retries >= 5:
                response.raise_for_status()
            wait_seconds = int(response.headers.get("Retry-After", "2"))
            time.sleep(wait_seconds)
            retries += 1
            continue
        response.raise_for_status()
        retries = 0
        payload = response.json()
        data.extend(payload.get("data", []))
        cursor = payload.get("meta", {}).get("next_cursor")
        if cursor is None:
            break
    return data


def fetch_games(
    base_url: str,
    seasons: Iterable[int],
    start_date: Optional[str],
    end_date: Optional[str],
    api_key: str,
) -> List[Dict[str, Any]]:
    session = requests.Session()
    url = f"{base_url}/games"
    all_games: List[Dict[str, Any]] = []
    for season in seasons:
        params: Dict[str, Any] = {"seasons[]": season}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        all_games.extend(fetch_paginated(session, url, params, api_key))
    return all_games


def fetch_teams(base_url: str, api_key: str) -> List[Dict[str, Any]]:
    session = requests.Session()
    url = f"{base_url}/teams"
    return fetch_paginated(session, url, {}, api_key)


def build_partition_path(layer: str, source: str, entity: str, run_date: str) -> str:
    return f"data/{layer}/nba/{source}/{entity}/dt={run_date}/{entity}.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest balldontlie data into MinIO")
    parser.add_argument("--season-start", type=int, required=True)
    parser.add_argument("--season-end", type=int, required=True)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--run-date", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_url = get_env("BALDONTLIE_API_BASE")
    api_key = get_env("BALDONTLIE_API_KEY")
    run_date = args.run_date or dt.date.today().isoformat()
    seasons = list(range(args.season_start, args.season_end + 1))

    games = fetch_games(base_url, seasons, args.start_date, args.end_date, api_key)
    teams = fetch_teams(base_url, api_key)

    if not games:
        raise RuntimeError("No games returned from balldontlie")

    s3 = get_s3_client()
    bucket = get_env("MINIO_BUCKET")
    ensure_bucket(s3, bucket)

    games_key = build_partition_path("raw", "balldontlie", "games", run_date)
    teams_key = build_partition_path("raw", "balldontlie", "teams", run_date)

    put_json(s3, bucket, games_key, games)
    put_json(s3, bucket, teams_key, teams)


if __name__ == "__main__":
    main()

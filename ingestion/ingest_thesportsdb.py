import argparse
import datetime as dt
import json
import os
from typing import Any, Dict, Optional

import boto3  # type: ignore
import requests
from botocore.exceptions import ClientError  # type: ignore


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


def fetch_teams(base_url: str, api_key: str, league: str) -> Dict[str, Any]:
    url = f"{base_url}/{api_key}/search_all_teams.php"
    response = requests.get(url, params={"l": league}, timeout=30)
    response.raise_for_status()
    return response.json()


def build_partition_path(layer: str, source: str, entity: str, run_date: str) -> str:
    return f"data/{layer}/nba/{source}/{entity}/dt={run_date}/{entity}.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest TheSportsDB data into MinIO")
    parser.add_argument("--league", type=str, default="NBA")
    parser.add_argument("--run-date", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_url = get_env("THESPORTSDB_API_BASE")
    api_key = get_env("THESPORTSDB_API_KEY")
    run_date = args.run_date or dt.date.today().isoformat()

    teams_payload = fetch_teams(base_url, api_key, args.league)
    if not teams_payload.get("teams"):
        raise RuntimeError("No teams returned from TheSportsDB")

    s3 = get_s3_client()
    bucket = get_env("MINIO_BUCKET")
    ensure_bucket(s3, bucket)

    teams_key = build_partition_path("raw", "thesportsdb", "teams", run_date)

    put_json(s3, bucket, teams_key, teams_payload)


if __name__ == "__main__":
    main()

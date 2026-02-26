import argparse
import datetime as dt
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional

import boto3  # type: ignore
import requests
from botocore.exceptions import ClientError  # type: ignore

DEFAULT_PER_PAGE = 100  # Max allowed by API
MAX_WORKERS = 5  # Parallel requests
MAX_RETRIES = 10
INITIAL_BACKOFF = 1  # seconds


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


def get_json(s3, bucket: str, key: str) -> Optional[Any]:
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


METADATA_KEY = "data/metadata/ingestion_last_run.json"


def get_last_run_date(s3, bucket: str) -> Optional[str]:
    """Get the date of the last successful ingestion."""
    metadata = get_json(s3, bucket, METADATA_KEY)
    if metadata:
        return metadata.get("last_date")
    return None


def save_last_run_date(s3, bucket: str, last_date: str, game_count: int) -> None:
    """Save the date of the last successful ingestion."""
    metadata = {
        "last_date": last_date,
        "last_game_count": game_count,
        "last_run": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    put_json(s3, bucket, METADATA_KEY, metadata)


def fetch_with_retry(session: requests.Session, url: str, params: Dict[str, Any], api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch a single page with exponential backoff retry."""
    backoff = INITIAL_BACKOFF
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                url,
                params=params,
                headers={"Authorization": api_key},
                timeout=30,
            )
            if response.status_code == 429:
                wait_seconds = int(response.headers.get("Retry-After", str(backoff)))
                time.sleep(wait_seconds)
                backoff *= 2
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
            else:
                raise e
    return None


def fetch_paginated(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    api_key: str,
) -> List[Dict[str, Any]]:
    """Fetch all pages sequentially (original method)."""
    data: List[Dict[str, Any]] = []
    cursor = None
    while True:
        page_params = dict(params)
        if cursor is not None:
            page_params["cursor"] = cursor
        page_params["per_page"] = DEFAULT_PER_PAGE
        
        payload = fetch_with_retry(session, url, page_params, api_key)
        if payload is None:
            break
            
        data.extend(payload.get("data", []))
        cursor = payload.get("meta", {}).get("next_cursor")
        if cursor is None:
            break
    return data


def fetch_paginated_parallel(
    url: str,
    params: Dict[str, Any],
    api_key: str,
    total_items_estimate: int = 3000,
) -> List[Dict[str, Any]]:
    """Fetch all pages in parallel using multiple threads."""
    session = requests.Session()
    all_data: List[Dict[str, Any]] = []
    cursor = None
    
    # First, get initial cursor and estimate total pages
    initial_params = dict(params)
    initial_params["per_page"] = DEFAULT_PER_PAGE
    
    first_payload = fetch_with_retry(session, url, initial_params, api_key)
    if first_payload is None:
        return []
    
    all_data.extend(first_payload.get("data", []))
    cursor = first_payload.get("meta", {}).get("next_cursor")
    
    if cursor is None:
        return all_data
    
    # Calculate remaining pages
    remaining = total_items_estimate - len(all_data)
    pages_remaining = (remaining + DEFAULT_PER_PAGE - 1) // DEFAULT_PER_PAGE
    
    # Fetch remaining pages in parallel
    def fetch_page(cursor_val: str) -> List[Dict[str, Any]]:
        p = dict(params)
        p["cursor"] = cursor_val
        p["per_page"] = DEFAULT_PER_PAGE
        payload = fetch_with_retry(session, url, p, api_key)
        return payload.get("data", []) if payload else []
    
    cursors = []
    for _ in range(pages_remaining):
        if cursor:
            cursors.append(cursor)
            # Get next cursor from last result
            test_params = dict(params)
            test_params["cursor"] = cursor
            test_params["per_page"] = 1
            test_payload = fetch_with_retry(session, url, test_params, api_key)
            if test_payload:
                cursor = test_payload.get("meta", {}).get("next_cursor")
            else:
                break
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_page, c): c for c in cursors}
        for future in as_completed(futures):
            try:
                page_data = future.result()
                all_data.extend(page_data)
            except Exception as e:
                print(f"Error fetching page: {e}")
    
    return all_data


def fetch_games(
    base_url: str,
    seasons: Iterable[int],
    start_date: Optional[str],
    end_date: Optional[str],
    api_key: str,
    parallel: bool = False,
) -> List[Dict[str, Any]]:
    url = f"{base_url}/games"
    all_games: List[Dict[str, Any]] = []
    
    for season in seasons:
        params: Dict[str, Any] = {"seasons[]": season}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        
        if parallel:
            # Use parallel fetching for large datasets
            season_games = fetch_paginated_parallel(url, params, api_key)
        else:
            # Use sequential fetching
            session = requests.Session()
            season_games = fetch_paginated(session, url, params, api_key)
        
        print(f"Season {season}: {len(season_games)} games fetched")
        all_games.extend(season_games)
    
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
    parser.add_argument("--parallel", action="store_true", help="Use parallel fetching for faster ingestion")
    parser.add_argument("--incremental", action="store_true", help="Only fetch games newer than last run (uses metadata)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_url = get_env("BALDONTLIE_API_BASE")
    api_key = get_env("BALDONTLIE_API_KEY")
    run_date = args.run_date or dt.date.today().isoformat()
    seasons = list(range(args.season_start, args.season_end + 1))

    s3 = get_s3_client()
    bucket = get_env("MINIO_BUCKET")
    ensure_bucket(s3, bucket)

    # Incremental mode: determine start_date from last run
    start_date = args.start_date
    if args.incremental:
        last_date = get_last_run_date(s3, bucket)
        if last_date:
            # Start from the day after last run
            last_dt = dt.datetime.strptime(last_date, "%Y-%m-%d")
            start_date = (last_dt + dt.timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"Incremental mode: fetching games from {start_date} (last run: {last_date})")
        else:
            print("Incremental mode: no previous run found, fetching all data")

    print(f"Fetching games for seasons {seasons} (parallel={args.parallel})...")
    games = fetch_games(base_url, seasons, start_date, args.end_date, api_key, parallel=args.parallel)
    print(f"Total games: {len(games)}")
    
    print("Fetching teams...")
    teams = fetch_teams(base_url, api_key)

    if not games and not args.incremental:
        raise RuntimeError("No games returned from balldontlie")

    # Save games and teams only if there are new games or not in incremental mode
    if games or not args.incremental:
        games_key = build_partition_path("raw", "balldontlie", "games", run_date)
        teams_key = build_partition_path("raw", "balldontlie", "teams", run_date)

        if games:
            put_json(s3, bucket, games_key, games)
        if teams:
            put_json(s3, bucket, teams_key, teams)

        # Update last run date
        if games:
            # Find the most recent game date
            dates = [g.get("date") for g in games if g.get("date")]
            if dates:
                # Extract just the date part (YYYY-MM-DD)
                game_dates = [d[:10] for d in dates if d]
                if game_dates:
                    latest_date = max(game_dates)
                    save_last_run_date(s3, bucket, latest_date, len(games))
                    print(f"Saved last run date: {latest_date} ({len(games)} games)")
    else:
        print("No new games to ingest")


if __name__ == "__main__":
    main()

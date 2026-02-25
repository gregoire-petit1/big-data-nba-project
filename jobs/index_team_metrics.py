import argparse
import datetime as dt
import json
import os

import requests
from pyspark.sql import SparkSession  # type: ignore

from spark_utils import SparkConfig, configure_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Index team metrics into Elasticsearch"
    )
    parser.add_argument("--run-date", type=str, default=None)
    return parser.parse_args()


def bulk_index(elastic_url: str, index: str, docs):
    lines = []
    for doc in docs:
        lines.append({"index": {"_index": index}})
        lines.append(doc)
    payload = "\n".join([json.dumps(line, ensure_ascii=True) for line in lines]) + "\n"
    response = requests.post(
        f"{elastic_url}/_bulk",
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=30,
    )
    response.raise_for_status()


def main() -> None:
    args = parse_args()
    run_date = args.run_date or dt.date.today().isoformat()

    spark = SparkSession.builder.appName("index_team_metrics").getOrCreate()
    config = SparkConfig()
    configure_spark(spark, config)

    elastic_host = os.getenv("ELASTIC_HOST", "http://elasticsearch")
    elastic_port = os.getenv("ELASTIC_PORT", "9200")
    elastic_url = f"{elastic_host}:{elastic_port}"

    df = spark.read.parquet(
        config.s3a_path(f"data/combined/nba/team_metrics/dt={run_date}")
    )
    docs = df.toJSON().map(lambda row: json.loads(row)).collect()

    docs = [
        {
            **row,
            "schedule_difficulty_next5": row.get("schedule_difficulty_next5"),
            "home_games_next5": row.get("home_games_next5"),
            "away_games_next5": row.get("away_games_next5"),
        }
        for row in docs
    ]

    if docs:
        bulk_index(elastic_url, "nba_team_metrics", docs)
    spark.stop()


if __name__ == "__main__":
    main()

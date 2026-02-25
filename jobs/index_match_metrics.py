import argparse
import datetime as dt
import json
import os

import requests
from pyspark.sql import SparkSession  # type: ignore

from spark_utils import SparkConfig, configure_spark

BATCH_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Index match metrics into Elasticsearch"
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
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def main() -> None:
    args = parse_args()
    run_date = args.run_date or dt.date.today().isoformat()

    spark = SparkSession.builder.appName("index_match_metrics").getOrCreate()
    config = SparkConfig()
    configure_spark(spark, config)

    elastic_host = os.getenv("ELASTIC_HOST", "http://elasticsearch")
    elastic_port = os.getenv("ELASTIC_PORT", "9200")
    elastic_url = f"{elastic_host}:{elastic_port}"

    df = spark.read.parquet(
        config.s3a_path(f"data/combined/nba/match_metrics_with_preds/dt={run_date}")
    )
    
    print(f"Total rows: {df.count()}")
    
    # Collect in batches
    docs = df.toJSON().map(lambda row: json.loads(row)).collect()
    total = len(docs)
    print(f"Collected {total} documents")
    
    # Index in batches
    for i in range(0, total, BATCH_SIZE):
        batch = docs[i:i+BATCH_SIZE]
        print(f"Indexing batch {i//BATCH_SIZE + 1}: {len(batch)} docs")
        result = bulk_index(elastic_url, "nba_match_metrics", batch)
        print(f"Indexed: {result.get('items', []).__len__()} items")
    
    print("Done indexing!")
    spark.stop()


if __name__ == "__main__":
    main()

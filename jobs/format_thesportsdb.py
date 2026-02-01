import argparse
import datetime as dt

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import col, lower, regexp_replace  # type: ignore

from spark_utils import SparkConfig, configure_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Format TheSportsDB raw data")
    parser.add_argument("--run-date", type=str, default=None)
    return parser.parse_args()


def format_teams(spark: SparkSession, config: SparkConfig, run_date: str) -> None:
    raw_path = config.s3a_path(
        f"data/raw/nba/thesportsdb/teams/dt={run_date}/teams.json"
    )
    df = spark.read.option("multiLine", "true").json(raw_path)
    teams = df.selectExpr("explode(teams) as team").select("team.*")
    formatted = (
        teams.withColumn("thesportsdb_team_name", col("strTeam"))
        .withColumn(
            "team_name_norm", regexp_replace(lower(col("strTeam")), "[^a-z0-9]+", "")
        )
        .withColumn("venue_name", col("strStadium"))
        .withColumn("venue_city", col("strLocation"))
        .withColumn("venue_capacity", col("intStadiumCapacity").cast("int"))
        .select(
            col("idTeam").alias("thesportsdb_team_id"),
            "thesportsdb_team_name",
            "team_name_norm",
            "venue_name",
            "venue_city",
            "venue_capacity",
            col("strWebsite").alias("team_website"),
            col("strBadge").alias("team_badge"),
        )
    )
    formatted.write.mode("overwrite").parquet(
        config.s3a_path(f"data/formatted/nba/thesportsdb/teams/dt={run_date}")
    )


def main() -> None:
    args = parse_args()
    run_date = args.run_date or dt.date.today().isoformat()
    spark = SparkSession.builder.appName("format_thesportsdb").getOrCreate()
    config = SparkConfig()
    configure_spark(spark, config)

    format_teams(spark, config, run_date)

    spark.stop()


if __name__ == "__main__":
    main()

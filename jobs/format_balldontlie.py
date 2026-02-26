import argparse
import datetime as dt

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import col, explode, lower, regexp_replace, to_date  # type: ignore

from spark_utils import SparkConfig, configure_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Format balldontlie raw data")
    parser.add_argument("--run-date", type=str, default=None)
    parser.add_argument("--all-files", action="store_true", help="Read all game files instead of just one run-date")
    return parser.parse_args()


def format_games(spark: SparkSession, config: SparkConfig, run_date: str, all_files: bool = False) -> None:
    if all_files:
        # Read ALL game files from all run-dates
        raw_path = config.s3a_path("data/raw/nba/balldontlie/games/dt=*/games.json")
    else:
        raw_path = config.s3a_path(
            f"data/raw/nba/balldontlie/games/dt={run_date}/games.json"
        )
    
    df = spark.read.option("multiLine", "true").json(raw_path)
    
    # Handle both wrapped and unwrapped formats
    if "data" in df.columns:
        df = df.select(explode(col("data")).alias("game"))
        df = df.select("game.*")
    
    # Deduplicate by game_id to handle overlapping data from multiple runs
    df = df.dropDuplicates(["id"])
    
    formatted = (
        df.withColumn("game_id", col("id"))
        .withColumn("game_date", to_date(col("date")))
        .withColumn("season", col("season").cast("int"))
        .withColumn("home_team_id", col("home_team.id"))
        .withColumn("visitor_team_id", col("visitor_team.id"))
        .withColumn("home_team_score", col("home_team_score").cast("int"))
        .withColumn("visitor_team_score", col("visitor_team_score").cast("int"))
        .withColumn("status", lower(col("status")))
        .select(
            "game_id",
            "game_date",
            "season",
            "home_team_id",
            "visitor_team_id",
            "home_team_score",
            "visitor_team_score",
            "status",
        )
    )
    
    # Use today's date as partition if reading all files, otherwise use run_date
    output_date = dt.date.today().isoformat() if all_files else run_date
    formatted.write.mode("overwrite").parquet(
        config.s3a_path(f"data/formatted/nba/balldontlie/games/dt={output_date}")
    )


def format_teams(spark: SparkSession, config: SparkConfig, run_date: str) -> None:
    raw_path = config.s3a_path(
        f"data/raw/nba/balldontlie/teams/dt={run_date}/teams.json"
    )
    df = spark.read.option("multiLine", "true").json(raw_path)
    
    # Handle both wrapped and unwrapped formats
    if "data" in df.columns:
        df = df.select(explode(col("data")).alias("team"))
        df = df.select("team.*")
    
    formatted = (
        df.withColumn("team_id", col("id"))
        .withColumn("team_name", col("full_name"))
        .withColumn(
            "team_name_norm", regexp_replace(lower(col("full_name")), "[^a-z0-9]+", "")
        )
        .select(
            "team_id",
            "team_name",
            "team_name_norm",
            "city",
            "abbreviation",
            "conference",
            "division",
        )
    )
    formatted.write.mode("overwrite").parquet(
        config.s3a_path(f"data/formatted/nba/balldontlie/teams/dt={run_date}")
    )


def main() -> None:
    args = parse_args()
    run_date = args.run_date or dt.date.today().isoformat()
    spark = SparkSession.builder.appName("format_balldontlie").getOrCreate()
    config = SparkConfig()
    configure_spark(spark, config)

    format_games(spark, config, run_date, all_files=args.all_files)
    format_teams(spark, config, run_date)

    spark.stop()


if __name__ == "__main__":
    main()

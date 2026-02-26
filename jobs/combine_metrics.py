import argparse
import datetime as dt

from pyspark.sql import SparkSession, Window  # type: ignore
from pyspark.sql.functions import (
    col,
    datediff,
    lag,
    lit,
    row_number,
    sum as spark_sum,
    when,
)  # type: ignore

from spark_utils import SparkConfig, configure_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine NBA datasets and compute KPIs"
    )
    parser.add_argument("--run-date", type=str, default=None)
    parser.add_argument("--all-files", action="store_true", help="Read all game files instead of just one run-date")
    return parser.parse_args()


def compute_team_kpis(games_df):
    home_games = games_df.select(
        col("game_id"),
        col("game_date"),
        col("season"),
        col("home_team_id").alias("team_id"),
        col("home_team_score").alias("points_for"),
        col("visitor_team_score").alias("points_against"),
        lit(1).alias("is_home"),
        when(col("home_team_score") > col("visitor_team_score"), 1)
        .otherwise(0)
        .alias("is_win"),
    )

    away_games = games_df.select(
        col("game_id"),
        col("game_date"),
        col("season"),
        col("visitor_team_id").alias("team_id"),
        col("visitor_team_score").alias("points_for"),
        col("home_team_score").alias("points_against"),
        lit(0).alias("is_home"),
        when(col("visitor_team_score") > col("home_team_score"), 1)
        .otherwise(0)
        .alias("is_win"),
    )

    all_games = home_games.unionByName(away_games)
    window = Window.partitionBy("team_id").orderBy(col("game_date").asc())

    enriched = (
        all_games.withColumn("game_number", row_number().over(window))
        .withColumn(
            "points_for_last5", spark_sum("points_for").over(window.rowsBetween(-4, 0))
        )
        .withColumn(
            "points_against_last5",
            spark_sum("points_against").over(window.rowsBetween(-4, 0)),
        )
        .withColumn("wins_last5", spark_sum("is_win").over(window.rowsBetween(-4, 0)))
    )

    return enriched


def compute_rest_days(team_games_df):
    window = Window.partitionBy("team_id").orderBy(col("game_date").asc())
    with_prev = team_games_df.withColumn("prev_date", lag("game_date").over(window))
    rest_days = with_prev.withColumn(
        "rest_days",
        when(col("prev_date").isNull(), None).otherwise(
            datediff(col("game_date"), col("prev_date"))
        ),
    )
    return rest_days


def compute_schedule_difficulty(team_games_df, horizon=5):
    """
    Compute strength of schedule for next N games.
    
    difficulty = opponent_win_rate 
      × (1 + (is_away? 0.20 : 0))      
      × (1 + (is_b2b? 0.20 : 0))
    
    Simplified version: count home/away in next 5 games
    """
    from pyspark.sql.functions import lead
    
    future_window = Window.partitionBy("team_id").orderBy(col("game_date").asc())
    
    # Create columns for next game's home/away status
    sos_df = team_games_df
    for i in range(1, horizon + 1):
        sos_df = sos_df.withColumn(f"next_is_home_{i}", lead("is_home", i).over(future_window))
    
    # Count home and away games in next horizon games
    home_cols = [col(f"next_is_home_{i}") == 1 for i in range(1, horizon + 1)]
    away_cols = [col(f"next_is_home_{i}") == 0 for i in range(1, horizon + 1)]
    
    from pyspark.sql.functions import sum as spark_sum
    
    sos_df = sos_df.withColumn(
        "home_games_next5",
        sum([when(c, 1).otherwise(0) for c in home_cols])
    ).withColumn(
        "away_games_next5",
        sum([when(c, 1).otherwise(0) for c in away_cols])
    )
    
    # Simplified difficulty: (away games * 0.20 + baseline) / horizon
    # This gives higher difficulty for more away games
    sos_df = sos_df.withColumn(
        "schedule_difficulty_next5",
        ((col("away_games_next5") * 0.20 + horizon) / horizon).cast("float")
    )
    
    return sos_df


def main() -> None:
    args = parse_args()
    run_date = args.run_date or dt.date.today().isoformat()

    spark = SparkSession.builder.appName("combine_metrics").getOrCreate()
    config = SparkConfig()
    configure_spark(spark, config)

    # Read all formatted games if --all-files flag is set
    if args.all_files:
        games_path = config.s3a_path("data/formatted/nba/balldontlie/games/dt=*")
        teams_path = config.s3a_path("data/formatted/nba/balldontlie/teams/dt=*")
        thesportsdb_path = config.s3a_path("data/formatted/nba/thesportsdb/teams/dt=*")
    else:
        games_path = config.s3a_path(f"data/formatted/nba/balldontlie/games/dt={run_date}")
        teams_path = config.s3a_path(f"data/formatted/nba/balldontlie/teams/dt={run_date}")
        thesportsdb_path = config.s3a_path(
            f"data/formatted/nba/thesportsdb/teams/dt={run_date}"
        )

    games_df = spark.read.parquet(games_path)
    teams_df = spark.read.parquet(teams_path)
    thesportsdb_df = spark.read.parquet(thesportsdb_path)

    team_kpis = compute_team_kpis(games_df)
    team_kpis = compute_rest_days(team_kpis)
    team_kpis = compute_schedule_difficulty(team_kpis, horizon=5)

    team_dim = teams_df.join(thesportsdb_df, on="team_name_norm", how="left")

    team_metrics = (
        team_kpis.join(team_dim, on="team_id", how="left")
        .withColumn(
            "win_rate_last5",
            when(col("game_number") >= 5, col("wins_last5") / lit(5)).otherwise(None),
        )
        .withColumn(
            "avg_points_last5",
            when(col("game_number") >= 5, col("points_for_last5") / lit(5)).otherwise(
                None
            ),
        )
        .withColumn(
            "home_away_diff",
            when(
                col("is_home") == 1, col("points_for") - col("points_against")
            ).otherwise(None),
        )
        .select(
            "game_id",
            "game_date",
            "season",
            "team_id",
            "team_name",
            "conference",
            "division",
            "is_home",
            "points_for",
            "points_against",
            "wins_last5",
            "win_rate_last5",
            "avg_points_last5",
            "home_away_diff",
            "rest_days",
            "schedule_difficulty_next5",
            "home_games_next5",
            "away_games_next5",
            "thesportsdb_team_id",
            "thesportsdb_team_name",
            "team_website",
            "team_badge",
            "venue_name",
            "venue_city",
            "venue_capacity",
        )
    )

    match_metrics = games_df.select(
        col("game_id"),
        col("game_date"),
        col("season"),
        col("home_team_id"),
        col("visitor_team_id"),
        col("home_team_score"),
        col("visitor_team_score"),
        when(col("home_team_score") > col("visitor_team_score"), 1)
        .otherwise(0)
        .alias("home_win"),
    )

    team_metrics.write.mode("overwrite").parquet(
        config.s3a_path(f"data/combined/nba/team_metrics/dt={run_date}")
    )
    match_metrics.write.mode("overwrite").parquet(
        config.s3a_path(f"data/combined/nba/match_metrics/dt={run_date}")
    )

    spark.stop()


if __name__ == "__main__":
    main()

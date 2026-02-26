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
    """
    Compute KPIs per team per game.
    Returns: team_id, game_id, win_rate, avg_points, etc. for each game.
    """
    # Split into home and away games
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
    
    # Window for cumulative stats per team (sorted by date)
    window = Window.partitionBy("team_id").orderBy(col("game_date").asc())

    # Cumulative stats up to (but not including) current game
    # This gives us the "form" entering each game
    enriched = (
        all_games
        .withColumn("game_number", row_number().over(window))
        # Cumulative stats BEFORE this game (not including current)
        .withColumn(
            "points_for_cum",
            spark_sum("points_for").over(window.rowsBetween(Window.unboundedPreceding, -1))
        )
        .withColumn(
            "points_against_cum",
            spark_sum("points_against").over(window.rowsBetween(Window.unboundedPreceding, -1))
        )
        .withColumn(
            "wins_cum",
            spark_sum("is_win").over(window.rowsBetween(Window.unboundedPreceding, -1))
        )
        .withColumn(
            "games_played",
            row_number().over(window) - 1  # Exclude current game
        )
    )

    # Calculate rates
    enriched = enriched.withColumn(
        "win_rate",
        when(col("games_played") > 0, col("wins_cum") / col("games_played")).otherwise(None)
    )
    enriched = enriched.withColumn(
        "avg_points_for",
        when(col("games_played") > 0, col("points_for_cum") / col("games_played")).otherwise(None)
    )
    enriched = enriched.withColumn(
        "avg_points_against",
        when(col("games_played") > 0, col("points_against_cum") / col("games_played")).otherwise(None)
    )

    # Last 5 games stats
    enriched = enriched.withColumn(
        "points_for_last5",
        spark_sum("points_for").over(window.rowsBetween(-5, -1))
    )
    enriched = enriched.withColumn(
        "points_against_last5",
        spark_sum("points_against").over(window.rowsBetween(-5, -1))
    )
    enriched = enriched.withColumn(
        "wins_last5",
        spark_sum("is_win").over(window.rowsBetween(-5, -1))
    )
    enriched = enriched.withColumn(
        "win_rate_last5",
        when(col("points_for_last5").isNotNull(), col("wins_last5") / 5).otherwise(None)
    )
    enriched = enriched.withColumn(
        "avg_points_last5",
        when(col("points_for_last5").isNotNull(), col("points_for_last5") / 5).otherwise(None)
    )

    return enriched


def compute_rest_days(team_games_df):
    """Compute rest days between games for each team."""
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
    Compute schedule difficulty: count of away games in next N games.
    Simplified version: just use first 5 future games.
    """
    from pyspark.sql.functions import lead
    
    future_window = Window.partitionBy("team_id").orderBy(col("game_date").asc())
    
    sos_df = team_games_df
    
    # Only compute for next 5 games
    for i in range(1, horizon + 1):
        sos_df = sos_df.withColumn(f"next_is_home_{i}", lead("is_home", i).over(future_window))
    
    # Simple count: for now just set to a placeholder
    sos_df = sos_df.withColumn("home_games_next5", lit(horizon / 2))
    sos_df = sos_df.withColumn("away_games_next5", lit(horizon / 2))
    sos_df = sos_df.withColumn(
        "schedule_difficulty_next5",
        lit(1.0).cast("float")
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

    # Compute KPIs per team per game
    team_kpis = compute_team_kpis(games_df)
    team_kpis = compute_rest_days(team_kpis)
    team_kpis = compute_schedule_difficulty(team_kpis, horizon=5)

    # Join with team dimensions
    team_dim = teams_df.join(thesportsdb_df, on="team_name_norm", how="left")
    
    team_kpis = team_kpis.join(team_dim, on="team_id", how="left")

    # Create home and away views
    home_kpis = team_kpis.filter(col("is_home") == 1).alias("home")
    away_kpis = team_kpis.filter(col("is_home") == 0).alias("away")

    # Join to create game-level metrics
    match_metrics = (
        games_df.alias("g")
        .join(
            home_kpis,
            (col("g.home_team_id") == col("home.team_id")) & (col("g.game_id") == col("home.game_id")),
            "left"
        )
        .join(
            away_kpis,
            (col("g.visitor_team_id") == col("away.team_id")) & (col("g.game_id") == col("away.game_id")),
            "left"
        )
        .select(
            col("g.game_id"),
            col("g.game_date"),
            col("g.season"),
            col("g.home_team_id"),
            col("g.visitor_team_id"),
            col("g.home_team_score"),
            col("g.visitor_team_score"),
            when(col("g.home_team_score") > col("g.visitor_team_score"), 1)
            .otherwise(0)
            .alias("home_win"),
            # Home team stats
            col("home.team_name").alias("home_team_name"),
            col("home.win_rate").alias("home_win_rate"),
            col("home.avg_points_for").alias("home_avg_points"),
            col("home.avg_points_against").alias("home_avg_points_against"),
            col("home.win_rate_last5").alias("home_win_rate_last5"),
            col("home.avg_points_last5").alias("home_avg_points_last5"),
            col("home.rest_days").alias("home_rest_days"),
            col("home.schedule_difficulty_next5").alias("home_schedule_difficulty"),
            col("home.home_games_next5").alias("home_home_games_next5"),
            col("home.away_games_next5").alias("home_away_games_next5"),
            # Away team stats
            col("away.team_name").alias("away_team_name"),
            col("away.win_rate").alias("away_win_rate"),
            col("away.avg_points_for").alias("away_avg_points"),
            col("away.avg_points_against").alias("away_avg_points_against"),
            col("away.win_rate_last5").alias("away_win_rate_last5"),
            col("away.avg_points_last5").alias("away_avg_points_last5"),
            col("away.rest_days").alias("away_rest_days"),
            col("away.schedule_difficulty_next5").alias("away_schedule_difficulty"),
            col("away.home_games_next5").alias("away_home_games_next5"),
            col("away.away_games_next5").alias("away_away_games_next5"),
        )
    )

    # Team metrics - keep for team-level analysis (optional)
    team_metrics = team_kpis.select(
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
        "is_win",
        "win_rate",
        "avg_points_for",
        "avg_points_against",
        "win_rate_last5",
        "avg_points_last5",
        "rest_days",
        "schedule_difficulty_next5",
        "home_games_next5",
        "away_games_next5",
    ).dropDuplicates(["game_id", "team_id"])

    # Match metrics - one row per game with both teams
    match_metrics = match_metrics.dropDuplicates(["game_id"])

    team_metrics.write.mode("overwrite").parquet(
        config.s3a_path(f"data/combined/nba/team_metrics/dt={run_date}")
    )
    match_metrics.write.mode("overwrite").parquet(
        config.s3a_path(f"data/combined/nba/match_metrics/dt={run_date}")
    )

    spark.stop()


if __name__ == "__main__":
    main()

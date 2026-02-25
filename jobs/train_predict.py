import argparse
import datetime as dt

from pyspark.ml.classification import LogisticRegression  # type: ignore
from pyspark.ml.feature import VectorAssembler  # type: ignore
from pyspark.ml.functions import vector_to_array  # type: ignore
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import col  # type: ignore

from spark_utils import SparkConfig, configure_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and predict NBA match outcomes")
    parser.add_argument("--run-date", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = args.run_date or dt.date.today().isoformat()

    spark = SparkSession.builder.appName("train_predict").getOrCreate()
    config = SparkConfig()
    configure_spark(spark, config)

    match_path = config.s3a_path(f"data/combined/nba/match_metrics/dt={run_date}")
    team_path = config.s3a_path(f"data/combined/nba/team_metrics/dt={run_date}")

    match_df = spark.read.parquet(match_path)
    team_df = spark.read.parquet(team_path)

    home_team_df = team_df.filter(col("is_home") == 1).select(
        "game_id",
        col("avg_points_last5").alias("home_avg_points_last5"),
        col("win_rate_last5").alias("home_win_rate_last5"),
        col("rest_days").alias("home_rest_days"),
    )

    away_team_df = team_df.filter(col("is_home") == 0).select(
        "game_id",
        col("avg_points_last5").alias("away_avg_points_last5"),
        col("win_rate_last5").alias("away_win_rate_last5"),
        col("rest_days").alias("away_rest_days"),
    )

    features_df = (
        match_df.join(home_team_df, "game_id", "left")
        .join(away_team_df, "game_id", "left")
        .withColumn("home_label", col("home_win"))
        .withColumn(
            "form_diff",
            col("home_win_rate_last5") - col("away_win_rate_last5"),
        )
        .withColumn(
            "points_diff",
            col("home_avg_points_last5") - col("away_avg_points_last5"),
        )
        .withColumn(
            "rest_diff",
            col("home_rest_days") - col("away_rest_days"),
        )
        .na.fill(
            {
                "home_avg_points_last5": 0.0,
                "home_win_rate_last5": 0.0,
                "home_rest_days": 0.0,
                "away_avg_points_last5": 0.0,
                "away_win_rate_last5": 0.0,
                "away_rest_days": 0.0,
                "form_diff": 0.0,
                "points_diff": 0.0,
                "rest_diff": 0.0,
            }
        )
    )

    assembler = VectorAssembler(
        inputCols=[
            "home_avg_points_last5",
            "home_win_rate_last5",
            "home_rest_days",
            "away_avg_points_last5",
            "away_win_rate_last5",
            "away_rest_days",
            "form_diff",
            "points_diff",
            "rest_diff",
        ],
        outputCol="features",
    )
    assembled = assembler.transform(features_df)

    lr = LogisticRegression(featuresCol="features", labelCol="home_label", maxIter=20)
    model = lr.fit(assembled)
    preds = model.transform(assembled)
    preds = preds.withColumn("prob_array", vector_to_array(col("probability")))

    output = preds.select(
        "game_id",
        "home_team_id",
        "visitor_team_id",
        "home_team_score",
        "visitor_team_score",
        "home_win",
        col("prob_array")[1].alias("win_probability_home"),
    )

    output.write.mode("overwrite").parquet(
        config.s3a_path(f"data/combined/nba/match_metrics_with_preds/dt={run_date}")
    )

    spark.stop()


if __name__ == "__main__":
    main()

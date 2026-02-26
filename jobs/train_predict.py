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

    match_df = spark.read.parquet(match_path)

    features_df = (
        match_df
        .withColumn("home_label", col("home_win"))
    )

    # Handle nulls in features
    features_cols = [
        "home_win_rate_last5", "home_avg_points_last5", "home_rest_days",
        "away_win_rate_last5", "away_avg_points_last5", "away_rest_days"
    ]
    
    for c in features_cols:
        features_df = features_df.withColumn(c, col(c).cast("double"))
    
    features_df = features_df.na.fill({
        "home_win_rate_last5": 0.5,
        "home_avg_points_last5": 100.0,
        "home_rest_days": 2.0,
        "away_win_rate_last5": 0.5,
        "away_avg_points_last5": 100.0,
        "away_rest_days": 2.0,
    })

    assembler = VectorAssembler(
        inputCols=[
            "home_win_rate_last5",
            "home_avg_points_last5",
            "home_rest_days",
            "away_win_rate_last5",
            "away_avg_points_last5",
            "away_rest_days",
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
        "game_date",
        "home_team_id",
        "visitor_team_id",
        "home_team_score",
        "visitor_team_score",
        "home_win",
        "home_team_name",
        "away_team_name",
        col("prob_array")[1].alias("win_probability_home"),
    )

    output.write.mode("overwrite").parquet(
        config.s3a_path(f"data/combined/nba/match_metrics_with_preds/dt={run_date}")
    )

    spark.stop()


if __name__ == "__main__":
    main()

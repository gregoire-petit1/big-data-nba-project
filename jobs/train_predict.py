import argparse
import datetime as dt

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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
        .withColumn("win_rate_diff", 
            when(col("home_win_rate").isNotNull() & col("away_win_rate").isNotNull(),
                 col("home_win_rate") - col("away_win_rate")).otherwise(0.0))
        .withColumn("avg_points_diff",
            when(col("home_avg_points").isNotNull() & col("away_avg_points").isNotNull(),
                 col("home_avg_points") - col("away_avg_points")).otherwise(0.0))
        .withColumn("avg_points_against_diff",
            when(col("home_avg_points_against").isNotNull() & col("away_avg_points_against").isNotNull(),
                 col("away_avg_points_against") - col("home_avg_points_against")).otherwise(0.0))
        .withColumn("rest_days_diff", col("home_rest_days") - col("away_rest_days"))
        .withColumn("form_last5_diff",
            when(col("home_win_rate_last5").isNotNull() & col("away_win_rate_last5").isNotNull(),
                 col("home_win_rate_last5") - col("away_win_rate_last5")).otherwise(0.0))
        .withColumn("home_advantage", col("home_rest_days") * 0.02)
    )

    features_cols = [
        "home_win_rate", "home_avg_points", "home_avg_points_against",
        "home_win_rate_last5", "home_avg_points_last5", "home_rest_days",
        "away_win_rate", "away_avg_points", "away_avg_points_against",
        "away_win_rate_last5", "away_avg_points_last5", "away_rest_days",
        "win_rate_diff", "avg_points_diff", "avg_points_against_diff",
        "rest_days_diff", "form_last5_diff", "home_advantage"
    ]
    
    for c in features_cols:
        if c in features_df.columns:
            features_df = features_df.withColumn(c, col(c).cast("double"))
    
    fill_values = {c: 0.5 if "win_rate" in c else 100.0 if "points" in c else 2.0 for c in features_cols}
    features_df = features_df.na.fill(fill_values)

    assembler = VectorAssembler(
        inputCols=[
            "home_win_rate", "home_avg_points", "home_avg_points_against",
            "home_win_rate_last5", "home_avg_points_last5", "home_rest_days",
            "away_win_rate", "away_avg_points", "away_avg_points_against",
            "away_win_rate_last5", "away_avg_points_last5", "away_rest_days",
            "win_rate_diff", "avg_points_diff", "avg_points_against_diff",
            "rest_days_diff", "form_last5_diff", "home_advantage"
        ],
        outputCol="features",
    )
    assembled = assembler.transform(features_df)

    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="home_label", 
        numTrees=50,
        maxDepth=5,
        seed=42
    )
    model = rf.fit(assembled)
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
        "home_win_rate", "home_avg_points", "home_avg_points_against",
        "home_win_rate_last5", "home_avg_points_last5", "home_rest_days",
        "away_win_rate", "away_avg_points", "away_avg_points_against",
        "away_win_rate_last5", "away_avg_points_last5", "away_rest_days",
        "win_rate_diff", "avg_points_diff", "avg_points_against_diff",
        "rest_days_diff", "form_last5_diff", "home_advantage"
    )

    output.write.mode("overwrite").parquet(
        config.s3a_path(f"data/combined/nba/match_metrics_with_preds/dt={run_date}")
    )

    spark.stop()


if __name__ == "__main__":
    main()

import argparse
import datetime as dt

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

from spark_utils import SparkConfig, configure_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and predict NBA match outcomes")
    parser.add_argument("--run-date", type=str, default=None)
    parser.add_argument("--all-files", action="store_true", help="Read all match data files")
    return parser.parse_args()


def add_features(df):
    """Add engineered features to the dataframe."""
    return (
        df
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


def prepare_features(df, feature_cols):
    """Prepare features: cast to double, fill nulls, assemble vector."""
    for c in feature_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("double"))
    
    fill_values = {c: 0.5 if "win_rate" in c else 100.0 if "points" in c else 2.0 for c in feature_cols}
    df = df.na.fill(fill_values)
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    return assembler.transform(df)


def main() -> None:
    args = parse_args()
    run_date = args.run_date or dt.date.today().isoformat()

    spark = SparkSession.builder.appName("train_predict").getOrCreate()
    config = SparkConfig()
    configure_spark(spark, config)

    # Read match data - either single date or all dates
    if args.all_files:
        match_path = config.s3a_path("data/combined/nba/match_metrics/dt=*")
    else:
        match_path = config.s3a_path(f"data/combined/nba/match_metrics/dt={run_date}")

    match_df = spark.read.parquet(match_path)
    print(f"Loaded {match_df.count()} matches")

    # Add features
    feature_cols = [
        "home_win_rate", "home_avg_points", "home_avg_points_against",
        "home_win_rate_last5", "home_avg_points_last5", "home_rest_days",
        "away_win_rate", "away_avg_points", "away_avg_points_against",
        "away_win_rate_last5", "away_avg_points_last5", "away_rest_days",
        "win_rate_diff", "avg_points_diff", "avg_points_against_diff",
        "rest_days_diff", "form_last5_diff", "home_advantage"
    ]
    
    features_df = add_features(match_df)
    assembled = prepare_features(features_df, feature_cols)

    # Split by time: regular season (train) vs rest of season (test)
    # NBA 2024-25: Oct-Jan = regular season, Feb-Jun = playoffs/remaining
    # Using Feb 1, 2025 as cutoff
    train_cutoff = "2025-02-01"
    
    train_df = assembled.filter(col("game_date") < train_cutoff).filter(col("home_label").isNotNull())
    test_df = assembled.filter(col("game_date") >= train_cutoff).filter(col("home_label").isNotNull())
    
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"Training set (Oct 2024 - Jan 2025): {train_count} matches")
    print(f"Test set (Feb 2025 - Jun 2025): {test_count} matches")

    if train_count == 0:
        print("ERROR: No training data found!")
        spark.stop()
        return
    
    if test_count == 0:
        print("WARNING: No test data found, using train for evaluation")
        test_df = train_df

    # Train RandomForest model
    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="home_label", 
        numTrees=50,
        maxDepth=5,
        seed=42
    )
    
    print("Training model...")
    model = rf.fit(train_df)
    
    # Print feature importance
    print("\nFeature Importance:")
    importance = model.featureImportances.toArray()
    for i, col_name in enumerate(feature_cols):
        print(f"  {col_name}: {importance[i]:.4f}")

    # Evaluate on training set
    train_preds = model.transform(train_df)
    train_preds = train_preds.withColumn("prob_array", vector_to_array(col("probability")))
    
    evaluator_auc = BinaryClassificationEvaluator(labelCol="home_label", rawPredictionCol="rawPrediction")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="home_label", predictionCol="prediction")
    
    train_auc = evaluator_auc.evaluate(train_preds)
    train_acc = evaluator_acc.evaluate(train_preds, {evaluator_acc.metricName: "accuracy"})
    print(f"\nTraining Metrics (Oct 2024 - Jan 2025):")
    print(f"  AUC-ROC: {train_auc:.4f}")
    print(f"  Accuracy: {train_acc:.4f}")

    # Predict on test set (this is what we want to visualize)
    test_preds = model.transform(test_df)
    test_preds = test_preds.withColumn("prob_array", vector_to_array(col("probability")))
    
    test_auc = evaluator_auc.evaluate(test_preds)
    test_acc = evaluator_acc.evaluate(test_preds, {evaluator_acc.metricName: "accuracy"})
    print(f"\nTest Metrics (Feb 2025 - Jun 2025):")
    print(f"  AUC-ROC: {test_auc:.4f}")
    print(f"  Accuracy: {test_acc:.4f}")

    # Select output columns (only test predictions for ES)
    output = test_preds.select(
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
        "prediction",
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

    print("\nModel saved successfully!")
    spark.stop()


if __name__ == "__main__":
    main()

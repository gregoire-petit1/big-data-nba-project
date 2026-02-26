import datetime as dt
import os

from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore


RUN_DATE = "{{ ds }}"
SEASON_START = 2024
SEASON_END = 2024

# Set to True after first successful full run
# When True, only new games since last run will be ingested
INCREMENTAL_MODE = os.getenv("INCREMENTAL_MODE", "false").lower() == "true"

# Full run flag - set to False after initial full ingestion
# When False, uses --incremental to only fetch new games
FULL_RUN = os.getenv("FULL_RUN", "true").lower() == "true"


def spark_submit(cmd: str) -> str:
    return (
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
        "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        "--conf spark.hadoop.fs.s3a.path.style.access=true "
        "--py-files /opt/airflow/jobs/spark_utils.py " + cmd
    )


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


with DAG(
    dag_id="nba_pipeline",
    default_args=default_args,
    start_date=dt.datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:
    # Build ingestion command based on mode
    ingest_extra_args = "--parallel"
    if INCREMENTAL_MODE and not FULL_RUN:
        ingest_extra_args += " --incremental"

    ingest_balldontlie = BashOperator(
        task_id="ingest_balldontlie",
        bash_command=(
            "python /opt/airflow/ingestion/ingest_balldontlie.py "
            f"--season-start {SEASON_START} --season-end {SEASON_END} "
            f"--run-date {RUN_DATE} " + ingest_extra_args
        ),
    )

    ingest_thesportsdb = BashOperator(
        task_id="ingest_thesportsdb",
        bash_command=(
            f"python /opt/airflow/ingestion/ingest_thesportsdb.py --run-date {RUN_DATE}"
        ),
    )

    format_balldontlie = BashOperator(
        task_id="format_balldontlie",
        bash_command=spark_submit(
            f"/opt/airflow/jobs/format_balldontlie.py --run-date {RUN_DATE} --all-files"
        ),
    )

    format_thesportsdb = BashOperator(
        task_id="format_thesportsdb",
        bash_command=spark_submit(
            f"/opt/airflow/jobs/format_thesportsdb.py --run-date {RUN_DATE}"
        ),
    )

    combine_metrics = BashOperator(
        task_id="combine_metrics",
        bash_command=spark_submit(
            f"/opt/airflow/jobs/combine_metrics.py --run-date {RUN_DATE} --all-files"
        ),
    )

    train_predict = BashOperator(
        task_id="train_predict",
        bash_command=spark_submit(
            f"/opt/airflow/jobs/train_predict.py --run-date {RUN_DATE}"
        ),
    )

    index_team_metrics = BashOperator(
        task_id="index_team_metrics",
        bash_command=spark_submit(
            f"/opt/airflow/jobs/index_team_metrics.py --run-date {RUN_DATE}"
        ),
    )

    index_match_metrics = BashOperator(
        task_id="index_match_metrics",
        bash_command=spark_submit(
            f"/opt/airflow/jobs/index_match_metrics.py --run-date {RUN_DATE}"
        ),
    )

    ingest_balldontlie >> format_balldontlie
    ingest_thesportsdb >> format_thesportsdb
    [format_balldontlie, format_thesportsdb] >> combine_metrics
    combine_metrics >> train_predict
    train_predict >> [index_team_metrics, index_match_metrics]

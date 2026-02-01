import os
from typing import Optional


class SparkConfig:
    def __init__(self) -> None:
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.bucket = os.getenv("MINIO_BUCKET", "datalake")

    def s3a_path(self, key: str) -> str:
        return f"s3a://{self.bucket}/{key}"


def configure_spark(spark, config: SparkConfig) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    hadoop_conf.set("fs.s3a.endpoint", config.minio_endpoint)
    hadoop_conf.set("fs.s3a.access.key", config.minio_access_key)
    hadoop_conf.set("fs.s3a.secret.key", config.minio_secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

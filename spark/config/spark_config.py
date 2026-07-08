"""Central Spark session factory and pipeline paths.

All jobs get their SparkSession here so tuning lives in ONE place
(docs/09_performance.md explains each setting).
"""
from pathlib import Path

from pyspark.sql import SparkSession

# lake layout — relative to the repo root so the project runs anywhere
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUARANTINE_DIR = SILVER_DIR / "_quarantine"

# SQL Server target for job 05 (override via environment in real deployments)
JDBC_URL = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=TrafficDW;encrypt=true;trustServerCertificate=true"
)
JDBC_PROPS = {
    "user": "etl_spark",
    "password": "ChangeMe_Demo1!",  # demo only — use a secret store in production
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "batchsize": "10000",
}


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(f"TrafficDW::{app_name}")
        .master("local[*]")  # cluster deployments override via spark-submit --master
        # shuffle behaviour: AQE resizes/skew-splits partitions at runtime,
        # so the static number only sets the upper bound before coalescing
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # idempotent day-level re-runs: overwrite ONLY the partitions written
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # cheaper serialization in shuffles/caches
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

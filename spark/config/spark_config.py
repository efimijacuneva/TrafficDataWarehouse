"""Central Spark session factory and pipeline paths.

All jobs get their SparkSession here so tuning lives in ONE place
(docs/09_performance.md explains each setting).
"""
import os
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

# SQL Server target for job 05.
#
# CREDENTIALS come from the environment, with the demo values as a fallback so
# the project still runs out of the box. Set ETL_SPARK_PASSWORD (and the others)
# in the shell, in docker-compose, or from a secret store and nothing in this
# repository holds a real credential:
#
#     $env:ETL_SPARK_PASSWORD = "..."      # PowerShell
#     export ETL_SPARK_PASSWORD=...        # bash
#
# The same fallback value is used by sql/etl/00_security.sql when it creates the
# login, so the two stay consistent for a demo run.
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST", "mssql")
SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT", "1433")
SQL_DATABASE = os.getenv("SQL_DATABASE", "TrafficDW")
ETL_SPARK_USER = os.getenv("ETL_SPARK_USER", "etl_spark")
ETL_SPARK_PASSWORD = os.getenv("ETL_SPARK_PASSWORD", "ChangeMe_Demo1!")  # demo fallback

JDBC_URL = (
    f"jdbc:sqlserver://{SQL_SERVER_HOST}:{SQL_SERVER_PORT};"
    f"databaseName={SQL_DATABASE};encrypt=true;trustServerCertificate=true"
)
JDBC_PROPS = {
    "user": ETL_SPARK_USER,
    "password": ETL_SPARK_PASSWORD,
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
        # Idempotent day-level re-runs: overwrite ONLY the partitions written.
        #
        # The trade-off to know: dynamic overwrite REPLACES the partitions a job
        # writes and NEVER REMOVES ones it does not touch. Re-generating the
        # source with a different date range or seed therefore leaves orphaned
        # partitions behind, and a later full-layer read (job 04 scans all of
        # gold) would silently include them. `scripts/clean_lake.ps1` prunes
        # partitions outside the current raw feed; Delta Lake's replaceWhere
        # would remove the need for that entirely (docs/11).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # cheaper serialization in shuffles/caches
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

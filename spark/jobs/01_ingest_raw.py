"""Job 01 — INGEST: raw landing zone → bronze Parquet.

Reads the three feed types exactly as they arrive:
  * sensor_readings_*.csv   — detector telemetry (CSV, explicit schema)
  * camera_events_*.json    — ANPR camera events (JSON)
  * weather_observations_*.json — weather archive (JSON)

BRONZE CONTRACT
---------------
Byte-faithful content + audit columns, immutable, partitioned by **ingest_date**.

`ingest_date` is the date of the FILE the row arrived in (derived from the
source filename), NOT `event_date` derived from the row's own timestamp.
That distinction is the whole point of the bronze zone:

    bronze is organised by WHEN DATA ARRIVED,
    not by a value INSIDE the data — which may itself be corrupt.

Partitioning bronze on an event-derived value is a silent-data-loss bug: a row
whose timestamp is wrong (clock skew, injected `future_ts` defect, NULL) lands
in a partition nobody looks in, so job 02's per-date read never sees it and it
reaches neither silver nor quarantine. `event_date` is still written as an
ordinary column so downstream jobs can use it — it just no longer decides
where the row is stored.

PERMISSIVE mode captures malformed CSV lines into `_corrupt_record` instead of
failing the batch — they stay in bronze and are counted, so nothing is lost.

Run:  spark-submit spark/jobs/01_ingest_raw.py [--date 2026-06-15]
"""
import argparse
import re
import sys
import uuid
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.spark_config import BRONZE_DIR, RAW_DIR, get_spark

# explicit schema: no inference pass, stable types, corrupt-line capture
SENSOR_CSV_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("sensor_serial", StringType()),
        StructField("segment_code", StringType()),
        StructField("direction", StringType()),
        StructField("vehicle_type_code", StringType()),
        StructField("speed_kmh", DecimalType(5, 1)),
        StructField("headway_seconds", DecimalType(6, 2)),
        StructField("occupancy_pct", DecimalType(5, 2)),
        StructField("_corrupt_record", StringType()),
    ]
)

# raw filenames always end in _YYYYMMDD.<ext> — that stamp IS the ingest date
_FILE_DATE = re.compile(r"_(\d{4})(\d{2})(\d{2})\.[a-z]+$")


def ingest_date_col():
    """ingest_date derived from the source FILENAME, not from row content.

    A row can lie about its own timestamp; it cannot lie about which file it
    arrived in. regexp_extract returns '' for an unexpected filename, and
    to_date('') is NULL, which surfaces loudly rather than corrupting a real
    partition.
    """
    return F.to_date(
        F.regexp_extract(F.input_file_name(), r"_(\d{8})\.[a-z]+$", 1), "yyyyMMdd"
    )


def add_audit(df, source: str, batch_id: str):
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source", F.lit(source))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("ingest_date", ingest_date_col())
    )


def write_bronze(df, name: str):
    """One tidy file set per ingest_date partition, zstd (write-once archive)."""
    (
        df.repartition("ingest_date")
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("ingest_date")
        .option("compression", "zstd")
        .parquet(str(BRONZE_DIR / name))
    )


def main(load_date: Optional[str]) -> None:
    spark = get_spark("01_ingest_raw")
    batch_id = str(uuid.uuid4())
    date_glob = load_date.replace("-", "") if load_date else "*"

    # ---------------------------------------------------------- sensor CSV --
    sensor_files = str(RAW_DIR / f"sensor_readings_{date_glob}.csv")
    sensors = (
        spark.read.schema(SENSOR_CSV_SCHEMA)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(sensor_files)
    )
    # event_date stays as a COLUMN (never a partition key) — it is data, and
    # data can be wrong. Rows whose event_date != ingest_date are exactly the
    # ones the silver gate must inspect (future timestamps, clock skew).
    sensors = add_audit(sensors, "sensor_csv", batch_id).withColumn(
        "event_date", F.to_date("event_ts")
    )
    sensors.cache()
    total = sensors.count()
    corrupt = sensors.filter(F.col("_corrupt_record").isNotNull()).count()
    off_date = sensors.filter(
        F.col("event_date").isNull() | (F.col("event_date") != F.col("ingest_date"))
    ).count()
    write_bronze(sensors, "sensor_readings")
    print(
        f"[bronze] sensor_readings: {total:,} rows "
        f"({corrupt:,} corrupt captured, {off_date:,} with event_date != ingest_date "
        f"— retained for the silver gate)"
    )
    sensors.unpersist()

    # ---------------------------------------------------------- camera JSON --
    camera_files = str(RAW_DIR / f"camera_events_{date_glob}.json")
    cameras = spark.read.option("multiLine", True).json(camera_files)
    cameras = (
        add_audit(cameras, "camera_json", batch_id)
        # nested JSON: detection payload is a struct → flatten what bronze
        # consumers filter on, keep the original struct for full fidelity
        .withColumn("event_ts", F.to_timestamp(F.col("detection.timestamp")))
        .withColumn("event_date", F.to_date("event_ts"))
    )
    write_bronze(cameras, "camera_events")
    print(f"[bronze] camera_events: {cameras.count():,} rows")

    # --------------------------------------------------------- weather JSON --
    weather_files = str(RAW_DIR / f"weather_observations_{date_glob}.json")
    weather = spark.read.option("multiLine", True).json(weather_files)
    weather = add_audit(weather, "weather_json", batch_id).withColumn(
        "event_date", F.to_date(F.to_timestamp("observed_at"))
    )
    write_bronze(weather, "weather_observations")
    print(f"[bronze] weather_observations: {weather.count():,} rows")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYY-MM-DD; omit = all raw files")
    main(parser.parse_args().date)

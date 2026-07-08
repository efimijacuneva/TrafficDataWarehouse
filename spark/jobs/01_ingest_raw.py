"""Job 01 — INGEST: raw landing zone → bronze Parquet.

Reads the three feed types exactly as they arrive:
  * sensor_readings_*.csv   — detector telemetry (CSV, explicit schema)
  * camera_events_*.json    — ANPR camera events (JSON)
  * weather_observations_*.json — weather archive (JSON)

Bronze contract: byte-faithful content + audit columns, immutable, partitioned
by event_date. PERMISSIVE mode captures malformed CSV lines into
_corrupt_record instead of failing the batch — they stay in bronze and are
counted, so nothing is silently lost.

Run:  spark-submit spark/jobs/01_ingest_raw.py [--date 2026-06-15]
"""
import argparse
import sys
import uuid
from pathlib import Path

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


def add_audit(df, source: str, batch_id: str):
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source", F.lit(source))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def main(load_date: str | None) -> None:
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
    sensors = add_audit(sensors, "sensor_csv", batch_id).withColumn(
        "event_date", F.to_date("event_ts")
    )
    (
        sensors.repartition("event_date")  # one tidy file set per date partition
        .write.mode("overwrite")
        .partitionBy("event_date")
        .option("compression", "zstd")  # bronze = write-once archive → smallest
        .parquet(str(BRONZE_DIR / "sensor_readings"))
    )
    corrupt = sensors.filter(F.col("_corrupt_record").isNotNull()).count()
    print(f"[bronze] sensor_readings: {sensors.count():,} rows ({corrupt:,} corrupt captured)")

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
    (
        cameras.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .option("compression", "zstd")
        .parquet(str(BRONZE_DIR / "camera_events"))
    )
    print(f"[bronze] camera_events: {cameras.count():,} rows")

    # --------------------------------------------------------- weather JSON --
    weather_files = str(RAW_DIR / f"weather_observations_{date_glob}.json")
    weather = spark.read.option("multiLine", True).json(weather_files)
    weather = add_audit(weather, "weather_json", batch_id).withColumn(
        "event_date", F.to_date(F.to_timestamp("observed_at"))
    )
    (
        weather.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .option("compression", "zstd")
        .parquet(str(BRONZE_DIR / "weather_observations"))
    )
    print(f"[bronze] weather_observations: {weather.count():,} rows")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYY-MM-DD; omit = all raw files")
    main(parser.parse_args().date)

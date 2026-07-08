"""Job 02 — CLEAN & VALIDATE: bronze → silver (the data-quality gate).

* normalizes column values (trim, upper-case codes, sentinel → NULL)
* applies the shared rule set (utils/quality.py) — bad rows go to
  silver/_quarantine with a reject_reason, never dropped silently
* deduplicates on the business event identity (event_id), keeping the
  latest arrival (window + row_number)
* unifies sensor-CSV and camera-JSON detections into ONE conformed
  silver dataset: traffic_events
* emits per-rule quality metrics (feeds the sensor-health KPI)

Run:  spark-submit spark/jobs/02_clean_validate.py [--date 2026-06-15]
"""
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config.spark_config import BRONZE_DIR, QUARANTINE_DIR, SILVER_DIR, get_spark
from utils.quality import apply_rules

SENTINELS = ("", "N/A", "NA", "-", "NULL", "null")


def normalize(df: DataFrame) -> DataFrame:
    """Cleansing: trim strings, upper-case codes, sentinel strings → NULL."""
    for c in ("sensor_serial", "segment_code", "vehicle_type_code", "direction"):
        df = df.withColumn(c, F.upper(F.trim(F.col(c))))
        df = df.withColumn(c, F.when(F.col(c).isin(*SENTINELS), None).otherwise(F.col(c)))
    return df


def main(load_date: str | None) -> None:
    spark = get_spark("02_clean_validate")

    # ------------------------------------------------------ read bronze -----
    sensors = spark.read.parquet(str(BRONZE_DIR / "sensor_readings"))
    cameras = spark.read.parquet(str(BRONZE_DIR / "camera_events"))
    if load_date:  # partition pruning: only the requested date's directories are read
        sensors = sensors.filter(F.col("event_date") == load_date)
        cameras = cameras.filter(F.col("event_date") == load_date)

    # drop CSV lines that failed parsing (already archived in bronze)
    sensors = sensors.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    # -------------------------- conform camera events to the sensor shape ---
    cameras_conformed = cameras.select(
        F.col("event_id"),
        F.col("event_ts"),
        F.col("camera_code").alias("sensor_serial"),  # cameras are detectors too
        F.col("detection.segment_code").alias("segment_code"),
        F.col("detection.direction").alias("direction"),
        F.col("detection.vehicle_class").alias("vehicle_type_code"),
        F.col("detection.speed_kmh").cast("decimal(5,1)").alias("speed_kmh"),
        F.lit(None).cast("decimal(6,2)").alias("headway_seconds"),
        F.lit(None).cast("decimal(5,2)").alias("occupancy_pct"),
        F.col("_ingest_ts"),
        F.col("event_date"),
    )
    sensors_conformed = sensors.select(*cameras_conformed.columns)
    events = sensors_conformed.unionByName(cameras_conformed)

    # ------------------------------------------------- cleanse + validate ---
    events = normalize(events)
    events = apply_rules(events)

    # -------------------------------------------- duplicate removal ---------
    # business identity = event_id; keep the LATEST arrival (highest _ingest_ts)
    ident = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())
    events = events.withColumn("_rn", F.row_number().over(ident))
    events = events.withColumn(
        "reject_reason",
        F.when(F.col("_rn") > 1, F.lit("DUPLICATE")).otherwise(F.col("reject_reason")),
    ).drop("_rn")

    good = events.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    bad = events.filter(F.col("reject_reason").isNotNull())

    # ------------------------------------------------------- write silver ---
    (
        good.repartition("event_date")
        .sortWithinPartitions("segment_code", "event_ts")  # RLE + tight min/max stats
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(SILVER_DIR / "traffic_events"))
    )
    (
        bad.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(QUARANTINE_DIR / "traffic_events"))
    )

    # -------------------------------------------------- weather → silver ----
    weather = spark.read.parquet(str(BRONZE_DIR / "weather_observations"))
    if load_date:
        weather = weather.filter(F.col("event_date") == load_date)
    weather_silver = (
        weather.select(
            F.col("station_code"),
            F.to_timestamp("observed_at").alias("observed_ts"),
            F.col("temperature_c").cast("decimal(4,1)"),
            F.col("precipitation_mm").cast("decimal(5,1)"),
            F.col("wind_speed_kmh").cast("decimal(5,1)"),
            F.col("visibility_m").cast("int"),
            F.upper(F.trim("condition_code")).alias("condition_code"),
            F.col("event_date"),
        )
        .dropDuplicates(["station_code", "observed_ts"])
        .filter(F.col("observed_ts").isNotNull())
    )
    (
        weather_silver.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(SILVER_DIR / "weather_observations"))
    )

    # ------------------------------------------------- quality metrics ------
    metrics = (
        bad.groupBy("event_date", "reject_reason")
        .agg(F.count("*").alias("rejected_rows"))
        .withColumn("measured_at", F.current_timestamp())
    )
    metrics.write.mode("overwrite").partitionBy("event_date").parquet(
        str(SILVER_DIR / "quality_metrics")
    )

    print(f"[silver] traffic_events: {good.count():,} good / {bad.count():,} quarantined")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYY-MM-DD; omit = all dates")
    main(parser.parse_args().date)

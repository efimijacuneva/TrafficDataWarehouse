"""Job 02 — CLEAN & VALIDATE: bronze → silver (the data-quality gate).

THE NO-SILENT-LOSS CONTRACT
---------------------------
Every row read from bronze for a load date leaves this job in exactly one of
two places:

    silver/traffic_events          (clean)
    silver/_quarantine/...         (with a reject_reason)

and `silver/reconciliation` records the arithmetic proving it:

    rows_in == rows_good + rows_quarantined

The read is filtered on **ingest_date** — the date of the file the row arrived
in — NOT on `event_date` derived from the row's own timestamp. Filtering on an
event-derived partition is how rows disappear: a row with a future or NULL
timestamp is written to a partition the per-date read never opens, so it
reaches neither silver nor quarantine. See spark/jobs/01_ingest_raw.py.

DETECTOR MODELLING
------------------
Sensor CSV and camera JSON are two feeds of the SAME business event (a vehicle
detected at a point). They are conformed into one silver dataset, but their
identity is kept distinct:

    detector_type = 'SENSOR' → sensor_serial populated, camera_code NULL
    detector_type = 'CAMERA' → camera_code   populated, sensor_serial NULL
    detector_code            = whichever applies (used by the shared rules)

Previously `camera_code` was aliased straight into `sensor_serial`, which made
every camera an inferred DimSensor member that could never be completed, while
dim.DimTrafficCamera was referenced by no fact at all. Keeping the two codes in
separate columns lets fact.FactTrafficEvent carry BOTH SensorKey and CameraKey,
each falling back to the unknown member (-1) when it does not apply.

Run:  spark-submit spark/jobs/02_clean_validate.py [--date 2026-06-15]
"""
import argparse
import sys
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config.spark_config import BRONZE_DIR, QUARANTINE_DIR, SILVER_DIR, get_spark
from utils.quality import apply_rules

SENTINELS = ("", "N/A", "NA", "-", "NULL", "null")

# the conformed silver shape both feeds are projected onto
CONFORMED_COLUMNS = [
    "event_id",
    "event_ts",
    "detector_type",
    "detector_code",
    "sensor_serial",
    "camera_code",
    "segment_code",
    "direction",
    "vehicle_type_code",
    "speed_kmh",
    "headway_seconds",
    "occupancy_pct",
    "_ingest_ts",
    "_source_file",
    "event_date",
    "ingest_date",
]


def normalize(df: DataFrame) -> DataFrame:
    """Cleansing: trim strings, upper-case codes, sentinel strings → NULL."""
    for c in ("detector_code", "sensor_serial", "camera_code",
              "segment_code", "vehicle_type_code", "direction"):
        df = df.withColumn(c, F.upper(F.trim(F.col(c))))
        df = df.withColumn(c, F.when(F.col(c).isin(*SENTINELS), None).otherwise(F.col(c)))
    return df


def main(load_date: Optional[str]) -> None:
    spark = get_spark("02_clean_validate")

    # ------------------------------------------------------ read bronze -----
    sensors = spark.read.parquet(str(BRONZE_DIR / "sensor_readings"))
    cameras = spark.read.parquet(str(BRONZE_DIR / "camera_events"))
    if load_date:
        # partition pruning on INGEST date — reads every row that arrived in
        # this batch, including rows whose own timestamp is wrong.
        sensors = sensors.filter(F.col("ingest_date") == load_date)
        cameras = cameras.filter(F.col("ingest_date") == load_date)

    # malformed CSV lines are counted, then quarantined with an explicit reason
    corrupt = (
        sensors.filter(F.col("_corrupt_record").isNotNull())
        .select(
            F.col("event_id"), F.col("event_ts"),
            F.lit("SENSOR").alias("detector_type"),
            F.col("sensor_serial").alias("detector_code"),
            F.col("sensor_serial"), F.lit(None).cast("string").alias("camera_code"),
            F.col("segment_code"), F.col("direction"), F.col("vehicle_type_code"),
            F.col("speed_kmh"), F.col("headway_seconds"), F.col("occupancy_pct"),
            F.col("_ingest_ts"), F.col("_source_file"),
            F.col("event_date"), F.col("ingest_date"),
        )
        .withColumn("reject_reason", F.lit("CORRUPT_RECORD"))
    )
    sensors = sensors.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    # -------------------------------- conform both feeds to one silver shape --
    sensors_conformed = sensors.select(
        F.col("event_id"),
        F.col("event_ts"),
        F.lit("SENSOR").alias("detector_type"),
        F.col("sensor_serial").alias("detector_code"),
        F.col("sensor_serial"),
        F.lit(None).cast("string").alias("camera_code"),
        F.col("segment_code"),
        F.col("direction"),
        F.col("vehicle_type_code"),
        F.col("speed_kmh"),
        F.col("headway_seconds"),
        F.col("occupancy_pct"),
        F.col("_ingest_ts"),
        F.col("_source_file"),
        F.col("event_date"),
        F.col("ingest_date"),
    )

    cameras_conformed = cameras.select(
        F.col("event_id"),
        F.col("event_ts"),
        F.lit("CAMERA").alias("detector_type"),
        F.col("camera_code").alias("detector_code"),
        F.lit(None).cast("string").alias("sensor_serial"),
        F.col("camera_code"),
        F.col("detection.segment_code").alias("segment_code"),
        F.col("detection.direction").alias("direction"),
        F.col("detection.vehicle_class").alias("vehicle_type_code"),
        F.col("detection.speed_kmh").cast("decimal(5,1)").alias("speed_kmh"),
        F.lit(None).cast("decimal(6,2)").alias("headway_seconds"),
        F.lit(None).cast("decimal(5,2)").alias("occupancy_pct"),
        F.col("_ingest_ts"),
        F.col("_source_file"),
        F.col("event_date"),
        F.col("ingest_date"),
    )

    events = sensors_conformed.select(*CONFORMED_COLUMNS).unionByName(
        cameras_conformed.select(*CONFORMED_COLUMNS)
    )

    # ------------------------------------------------- cleanse + validate ---
    events = apply_rules(normalize(events))

    # -------------------------------------------- duplicate removal ---------
    # Business identity = event_id. _ingest_ts CANNOT be the tie-breaker: it is
    # current_timestamp(), which Spark evaluates once per query, so it is the
    # same constant on every row of a batch and row_number() would pick
    # arbitrarily (unreproducible between runs). Order by real, row-level
    # attributes instead so the survivor is deterministic.
    ident = Window.partitionBy("event_id").orderBy(
        F.col("event_ts").desc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
        F.col("detector_code").asc_nulls_last(),
    )
    events = events.withColumn("_rn", F.row_number().over(ident))
    events = events.withColumn(
        "reject_reason",
        F.when(F.col("_rn") > 1, F.lit("DUPLICATE")).otherwise(F.col("reject_reason")),
    ).drop("_rn")

    # corrupt rows bypass the rules (they have no parseable content to judge)
    events = events.unionByName(corrupt.select(*CONFORMED_COLUMNS, "reject_reason"))
    events.cache()  # read three times below: good, bad, and the row counts

    good = events.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    bad = events.filter(F.col("reject_reason").isNotNull())

    rows_in = events.count()
    rows_good = good.count()
    rows_bad = bad.count()

    # ------------------------------------------------------- write silver ---
    (
        good.repartition("event_date")
        .sortWithinPartitions("segment_code", "event_ts")  # RLE + tight min/max stats
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(str(SILVER_DIR / "traffic_events"))
    )
    # quarantine is partitioned by INGEST date: a quarantined row's event_date
    # is exactly the field that may be wrong, so it cannot organise storage.
    (
        bad.repartition("ingest_date")
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("ingest_date")
        .parquet(str(QUARANTINE_DIR / "traffic_events"))
    )

    # -------------------------------------------------- weather → silver ----
    weather = spark.read.parquet(str(BRONZE_DIR / "weather_observations"))
    if load_date:
        weather = weather.filter(F.col("ingest_date") == load_date)
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
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(str(SILVER_DIR / "weather_observations"))
    )

    # ------------------------------------------------- quality metrics ------
    metrics = (
        bad.groupBy("ingest_date", "reject_reason")
        .agg(F.count("*").alias("rejected_rows"))
        .withColumn("measured_at", F.current_timestamp())
    )
    metrics.write.mode("overwrite").option(
        "partitionOverwriteMode", "dynamic"
    ).partitionBy("ingest_date").parquet(str(SILVER_DIR / "quality_metrics"))

    # ---------------------------------------------- reconciliation record ---
    # The no-silent-loss proof, written as data so tests, the quality gate and
    # the dashboard can all assert on it instead of trusting a log line.
    recon = spark.createDataFrame(
        [(rows_in, rows_good, rows_bad, rows_in == rows_good + rows_bad)],
        "rows_in long, rows_good long, rows_quarantined long, balanced boolean",
    ).withColumn("ingest_date", F.lit(load_date).cast("date")).withColumn(
        "measured_at", F.current_timestamp()
    )
    recon.write.mode("overwrite").option(
        "partitionOverwriteMode", "dynamic"
    ).partitionBy("ingest_date").parquet(str(SILVER_DIR / "reconciliation"))

    status = "BALANCED" if rows_in == rows_good + rows_bad else "*** IMBALANCE ***"
    print(
        f"[silver] traffic_events: {rows_in:,} in = {rows_good:,} good "
        f"+ {rows_bad:,} quarantined  [{status}]"
    )
    events.unpersist()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYY-MM-DD; omit = all dates")
    main(parser.parse_args().date)

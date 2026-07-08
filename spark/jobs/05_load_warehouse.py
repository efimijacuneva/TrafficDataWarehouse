"""Job 05 — LOAD: gold Parquet → SQL Server staging (JDBC).

Spark hands over to T-SQL here: gold datasets are bulk-written to stg.* and
the warehouse procs (etl.usp_RunNightlyPipeline) perform SK lookups, SCD
handling and fact loads. Spark deliberately does NOT write dim/fact directly —
conformance logic lives in one engine only (docs/07).

Idempotent per date: stg rows for the load date are replaced, not appended
(handled by the truncate-before-write below + the procs' delete-by-date).

Requires the MS SQL JDBC driver, e.g.:
  spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11 \
      spark/jobs/05_load_warehouse.py --date 2026-06-15
"""
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from config.spark_config import GOLD_DIR, JDBC_PROPS, JDBC_URL, get_spark


def write_jdbc(df, table: str, truncate: bool) -> None:
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .option("batchsize", JDBC_PROPS["batchsize"])
        .option("truncate", str(truncate).lower())
        .mode("overwrite" if truncate else "append")
        .save()
    )


def main(load_date: str) -> None:
    spark = get_spark("05_load_warehouse")

    # ---------------------------------------------------- stg.TrafficEvent --
    events = (
        spark.read.parquet(str(GOLD_DIR / "traffic_events"))
        .filter(F.col("event_date") == load_date)  # partition pruning
        .select(
            F.col("event_id").alias("EventID"),
            F.col("event_ts").alias("EventTimestamp"),
            F.col("sensor_serial").alias("SensorSerial"),
            F.col("segment_code").alias("SegmentCode"),
            F.col("vehicle_type_code").alias("VehicleTypeCode"),
            F.col("speed_kmh").alias("SpeedKmh"),
            F.col("headway_seconds").alias("HeadwaySeconds"),
            F.col("occupancy_pct").alias("OccupancyPct"),
            F.col("condition_code").alias("ConditionCode"),
            F.col("avg_temp_c").alias("TempC"),
            F.col("precipitation_mm").alias("PrecipitationMm"),
            F.col("visibility_m").alias("VisibilityM"),
            F.col("event_date").alias("LoadDate"),
        )
    )
    write_jdbc(events, "stg.TrafficEvent", truncate=True)
    print(f"[jdbc] stg.TrafficEvent ← {events.count():,} rows for {load_date}")

    # --------------------------------------------------- stg.HourlyTraffic --
    hourly = (
        spark.read.parquet(str(GOLD_DIR / "hourly_traffic"))
        .filter(F.col("event_date") == load_date)
        .select(
            F.col("event_date").alias("EventDate"),
            F.col("hour_of_day").alias("HourOfDay"),
            F.col("segment_code").alias("SegmentCode"),
            F.col("vehicle_count").alias("VehicleCount"),
            F.col("heavy_vehicle_count").alias("HeavyVehicleCount"),
            F.col("avg_speed_kmh").alias("AvgSpeedKmh"),
            F.col("p85_speed_kmh").alias("P85SpeedKmh"),
            F.col("avg_occupancy_pct").alias("AvgOccupancyPct"),
            F.col("condition_code").alias("ConditionCode"),
            F.col("avg_temp_c").alias("TempBandSource"),
            F.col("precipitation_mm").alias("PrecipitationMm"),
            F.col("visibility_m").alias("VisibilityM"),
        )
    )
    write_jdbc(hourly, "stg.HourlyTraffic", truncate=True)
    print(f"[jdbc] stg.HourlyTraffic ← {hourly.count():,} rows for {load_date}")

    print("Handover complete — run: EXEC etl.usp_RunNightlyPipeline @LoadDate = "
          f"'{load_date}';")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD load date")
    main(parser.parse_args().date)

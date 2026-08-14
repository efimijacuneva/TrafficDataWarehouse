"""Job 03 — TRANSFORM & AGGREGATE: silver → gold hourly grain.

* enriches detections with the hour's city-wide weather aggregate (worst
  condition across stations, joined via BROADCAST — the weather side is tiny)
* aggregates to the warehouse snapshot grain: segment × hour
* deliberately mixes the two APIs (docs/07):
    - DataFrame API for pipeline plumbing (joins, caching, writes)
    - Spark SQL for the business aggregations (analyst-readable)

Run:  spark-submit spark/jobs/03_transform_aggregate.py [--date 2026-06-15]
"""
import argparse
import sys
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from config.spark_config import GOLD_DIR, SILVER_DIR, get_spark

HEAVY_CLASSES = ("TRK", "ART", "BUS")


def main(load_date: Optional[str]) -> None:
    spark = get_spark("03_transform_aggregate")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    events = spark.read.parquet(str(SILVER_DIR / "traffic_events"))
    weather = spark.read.parquet(str(SILVER_DIR / "weather_observations"))
    if load_date:
        events = events.filter(F.col("event_date") == load_date)
        weather = weather.filter(F.col("event_date") == load_date)

    # cached: reused by the hourly aggregate AND the KPI job's input write
    events = events.withColumn("event_hour", F.hour("event_ts")).cache()

    # ---------------- weather at hourly grain (city-wide average of stations)
    weather_hourly = (
        weather.withColumn("obs_hour", F.hour("observed_ts"))
        .groupBy("event_date", "obs_hour")
        .agg(
            F.avg("temperature_c").alias("avg_temp_c"),
            # AVG, not SUM: the three stations are three MEASUREMENTS OF THE
            # SAME HOUR of city weather, not three separate rainfalls. Summing
            # them tripled the hourly precipitation, which then pushed the
            # weather banding (etl.fn_WeatherBands: <2.5 Light, <7.5 Moderate,
            # else Heavy) into the wrong band and skewed every weather KPI.
            F.avg("precipitation_mm").alias("precipitation_mm"),
            # MIN for visibility is deliberate and different: visibility is a
            # hazard measure, so the worst reading in the city is the one that
            # matters — consistent with taking the WORST condition below.
            F.min("visibility_m").alias("visibility_m"),
            # dominant condition of the hour = the worst one reported
            F.max(
                F.when(F.col("condition_code") == "STORM", 6)
                .when(F.col("condition_code") == "ICE", 5)
                .when(F.col("condition_code") == "SNOW", 4)
                .when(F.col("condition_code") == "FOG", 3)
                .when(F.col("condition_code") == "RAIN", 2)
                .otherwise(1)
            ).alias("severity_rank"),
        )
        .withColumn(
            "condition_code",
            F.element_at(
                F.array(*[F.lit(c) for c in ("DRY", "RAIN", "FOG", "SNOW", "ICE", "STORM")]),
                F.col("severity_rank"),
            ),
        )
        .drop("severity_rank")
    )

    # broadcast join: ≤ 24 weather rows/day vs millions of detections —
    # ships the small side to every executor, zero shuffle of the big side
    enriched = events.join(
        broadcast(weather_hourly),
        (events.event_date == weather_hourly.event_date)
        & (events.event_hour == weather_hourly.obs_hour),
        "left",
    ).drop(weather_hourly.event_date).drop("obs_hour")

    # ------------------------------- business aggregation in SPARK SQL ------
    enriched.createOrReplaceTempView("traffic_enriched")
    heavy_list = ", ".join(f"'{c}'" for c in HEAVY_CLASSES)

    hourly = spark.sql(
        f"""
        SELECT
            event_date,
            event_hour                                        AS hour_of_day,
            segment_code,
            COUNT(*)                                          AS vehicle_count,
            SUM(CASE WHEN vehicle_type_code IN ({heavy_list})
                     THEN 1 ELSE 0 END)                       AS heavy_vehicle_count,
            ROUND(AVG(speed_kmh), 1)                          AS avg_speed_kmh,
            ROUND(PERCENTILE_APPROX(speed_kmh, 0.85), 1)      AS p85_speed_kmh,
            ROUND(AVG(occupancy_pct), 2)                      AS avg_occupancy_pct,
            FIRST(condition_code, TRUE)                       AS condition_code,
            ROUND(AVG(avg_temp_c), 1)                         AS avg_temp_c,
            ROUND(FIRST(precipitation_mm, TRUE), 1)           AS precipitation_mm,
            FIRST(visibility_m, TRUE)                         AS visibility_m
        FROM traffic_enriched
        WHERE segment_code IS NOT NULL
        GROUP BY event_date, event_hour, segment_code
        """
    )

    (
        hourly.repartition("event_date")
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(str(GOLD_DIR / "hourly_traffic"))
    )
    print(f"[gold] hourly_traffic: {hourly.count():,} segment-hours")

    # enriched event-grain detail also lands in gold for the DW transaction fact.
    # detector_type / sensor_serial / camera_code are all carried through so the
    # warehouse can resolve BOTH DimSensor and DimTrafficCamera (job 02 header).
    (
        enriched.select(
            "event_id", "event_ts", "event_date",
            "detector_type", "sensor_serial", "camera_code", "segment_code",
            "vehicle_type_code", "speed_kmh", "headway_seconds", "occupancy_pct",
            "condition_code", "avg_temp_c", "precipitation_mm", "visibility_m",
        )
        .repartition("event_date")
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(str(GOLD_DIR / "traffic_events"))
    )

    events.unpersist()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYY-MM-DD; omit = all dates")
    main(parser.parse_args().date)

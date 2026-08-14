"""Job 04 — KPIs: gold hourly grain → gold KPI datasets.

Window functions over the gold layer produce the KPI catalogue of docs/01:
  * kpi_segment_daily   — congestion index, peak hour, daily rank per segment
  * kpi_weather_penalty — % speed drop per condition vs. the DRY baseline
  * kpi_rush_hours      — top-3 volume hours per segment/weekday (peak prediction)
  * quality_daily       — reject rates per rule (sensor-health KPI)

Spark SQL is used throughout — these queries are the lake-side mirror of the
warehouse analytics in sql/analytics/, demonstrating the same logic in both engines.

Run:  spark-submit spark/jobs/04_generate_kpis.py
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from config.spark_config import GOLD_DIR, SILVER_DIR, get_spark


def main() -> None:
    spark = get_spark("04_generate_kpis")

    spark.read.parquet(str(GOLD_DIR / "hourly_traffic")).createOrReplaceTempView("hourly")

    # ------------------------------------------------ segment daily KPIs ----
    kpi_segment_daily = spark.sql(
        """
        WITH daily AS (
            SELECT event_date, segment_code,
                   SUM(vehicle_count)                        AS day_volume,
                   ROUND(AVG(avg_speed_kmh), 1)              AS avg_speed_kmh,
                   MAX_BY(hour_of_day, vehicle_count)        AS peak_hour,
                   MAX(vehicle_count)                        AS peak_hour_volume
            FROM hourly
            GROUP BY event_date, segment_code
        )
        SELECT *,
               RANK() OVER (PARTITION BY event_date ORDER BY day_volume DESC)
                   AS volume_rank_in_city,
               ROUND(day_volume - LAG(day_volume) OVER (
                     PARTITION BY segment_code ORDER BY event_date), 0)
                   AS volume_delta_vs_prev_day
        FROM daily
        """
    )
    kpi_segment_daily.write.mode("overwrite").partitionBy("event_date").parquet(
        str(GOLD_DIR / "kpi_segment_daily")
    )

    # -------------------------------------------- weather speed penalty -----
    kpi_weather_penalty = spark.sql(
        """
        WITH by_condition AS (
            SELECT segment_code, condition_code,
                   AVG(avg_speed_kmh) AS avg_speed
            FROM hourly
            WHERE condition_code IS NOT NULL AND avg_speed_kmh IS NOT NULL
            GROUP BY segment_code, condition_code
        ),
        with_baseline AS (
            SELECT *,
                   MAX(CASE WHEN condition_code = 'DRY' THEN avg_speed END)
                       OVER (PARTITION BY segment_code) AS dry_baseline
            FROM by_condition
        )
        SELECT segment_code, condition_code,
               ROUND(avg_speed, 1)   AS avg_speed,
               ROUND(dry_baseline, 1) AS dry_baseline,
               ROUND(100.0 * (dry_baseline - avg_speed) / dry_baseline, 1)
                   AS speed_penalty_pct
        FROM with_baseline
        WHERE dry_baseline IS NOT NULL AND condition_code <> 'DRY'
        """
    )
    kpi_weather_penalty.write.mode("overwrite").parquet(str(GOLD_DIR / "kpi_weather_penalty"))

    # ------------------------------------ rush hours / peak prediction ------
    # historical peak pattern = 85th percentile volume per segment-hour-weekday;
    # the top-3 hours per segment/weekday are 'predicted peaks' for planning
    kpi_rush_hours = spark.sql(
        """
        WITH hist AS (
            SELECT segment_code,
                   DAYOFWEEK(event_date)  AS weekday,
                   hour_of_day,
                   PERCENTILE_APPROX(vehicle_count, 0.85) AS p85_volume
            FROM hourly
            GROUP BY segment_code, DAYOFWEEK(event_date), hour_of_day
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY segment_code, weekday
                                      ORDER BY p85_volume DESC) AS rn
            FROM hist
        )
        SELECT segment_code, weekday, hour_of_day AS predicted_peak_hour, p85_volume
        FROM ranked
        WHERE rn <= 3
        """
    )
    kpi_rush_hours.write.mode("overwrite").parquet(str(GOLD_DIR / "kpi_rush_hours"))

    # -------------------------------------------------- quality / health ----
    # TWO DIFFERENT DATE KEYS MEET HERE, and the distinction matters:
    #   * silver/quality_metrics is keyed by INGEST_DATE - the date of the file a
    #     row arrived in. Quarantined rows are exactly the ones whose own
    #     timestamp may be wrong (FUTURE_TIMESTAMP, DATE_MISMATCH, MISSING_KEY),
    #     so their event_date cannot organise anything.
    #   * gold/hourly_traffic is keyed by EVENT_DATE, because by then every row
    #     has passed validation.
    # For rows that pass, the two are equal BY CONSTRUCTION - DATE_MISMATCH
    # quarantines anything where they differ - so the batch date is the correct
    # join key for a per-day "how healthy was this load" summary. Both sides are
    # renamed to load_date so the joined output cannot be misread as either one.
    quality = (
        spark.read.parquet(str(SILVER_DIR / "quality_metrics"))
        .withColumnRenamed("ingest_date", "load_date")
    )
    good_counts = spark.sql(
        "SELECT event_date AS load_date, SUM(vehicle_count) AS valid_detections "
        "FROM hourly GROUP BY event_date"
    )
    quality_daily = (
        quality.groupBy("load_date")
        .pivot("reject_reason")
        .sum("rejected_rows")
        .join(good_counts, "load_date", "full")
        .na.fill(0)
    )
    quality_daily.write.mode("overwrite").parquet(str(GOLD_DIR / "quality_daily"))

    for name in ("kpi_segment_daily", "kpi_weather_penalty", "kpi_rush_hours", "quality_daily"):
        print(f"[gold] {name} written")
    spark.stop()


if __name__ == "__main__":
    main()

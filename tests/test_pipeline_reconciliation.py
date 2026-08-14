"""Tier 2 — the no-silent-loss proof, end to end across the lake.

THIS IS THE TEST THAT WOULD HAVE CAUGHT THE 689 LOST ROWS.

The project's loudest claim is that rejected rows are quarantined, never
dropped. Nothing enforced it: `_manifest.json` was generated for exactly this
purpose and never read back, and every reconciliation check lived downstream of
staging, so a row lost between bronze and silver was invisible to all of them.

The assertion chain here is:

    raw files  ==  bronze
    bronze     ==  silver + quarantine        (per ingest_date)
    silver     ==  gold event grain

Requires a completed Spark run. Skips with a reason otherwise — it never
silently passes.
"""
import json
from pathlib import Path

import pytest

from conftest import requires_spark

pytestmark = [pytest.mark.spark, requires_spark]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA = PROJECT_ROOT / "data"


def _require(path: Path, hint: str):
    if not path.exists() or not any(path.rglob("*.parquet")):
        pytest.skip(f"NOT VERIFIED - {path.relative_to(PROJECT_ROOT)} is empty. {hint}")
    return path


HINT = "Run the pipeline first: .\\scripts\\run_end_to_end.ps1"


# =============================================== the reconciliation record ==
def test_silver_reconciliation_record_balances(spark):
    """Job 02 writes its own arithmetic as data so it can be asserted on.

    rows_in == rows_good + rows_quarantined, per ingest_date. If this fails,
    rows vanished inside the validation gate.
    """
    path = _require(DATA / "silver" / "reconciliation", HINT)
    rows = spark.read.parquet(str(path)).collect()
    assert rows, "no reconciliation records written"

    for r in rows:
        assert r["rows_in"] == r["rows_good"] + r["rows_quarantined"], (
            f"ingest_date={r['ingest_date']}: {r['rows_in']:,} in but "
            f"{r['rows_good']:,} good + {r['rows_quarantined']:,} quarantined "
            f"= {r['rows_good'] + r['rows_quarantined']:,} — rows disappeared"
        )
        assert r["balanced"], f"ingest_date={r['ingest_date']} flagged unbalanced by the job"


def test_every_bronze_row_reaches_silver_or_quarantine(spark):
    """The bronze -> silver boundary, counted directly rather than trusted.

    Independent of the job's own reconciliation record: this recounts the
    Parquet itself, so it catches a wrong record as well as a wrong pipeline.
    """
    from pyspark.sql import functions as F

    bronze = _require(DATA / "bronze" / "sensor_readings", HINT)
    silver = _require(DATA / "silver" / "traffic_events", HINT)
    quarantine = _require(DATA / "silver" / "_quarantine" / "traffic_events", HINT)

    b = spark.read.parquet(str(bronze))
    s = spark.read.parquet(str(silver)).filter(F.col("detector_type") == "SENSOR")
    q = spark.read.parquet(str(quarantine)).filter(F.col("detector_type") == "SENSOR")

    # scope to ingest dates the silver layer actually covers, so a partial run
    # (3 of 7 days loaded) does not read as data loss
    processed = [r["ingest_date"] for r in q.select("ingest_date").distinct().collect()]
    if not processed:
        pytest.skip(f"NOT VERIFIED - no processed ingest dates found. {HINT}")

    bronze_n = b.filter(F.col("ingest_date").isin(processed)).count()
    quarantine_n = q.count()
    # clean rows are partitioned by event_date, which for a valid row equals its
    # ingest_date (DATE_MISMATCH quarantines anything where it does not)
    silver_n = s.filter(F.col("event_date").isin(processed)).count()

    assert bronze_n == silver_n + quarantine_n, (
        f"bronze {bronze_n:,} != silver {silver_n:,} + quarantine {quarantine_n:,} "
        f"(difference {bronze_n - silver_n - quarantine_n:+,}). "
        f"Rows are being dropped between bronze and silver."
    )


def test_future_dated_rows_reach_quarantine_not_oblivion(spark):
    """The specific regression: future-dated rows must be VISIBLE as rejects.

    Before the fix these rows were written to an event-derived bronze partition
    (event_date=2027-*) that the per-date silver read never opened. They existed
    in bronze and nowhere else — not in silver, not in quarantine, not in any
    count. Bronze is now partitioned by ingest_date, so the FUTURE_TIMESTAMP
    rule sees them and they land in quarantine with an explicit reason.
    """
    from pyspark.sql import functions as F

    quarantine = _require(DATA / "silver" / "_quarantine" / "traffic_events", HINT)
    q = spark.read.parquet(str(quarantine))

    reasons = {r["reject_reason"]: r["n"] for r in
               q.groupBy("reject_reason").agg(F.count("*").alias("n")).collect()}

    assert "FUTURE_TIMESTAMP" in reasons and reasons["FUTURE_TIMESTAMP"] > 0, (
        "No FUTURE_TIMESTAMP rejects in quarantine. The generator injects these "
        f"defects, so they must appear here. Reasons found: {sorted(reasons)}"
    )
    # and they must NOT have leaked into the clean layer
    silver = spark.read.parquet(str(_require(DATA / "silver" / "traffic_events", HINT)))
    leaked = silver.filter(F.col("event_ts") > F.current_timestamp()).count()
    assert leaked == 0, f"{leaked:,} future-dated rows reached silver"


def test_raw_manifest_reconciles_with_the_lake(spark):
    """Close the loop on _manifest.json — generated for this and never used."""
    from pyspark.sql import functions as F

    manifest_path = DATA / "raw" / "_manifest.json"
    if not manifest_path.exists():
        pytest.skip("NOT VERIFIED - data/raw/_manifest.json missing.")
    manifest = {e["date"]: e for e in json.loads(manifest_path.read_text(encoding="utf-8"))}

    bronze = spark.read.parquet(str(_require(DATA / "bronze" / "sensor_readings", HINT)))
    per_date = {
        str(r["ingest_date"]): r["n"]
        for r in bronze.groupBy("ingest_date").agg(F.count("*").alias("n")).collect()
    }

    checked = 0
    for day, entry in manifest.items():
        if day not in per_date:
            continue  # that date was not ingested in this run
        assert per_date[day] == entry["sensor_csv_rows"], (
            f"{day}: manifest {entry['sensor_csv_rows']:,} rows, bronze {per_date[day]:,} "
            f"— ingest dropped rows before any validation ran"
        )
        checked += 1

    if checked == 0:
        pytest.skip(f"NOT VERIFIED - no ingested dates overlap the manifest. {HINT}")


def test_gold_event_grain_matches_silver(spark):
    """Aggregation must not lose event-grain rows on the silver -> gold hop."""
    from pyspark.sql import functions as F

    silver = spark.read.parquet(str(_require(DATA / "silver" / "traffic_events", HINT)))
    gold = spark.read.parquet(str(_require(DATA / "gold" / "traffic_events", HINT)))

    dates = [r["event_date"] for r in gold.select("event_date").distinct().collect()]
    s_n = silver.filter(F.col("event_date").isin(dates)).count()
    g_n = gold.count()
    assert s_n == g_n, (
        f"silver {s_n:,} != gold {g_n:,} for the same dates — the enrichment join "
        f"either dropped rows (bad join key) or duplicated them (fan-out)"
    )


def test_hourly_aggregate_never_exceeds_its_own_detail(spark):
    """Sanity on the segment x hour rollup: counts cannot exceed the detail."""
    from pyspark.sql import functions as F

    gold_events = spark.read.parquet(str(_require(DATA / "gold" / "traffic_events", HINT)))
    hourly = spark.read.parquet(str(_require(DATA / "gold" / "hourly_traffic", HINT)))

    detail = gold_events.filter(F.col("segment_code").isNotNull()).count()
    aggregated = hourly.agg(F.sum("vehicle_count")).collect()[0][0] or 0
    assert aggregated <= detail, (
        f"hourly vehicle_count sums to {aggregated:,} but only {detail:,} detections "
        f"with a segment exist — the aggregation is double-counting"
    )
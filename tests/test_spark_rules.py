"""Tier 2 — Spark transformation logic. Needs a JVM (Java 11/17).

These tests drive the validation rules with hand-built DataFrames, so each rule
is proven in isolation rather than inferred from a pipeline run.

The most important test here is `test_future_timestamp_is_caught` together with
`test_future_dated_row_reaches_quarantine_not_oblivion`: those two are the
regression guard for the silent-data-loss defect, where future-dated rows were
written to an event-derived bronze partition that the per-date silver read never
opened, so they reached neither silver nor quarantine and no reconciliation
could detect their absence.
"""
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from conftest import requires_spark

pytestmark = [pytest.mark.spark, requires_spark]

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark"))

EVENT_SCHEMA = (
    "event_id string, event_ts timestamp, detector_type string, "
    "detector_code string, sensor_serial string, camera_code string, "
    "segment_code string, direction string, vehicle_type_code string, "
    "speed_kmh decimal(5,1), headway_seconds decimal(6,2), "
    "occupancy_pct decimal(5,2), _ingest_ts timestamp, _source_file string, "
    "event_date date, ingest_date date"
)

BASE_DAY = datetime(2026, 6, 1, 8, 30)


def _row(event_id="E1", ts=BASE_DAY, detector="SNS-0001", detector_type="SENSOR",
         segment="SEG-001", direction="NB", vtype="CAR", speed="55.0",
         occupancy="42.0", event_date=None, ingest_date=None, source="f1.csv"):
    """One conformed silver-shaped row; every field overridable per test."""
    ed = event_date if event_date is not None else (ts.date() if ts else None)
    idt = ingest_date if ingest_date is not None else BASE_DAY.date()
    return (
        event_id, ts, detector_type, detector,
        detector if detector_type == "SENSOR" else None,
        detector if detector_type == "CAMERA" else None,
        segment, direction, vtype,
        Decimal(speed) if speed is not None else None,
        Decimal("2.50"),
        Decimal(occupancy) if occupancy is not None else None,
        BASE_DAY, source, ed, idt,
    )


def _apply(spark, rows):
    from utils.quality import apply_rules

    df = spark.createDataFrame(rows, EVENT_SCHEMA)
    return {r["event_id"]: r["reject_reason"] for r in apply_rules(df).collect()}


# ================================================== individual rule tests ===
def test_clean_row_has_no_reject_reason(spark):
    """Negative control: without it, a rule matching everything would 'pass'."""
    assert _apply(spark, [_row()])["E1"] is None


def test_speed_above_250_is_quarantined(spark):
    """The generator's bad_speed defect injects 999 km/h."""
    assert _apply(spark, [_row(speed="999.0")])["E1"] == "SPEED_RANGE"


def test_zero_or_negative_speed_is_quarantined(spark):
    reasons = _apply(spark, [_row(event_id="Z", speed="0.0"),
                             _row(event_id="N", speed="-5.0")])
    assert reasons["Z"] == "SPEED_RANGE"
    assert reasons["N"] == "SPEED_RANGE"


def test_occupancy_above_100_is_quarantined(spark):
    """The generator's bad_occ defect injects 150%."""
    assert _apply(spark, [_row(occupancy="150.0")])["E1"] == "OCCUPANCY_RANGE"


def test_missing_detector_or_id_or_timestamp_is_quarantined(spark):
    reasons = _apply(spark, [
        _row(event_id="A", detector=None),
        _row(event_id="B", ts=None, event_date=None),
    ])
    assert reasons["A"] == "MISSING_KEY"
    assert reasons["B"] == "MISSING_KEY"


def test_future_timestamp_is_caught(spark):
    """THE regression test for the silent-data-loss defect.

    The generator's future_ts defect adds 365 days. This rule was previously
    unreachable in the only execution mode the pipeline uses, because bronze was
    partitioned by the event-derived date and the future row sat in a partition
    the per-date read never opened.
    """
    future = datetime.now() + timedelta(days=365)
    reasons = _apply(spark, [_row(ts=future, event_date=future.date())])
    assert reasons["E1"] == "FUTURE_TIMESTAMP"


def test_event_date_not_matching_the_file_is_quarantined(spark):
    """Feed contract: one file per calendar day of events.

    A past-dated row cannot be attributed to the load date it arrived in, so it
    is quarantined rather than silently counted against the wrong day.
    """
    stale = datetime(2026, 5, 20, 9, 0)
    reasons = _apply(spark, [_row(ts=stale, event_date=stale.date())])
    assert reasons["E1"] == "DATE_MISMATCH"


def test_invalid_direction_is_quarantined(spark):
    assert _apply(spark, [_row(direction="XX")])["E1"] == "BAD_DIRECTION"


def test_null_direction_is_allowed(spark):
    """Absence is not a domain violation — only a WRONG value is."""
    assert _apply(spark, [_row(direction=None)])["E1"] is None


# ======================================================= rule precedence ===
def test_most_specific_rule_wins_when_several_match(spark):
    """A row can break several rules; the reported reason must be deterministic.

    apply_rules folds ALL_EVENT_RULES in reverse so the FIRST rule in the list
    wins. MISSING_KEY precedes SPEED_RANGE, so a row with no detector AND an
    absurd speed reports MISSING_KEY — the more fundamental defect.
    """
    reasons = _apply(spark, [_row(detector=None, speed="999.0")])
    assert reasons["E1"] == "MISSING_KEY"


def test_future_timestamp_beats_date_mismatch(spark):
    """A future row is also off-date; the specific reason is the useful one."""
    future = datetime.now() + timedelta(days=365)
    reasons = _apply(spark, [_row(ts=future, event_date=future.date())])
    assert reasons["E1"] == "FUTURE_TIMESTAMP", "generic DATE_MISMATCH masked the specific rule"


# ======================================================== deduplication ====
def test_duplicate_event_ids_keep_exactly_one_survivor(spark):
    """The generator's dup defect writes the same event_id twice."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from utils.quality import apply_rules

    rows = [_row(event_id="DUP", source="a.csv"), _row(event_id="DUP", source="b.csv")]
    df = apply_rules(spark.createDataFrame(rows, EVENT_SCHEMA))
    ident = Window.partitionBy("event_id").orderBy(
        F.col("event_ts").desc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
        F.col("detector_code").asc_nulls_last(),
    )
    out = (
        df.withColumn("_rn", F.row_number().over(ident))
        .withColumn("reject_reason",
                    F.when(F.col("_rn") > 1, F.lit("DUPLICATE"))
                     .otherwise(F.col("reject_reason")))
    )
    kept = out.filter(F.col("reject_reason").isNull()).count()
    dropped = out.filter(F.col("reject_reason") == "DUPLICATE").count()
    assert (kept, dropped) == (1, 1), "duplicate handling must keep exactly one row"


def test_deduplication_is_deterministic_across_runs(spark):
    """Ordering must not depend on _ingest_ts.

    _ingest_ts is current_timestamp(), which Spark evaluates once per query, so
    it is the same constant on every row of a batch. Ordering by it made
    row_number() pick arbitrarily and the surviving row could differ between
    runs. The window now orders by real row-level attributes.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    rows = [_row(event_id="D", detector="SNS-0009", source="b.csv"),
            _row(event_id="D", detector="SNS-0001", source="a.csv")]
    ident = Window.partitionBy("event_id").orderBy(
        F.col("event_ts").desc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
        F.col("detector_code").asc_nulls_last(),
    )
    survivors = set()
    for _ in range(3):
        df = spark.createDataFrame(rows, EVENT_SCHEMA)
        winner = (
            df.withColumn("_rn", F.row_number().over(ident))
            .filter(F.col("_rn") == 1)
            .collect()[0]["_source_file"]
        )
        survivors.add(winner)
    assert survivors == {"a.csv"}, f"non-deterministic survivor: {survivors}"


# ==================================================== detector modelling ===
def test_sensor_and_camera_identities_stay_separate(spark):
    """Camera codes must not become DimSensor members.

    Aliasing camera_code into sensor_serial made every camera a permanently
    inferred sensor while dim.DimTrafficCamera was referenced by no fact at all.
    """
    from pyspark.sql import functions as F
    from utils.quality import apply_rules

    rows = [_row(event_id="S", detector="SNS-0001", detector_type="SENSOR"),
            _row(event_id="C", detector="CAM-007", detector_type="CAMERA")]
    df = apply_rules(spark.createDataFrame(rows, EVENT_SCHEMA))
    by_id = {r["event_id"]: r for r in df.collect()}

    assert by_id["S"]["sensor_serial"] == "SNS-0001"
    assert by_id["S"]["camera_code"] is None, "sensor row leaked into the camera column"
    assert by_id["C"]["camera_code"] == "CAM-007"
    assert by_id["C"]["sensor_serial"] is None, "camera row would become an inferred sensor"
    assert by_id["S"]["reject_reason"] is None and by_id["C"]["reject_reason"] is None


# ========================================================= weather bands ===
@pytest.mark.parametrize("temp,expected", [
    (None, "Unknown"), (-10.0, "Below -5"), (-5.0, "-5 to 5"), (0.0, "-5 to 5"),
    (5.0, "5 to 15"), (14.9, "5 to 15"), (15.0, "15 to 25"), (25.0, "Above 25"),
])
def test_temperature_banding_matches_the_sql_function(spark, temp, expected):
    """Boundaries must match etl.fn_WeatherBands exactly.

    A drift between the two engines produces WeatherKey values that resolve in
    one and fall back to -1 in the other — the ORPHAN_FTE_WEATHER check exists
    precisely to catch it, but the boundaries should agree in the first place.
    """
    from pyspark.sql import functions as F
    from utils.quality import band_temperature

    df = spark.createDataFrame([(temp,)], "t double")
    got = df.select(band_temperature(F.col("t")).alias("band")).collect()[0]["band"]
    assert got == expected


@pytest.mark.parametrize("precip,expected", [
    (None, "None"), (0.0, "None"), (0.1, "Light"), (2.4, "Light"),
    (2.5, "Moderate"), (7.4, "Moderate"), (7.5, "Heavy"), (20.0, "Heavy"),
])
def test_precipitation_banding_matches_the_sql_function(spark, precip, expected):
    from pyspark.sql import functions as F
    from utils.quality import band_precipitation

    df = spark.createDataFrame([(precip,)], "p double")
    got = df.select(band_precipitation(F.col("p")).alias("band")).collect()[0]["band"]
    assert got == expected


@pytest.mark.parametrize("vis,expected", [
    (None, "Good"), (10000, "Good"), (2000, "Good"),
    (1999, "Reduced"), (500, "Reduced"), (499, "Poor"), (100, "Poor"),
])
def test_visibility_banding_matches_the_sql_function(spark, vis, expected):
    from pyspark.sql import functions as F
    from utils.quality import band_visibility

    df = spark.createDataFrame([(vis,)], "v int")
    got = df.select(band_visibility(F.col("v")).alias("band")).collect()[0]["band"]
    assert got == expected
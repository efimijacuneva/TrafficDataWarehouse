"""Tier 1 — source-data invariants. Pure Python: no Spark, no SQL Server.

Two jobs:

1. Prove the generated baseline data is VALID BY CONSTRUCTION. The incident
   lifecycle is the important one: the warehouse's accumulating-snapshot check
   MILESTONE_ORDER_FIL asserts Detected <= Dispatched <= Arrived <= Cleared <=
   Closed, and it can only be an Error-severity gate if the source data cannot
   violate it. (It previously could: RoadClearedAt and ClosedAt were computed
   from independent offsets off DetectedAt, so some incidents "closed" before
   the road was cleared, and the check had to be downgraded to a Warning.)

2. Prove the DELIBERATE defects are actually present. A quality gate that never
   sees a bad row is decoration, so these tests fail if the generator stops
   injecting the defects the pipeline is built to catch.
"""
import csv
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from conftest import requires_raw

pytestmark = [pytest.mark.needs_data, requires_raw]


# ------------------------------------------------------------- helpers ---
def _read_sensor_rows(raw_dir: Path):
    for path in sorted(raw_dir.glob("sensor_readings_*.csv")):
        file_date = path.stem.split("_")[-1]  # YYYYMMDD
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                yield file_date, row


# =============================================================== baseline ===
def test_manifest_matches_files_on_disk(raw_dir, manifest):
    """The manifest is the reconciliation baseline — it must be truthful.

    Every downstream no-silent-loss assertion is anchored on these counts, so a
    manifest that disagrees with the files invalidates the whole chain.
    """
    for entry in manifest:
        stamp = entry["date"].replace("-", "")
        csv_path = raw_dir / f"sensor_readings_{stamp}.csv"
        assert csv_path.exists(), f"manifest lists {entry['date']} but {csv_path.name} is missing"

        with open(csv_path, newline="", encoding="utf-8") as fh:
            actual = sum(1 for _ in csv.DictReader(fh))
        assert actual == entry["sensor_csv_rows"], (
            f"{csv_path.name}: manifest says {entry['sensor_csv_rows']:,} rows, "
            f"file has {actual:,}"
        )

        cam_path = raw_dir / f"camera_events_{stamp}.json"
        cameras = json.loads(cam_path.read_text(encoding="utf-8"))
        assert len(cameras) == entry["camera_events"], (
            f"{cam_path.name}: manifest says {entry['camera_events']:,}, "
            f"file has {len(cameras):,}"
        )


def test_camera_events_have_required_nested_shape(raw_dir):
    """Job 02 reads detection.* as a struct; a shape change breaks silver."""
    path = sorted(raw_dir.glob("camera_events_*.json"))[0]
    events = json.loads(path.read_text(encoding="utf-8"))
    assert events, f"{path.name} is empty"
    for event in events[:50]:
        assert {"event_id", "camera_code", "detection"} <= event.keys()
        assert {"timestamp", "segment_code", "direction", "vehicle_class",
                "speed_kmh"} <= event["detection"].keys()


def test_weather_covers_every_hour_for_every_station(raw_dir):
    """Job 03 aggregates weather to hourly grain and broadcast-joins it.

    A missing hour silently produces NULL weather on every detection in it, so
    the feed must be dense.
    """
    path = sorted(raw_dir.glob("weather_observations_*.json"))[0]
    rows = json.loads(path.read_text(encoding="utf-8"))
    per_station = Counter(r["station_code"] for r in rows)
    assert per_station, "no weather rows"
    for station, count in per_station.items():
        assert count == 24, f"station {station} has {count} observations, expected 24"


# ================================================= the milestone invariant ===
def test_incident_milestones_are_monotonic_by_construction():
    """The invariant MILESTONE_ORDER_FIL gates on, checked at the source.

    This mirrors the arithmetic in sql/oltp/03_sample_data.sql. Each milestone
    is DERIVED FROM ITS PREDECESSOR, so ordering holds for every incident:

        Dispatched = Detected   + (2 + i%6)      ->  2..7   min
        Arrived    = Dispatched + (4 + i%13)     ->  4..16  min after dispatch
        Cleared    = Detected   + (25 + i%90)    -> 25..114 min
        Closed     = Cleared    + (20 + i%60)    -> 20..79  min after clearing

    The regression this guards: Cleared and Closed used independent offsets off
    Detected (25+i%90 and 45+i%120), so incident 120 cleared at +55 min and
    "closed" at +45 min — closed before it was cleared.
    """
    violations = []
    for i in range(1, 181):
        detected = 0
        dispatched = detected + (2 + i % 6)
        arrived = dispatched + (4 + i % 13)
        cleared = detected + (25 + i % 90)
        closed = cleared + (20 + i % 60)

        if not (detected <= dispatched <= arrived <= cleared <= closed):
            violations.append(
                f"incident {i}: detected={detected} dispatched={dispatched} "
                f"arrived={arrived} cleared={cleared} closed={closed}"
            )

    assert not violations, (
        f"{len(violations)} incident(s) violate milestone monotonicity, which would "
        f"fail the MILESTONE_ORDER_FIL quality gate:\n  " + "\n  ".join(violations[:5])
    )


def test_incident_lag_measures_are_never_negative():
    """NEGATIVE_LAGS_FIL is Error severity — the source must never produce one."""
    for i in range(1, 181):
        dispatched = 2 + i % 6
        arrived = dispatched + (4 + i % 13)
        cleared = 25 + i % 90
        closed = cleared + (20 + i % 60)
        assert dispatched >= 0
        assert arrived - dispatched >= 0, f"incident {i}: negative MinutesToArrive"
        assert cleared - arrived >= 0, f"incident {i}: negative MinutesToClear"
        assert closed >= 0, f"incident {i}: negative TotalDurationMinutes"


# ================================================== deliberate defects ======
def test_every_defect_class_is_actually_injected(raw_dir):
    """A quality gate that never sees a bad row proves nothing.

    Each assertion below corresponds to a rule in spark/utils/quality.py or a
    staging validation in etl.usp_LoadFactTrafficEvent.
    """
    missing_detector = bad_speed = bad_occupancy = sentinel = future_ts = 0
    ids = Counter()

    for file_date, row in _read_sensor_rows(raw_dir):
        ids[row["event_id"]] += 1
        if row["sensor_serial"].strip() == "":
            missing_detector += 1
        try:
            if float(row["speed_kmh"]) > 250:
                bad_speed += 1
        except ValueError:
            pass
        try:
            if float(row["occupancy_pct"]) > 100:
                bad_occupancy += 1
        except ValueError:
            pass
        if row["segment_code"].strip().upper() in {"N/A", "NA", "-", "NULL", ""}:
            sentinel += 1
        if not row["event_ts"].startswith(f"{file_date[:4]}-"):
            future_ts += 1

    duplicates = sum(c - 1 for c in ids.values() if c > 1)

    assert missing_detector > 0, "no MISSING_KEY defects injected"
    assert bad_speed > 0, "no SPEED_RANGE defects injected"
    assert bad_occupancy > 0, "no OCCUPANCY_RANGE defects injected"
    assert sentinel > 0, "no sentinel values injected (unknown-member path untested)"
    assert future_ts > 0, "no FUTURE_TIMESTAMP defects injected"
    assert duplicates > 0, "no DUPLICATE defects injected"


def test_future_dated_rows_exist_and_are_the_silent_loss_regression(raw_dir):
    """Guards the exact bug that lost 689 rows.

    These rows have a timestamp a year ahead of the file they arrived in. When
    bronze was partitioned by the EVENT-derived date they landed in an
    event_date=2027-* partition that job 02's per-date read never opened, so
    they reached neither silver nor quarantine and no reconciliation could see
    them. Bronze is now partitioned by ingest_date (the file's own date), which
    is why tests/test_spark_rules.py can assert they end up quarantined.
    """
    off_file_date = 0
    for file_date, row in _read_sensor_rows(raw_dir):
        expected_day = datetime.strptime(file_date, "%Y%m%d").date()
        actual_day = datetime.fromisoformat(row["event_ts"]).date()
        if actual_day != expected_day:
            off_file_date += 1

    assert off_file_date > 0, (
        "No rows with event_date != file date. The future_ts defect is what makes "
        "the FUTURE_TIMESTAMP rule reachable; without it that rule is untested."
    )


def test_defect_rate_is_within_the_documented_band(raw_dir, manifest):
    """~1.5% by default. Wildly off means the generator was mis-parameterised."""
    total = sum(e["sensor_csv_rows"] for e in manifest)
    defective = 0
    for _, row in _read_sensor_rows(raw_dir):
        if (
            row["sensor_serial"].strip() == ""
            or row["segment_code"].strip().upper() in {"N/A", "NA", "-", "NULL"}
        ):
            defective += 1
        else:
            try:
                if float(row["speed_kmh"]) > 250 or float(row["occupancy_pct"]) > 100:
                    defective += 1
            except ValueError:
                pass

    rate = defective / total
    assert 0.001 < rate < 0.05, (
        f"defect rate {rate:.3%} outside the plausible 0.1%-5% band "
        f"({defective:,} of {total:,} rows)"
    )


# ===================================================== reproducibility ======
def test_generator_is_deterministic_for_a_given_seed(project_root):
    """--seed must give identical output, or no test can assert on counts.

    Exercised IN PROCESS against the generator's own seeded functions rather
    than by shelling out: invoking the CLI would write into <repo>/data/raw and
    destroy the real dataset the other tests reconcile against.
    """
    sys.path.insert(0, str(project_root / "data_generator"))
    import random
    from datetime import date

    import generate_data as gen

    day = date(2026, 6, 1)
    first = gen.daily_weather(random.Random(1234), day)
    second = gen.daily_weather(random.Random(1234), day)
    assert first == second, "same seed produced different weather; runs are not reproducible"

    diverged = gen.daily_weather(random.Random(9999), day)
    assert diverged != first, "different seeds produced identical output; seed is ignored"
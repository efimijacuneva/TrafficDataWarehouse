"""Data access for the dashboard — reads REAL project output, never demo data.

Two sources, each optional and each degrading honestly:

  * the Parquet lake  (data/bronze, data/silver, data/gold) via pyarrow — no
    Spark and no JVM needed, because the dashboard only READS finished files;
  * the warehouse     (TrafficDW) via pyodbc — optional.

Every accessor returns `(dataframe, status)` where status is one of
'ok' | 'missing' | 'error'. A page that cannot get its data says so on screen
instead of rendering a plausible-looking empty chart, which is the same
"never claim something you did not verify" rule the test suite follows.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA = PROJECT_ROOT / "data"
RAW, BRONZE, SILVER, GOLD = DATA / "raw", DATA / "bronze", DATA / "silver", DATA / "gold"
QUARANTINE = SILVER / "_quarantine"

Result = Tuple[pd.DataFrame, str]
EMPTY: Result = (pd.DataFrame(), "missing")


def weighted_by(df, group, measure, weight):
    """Collapse a finer-grained result to `group`, weighting `measure` by `weight`.

    The hourly snapshot's speed and congestion measures are ALREADY averages
    over the vehicles seen in that segment-hour. Rolling them up further with a
    plain mean - or worse, letting a chart library stack them - is wrong: it
    weights a near-empty band the same as the dominant one. The correct rollup
    is SUM(measure * weight) / SUM(weight), which is exactly what the mart views
    do in SQL (sql/warehouse/06_mart_views.sql). This is the same rule applied
    on the presentation side when a chart's grain is coarser than the view's.
    """
    g = df.copy()
    g["_num"] = g[measure] * g[weight]
    out = g.groupby(group, as_index=False).agg(_num=("_num", "sum"),
                                               _den=(weight, "sum"))
    out[measure] = (out["_num"] / out["_den"]).round(1)
    out[weight] = out["_den"]
    return out.drop(columns=["_num", "_den"]).sort_values(measure, ascending=False)


# ------------------------------------------------------------------ lake ---
def read_parquet_dir(path: Path) -> Result:
    """Read a partitioned Parquet dataset. Partition columns come back as columns."""
    if not path.exists():
        return EMPTY
    try:
        import pyarrow.parquet as pq

        table = pq.read_table(str(path))
        if table.num_rows == 0:
            return pd.DataFrame(), "missing"
        return table.to_pandas(), "ok"
    except ImportError:
        return pd.DataFrame(), "error:pyarrow not installed (pip install -r requirements-dev.txt)"
    except Exception as exc:  # unreadable/partial write — say so, do not fake it
        return pd.DataFrame(), f"error:{exc}"


def lake_zone_summary() -> pd.DataFrame:
    """Row counts and on-disk size per lake zone — powers the pipeline page."""
    zones = [
        ("Raw",        RAW,                              "*.csv"),
        ("Bronze",     BRONZE / "sensor_readings",       "*.parquet"),
        ("Silver",     SILVER / "traffic_events",        "*.parquet"),
        ("Quarantine", QUARANTINE / "traffic_events",    "*.parquet"),
        ("Gold",       GOLD / "traffic_events",          "*.parquet"),
    ]
    rows = []
    for name, path, pattern in zones:
        if not path.exists():
            rows.append({"Zone": name, "Exists": False, "Partitions": 0,
                         "Files": 0, "SizeMB": 0.0})
            continue
        files = list(path.rglob(pattern))
        parts = [d for d in path.iterdir() if d.is_dir()] if path.is_dir() else []
        rows.append({
            "Zone": name,
            "Exists": True,
            "Partitions": len(parts),
            "Files": len(files),
            "SizeMB": round(sum(f.stat().st_size for f in files) / 1_048_576, 2),
        })
    return pd.DataFrame(rows)


def raw_manifest() -> Result:
    """The generator's own row counts — the reconciliation baseline."""
    path = RAW / "_manifest.json"
    if not path.exists():
        return EMPTY
    try:
        return pd.read_json(path), "ok"
    except Exception as exc:
        return pd.DataFrame(), f"error:{exc}"


def reconciliation() -> Result:
    """silver/reconciliation: rows_in == rows_good + rows_quarantined, per date."""
    return read_parquet_dir(SILVER / "reconciliation")


def quarantine() -> Result:
    return read_parquet_dir(QUARANTINE / "traffic_events")


def hourly_traffic() -> Result:
    return read_parquet_dir(GOLD / "hourly_traffic")


def kpi(name: str) -> Result:
    return read_parquet_dir(GOLD / name)


# ------------------------------------------------------------- warehouse ---
def _connection_string() -> str:
    host = os.getenv("SQL_SERVER_HOST", "localhost")
    port = os.getenv("SQL_SERVER_PORT", "1433")
    pwd = os.getenv("SQL_PASSWORD", "ChangeMe_Demo1!")
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host},{port};DATABASE=TrafficDW;UID=sa;PWD={pwd};"
        "TrustServerCertificate=yes;Encrypt=yes;Connection Timeout=5"
    )


def warehouse_available() -> tuple[bool, str]:
    try:
        import pyodbc  # noqa: F401
    except ImportError:
        return False, "pyodbc not installed (pip install -r requirements-dev.txt)"
    try:
        import pyodbc

        with pyodbc.connect(_connection_string(), timeout=5):
            return True, "connected"
    except Exception as exc:
        return False, f"SQL Server unreachable: {str(exc)[:120]}"


def query(sql: str) -> Result:
    """Run a read-only query against TrafficDW."""
    try:
        import pyodbc
    except ImportError:
        return pd.DataFrame(), "error:pyodbc not installed"
    try:
        with pyodbc.connect(_connection_string(), timeout=5) as conn:
            df = pd.read_sql(sql, conn)
        return (df, "ok") if not df.empty else (df, "missing")
    except Exception as exc:
        return pd.DataFrame(), f"error:{str(exc)[:200]}"


# ------------------------------------------------------- warehouse queries --
SQL_FACT_GRAINS = """
SELECT 'FactTrafficEvent'      AS FactTable, 'Transaction'          AS FactType,
       'one vehicle detected by one sensor or camera' AS Grain,
       COUNT_BIG(*)            AS Rows FROM fact.FactTrafficEvent
UNION ALL
SELECT 'FactHourlyTraffic', 'Periodic snapshot',
       'one road segment per clock hour (incl. zero-traffic hours)',
       COUNT_BIG(*) FROM fact.FactHourlyTraffic
UNION ALL
SELECT 'FactIncidentLifecycle', 'Accumulating snapshot',
       'one incident, updated as milestones occur',
       COUNT_BIG(*) FROM fact.FactIncidentLifecycle
"""

SQL_SCD_HISTORY = """
SELECT SegmentCode, RoadSegmentKey, VersionNumber, SpeedLimitKmh,
       OriginalSpeedLimitKmh, CurrentSpeedLimitKmh,
       EffectiveDate, ExpirationDate, IsCurrent, IsInferred
FROM dim.DimRoadSegment
WHERE SegmentCode = ?
ORDER BY VersionNumber
"""

SQL_SCD_VERSIONED = """
SELECT TOP (25) SegmentCode, COUNT(*) AS Versions
FROM dim.DimRoadSegment
WHERE RoadSegmentKey <> -1
GROUP BY SegmentCode
ORDER BY COUNT(*) DESC, SegmentCode
"""

SQL_QUALITY_LATEST = """
WITH latest AS (
    SELECT CheckName, Category, Severity, Status, FailedRows, DurationMs, CheckedAt,
           ROW_NUMBER() OVER (PARTITION BY CheckName
                              ORDER BY CheckedAt DESC, QualityCheckLogID DESC) AS rn
    FROM etl.QualityCheckLog
)
SELECT CheckName, Category, Severity, Status, FailedRows, DurationMs, CheckedAt
FROM latest WHERE rn = 1
ORDER BY CASE WHEN Status='Fail' AND Severity='Error' THEN 0
              WHEN Status='Fail' THEN 1 ELSE 2 END, Category, CheckName
"""

SQL_BATCHES = """
SELECT b.ETLBatchID, b.LoadDate, b.Status,
       DATEDIFF(SECOND, b.StartedAt, ISNULL(b.FinishedAt, b.StartedAt)) AS DurationSec,
       ISNULL(SUM(r.RowsExtracted),0) AS RowsExtracted,
       ISNULL(SUM(r.RowsInserted),0)  AS RowsInserted,
       ISNULL(SUM(r.RowsRejected),0)  AS RowsRejected
FROM etl.BatchLog b LEFT JOIN etl.RowLog r ON r.ETLBatchID = b.ETLBatchID
GROUP BY b.ETLBatchID, b.LoadDate, b.Status, b.StartedAt, b.FinishedAt
ORDER BY b.ETLBatchID
"""

SQL_REJECTS_BY_REASON = """
SELECT RejectReason, COUNT_BIG(*) AS RejectedRows
FROM stg.RejectTrafficEvent GROUP BY RejectReason ORDER BY RejectedRows DESC
"""

SQL_KPI_DAILY = "SELECT * FROM mart.vPbiKpiDaily ORDER BY DateKey"
SQL_TOP_ROADS = "SELECT * FROM mart.vTopBusiestRoads ORDER BY TotalVehicles DESC"
SQL_BY_WEATHER = "SELECT * FROM mart.vTrafficByWeather ORDER BY TotalVehicles DESC"
SQL_BY_WEEKDAY = "SELECT * FROM mart.vTrafficByWeekday ORDER BY DayOfWeek"
SQL_RUSH_HOUR = "SELECT * FROM mart.vRushHourProfile ORDER BY HourOfDay"
SQL_DETECTOR_MIX = """
SELECT DetectorType, COUNT_BIG(*) AS Detections
FROM fact.FactTrafficEvent GROUP BY DetectorType
"""
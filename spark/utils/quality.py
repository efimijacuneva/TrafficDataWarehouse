"""Data-quality rules shared by the silver gate (job 02).

Each rule returns (rule_name, boolean Column) where True = row FAILS the rule.
Rules mirror the SQL-side checks (etl.fn_WeatherBands, the staging validations
in etl.usp_LoadFactTrafficEvent) so the two engines classify records
identically — one definition of 'valid'.

RULE ORDER IS SIGNIFICANT: `apply_rules` folds the list in reverse, building a
nested when/otherwise chain, so the FIRST matching rule in ALL_EVENT_RULES wins
and every row carries exactly one reject_reason. Order therefore runs from most
specific to most general — FUTURE_TIMESTAMP before DATE_MISMATCH, because a
future timestamp is also an off-date timestamp but the specific reason is the
useful one.
"""
from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MAX_SPEED_KMH = 250.0
VALID_DIRECTIONS = ("NB", "SB", "EB", "WB")


def rule_missing_keys(df: DataFrame) -> Tuple[str, Column]:
    """No business identity, no timestamp, or no detector → unusable."""
    return (
        "MISSING_KEY",
        F.col("event_id").isNull()
        | F.col("detector_code").isNull()
        | F.col("event_ts").isNull(),
    )


def rule_speed_range(df: DataFrame) -> Tuple[str, Column]:
    return (
        "SPEED_RANGE",
        F.col("speed_kmh").isNotNull()
        & ((F.col("speed_kmh") <= 0) | (F.col("speed_kmh") > MAX_SPEED_KMH)),
    )


def rule_occupancy_range(df: DataFrame) -> Tuple[str, Column]:
    return (
        "OCCUPANCY_RANGE",
        F.col("occupancy_pct").isNotNull()
        & ((F.col("occupancy_pct") < 0) | (F.col("occupancy_pct") > 100)),
    )


def rule_future_timestamp(df: DataFrame) -> Tuple[str, Column]:
    """Events cannot have happened after now.

    Reachable ONLY because bronze is partitioned by ingest_date (job 01): if
    bronze were partitioned by the event-derived date, a future-dated row would
    sit in a future partition that the per-date silver read never opens, and
    this rule would be dead code.
    """
    return ("FUTURE_TIMESTAMP", F.col("event_ts") > F.current_timestamp())


def rule_date_mismatch(df: DataFrame) -> Tuple[str, Column]:
    """The event date must match the file the row arrived in.

    The feed contract is one file per calendar day of events. A row whose
    event_date differs from its ingest_date is either clock-skewed or
    mis-filed; either way it cannot be attributed to a load date, so it is
    quarantined rather than silently attributed to the wrong day. Mirrors the
    DATE_MISMATCH reason in etl.usp_LoadFactTrafficEvent.
    """
    return (
        "DATE_MISMATCH",
        F.col("ingest_date").isNotNull()
        & F.col("event_date").isNotNull()
        & (F.col("event_date") != F.col("ingest_date")),
    )


def rule_bad_direction(df: DataFrame) -> Tuple[str, Column]:
    return (
        "BAD_DIRECTION",
        F.col("direction").isNotNull()
        & ~F.col("direction").isin(*VALID_DIRECTIONS),
    )


# most specific → most general (see module docstring)
ALL_EVENT_RULES = [
    rule_missing_keys,
    rule_speed_range,
    rule_occupancy_range,
    rule_future_timestamp,
    rule_date_mismatch,
    rule_bad_direction,
]


def apply_rules(df: DataFrame, rules=None) -> DataFrame:
    """Stamp each row with reject_reason (first failed rule) or NULL if clean."""
    rules = rules or ALL_EVENT_RULES
    reason = F.lit(None).cast("string")
    # build in reverse so the FIRST rule in the list wins
    for rule in reversed(rules):
        name, failed = rule(df)
        reason = F.when(failed, F.lit(name)).otherwise(reason)
    return df.withColumn("reject_reason", reason)


def band_temperature(col: Column) -> Column:
    """Mirror of etl.fn_WeatherBands TempBand."""
    return (
        F.when(col.isNull(), "Unknown")
        .when(col < -5, "Below -5")
        .when(col < 5, "-5 to 5")
        .when(col < 15, "5 to 15")
        .when(col < 25, "15 to 25")
        .otherwise("Above 25")
    )


def band_precipitation(col: Column) -> Column:
    return (
        F.when(col.isNull() | (col == 0), "None")
        .when(col < 2.5, "Light")
        .when(col < 7.5, "Moderate")
        .otherwise("Heavy")
    )


def band_visibility(col: Column) -> Column:
    return (
        F.when(col.isNull() | (col >= 2000), "Good")
        .when(col >= 500, "Reduced")
        .otherwise("Poor")
    )

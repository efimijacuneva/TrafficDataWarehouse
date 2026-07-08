"""Data-quality rules shared by the silver gate (job 02).

Each rule returns (rule_name, boolean Column) where True = row FAILS the rule.
Rules mirror the SQL-side checks (etl.fn_WeatherBands, staging validations) so
the two engines classify records identically — one definition of 'valid'.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MAX_SPEED_KMH = 250.0


def rule_missing_keys(df: DataFrame) -> tuple[str, Column]:
    return (
        "MISSING_KEY",
        F.col("event_id").isNull()
        | F.col("sensor_serial").isNull()
        | F.col("event_ts").isNull(),
    )


def rule_speed_range(df: DataFrame) -> tuple[str, Column]:
    return (
        "SPEED_RANGE",
        F.col("speed_kmh").isNotNull()
        & ((F.col("speed_kmh") <= 0) | (F.col("speed_kmh") > MAX_SPEED_KMH)),
    )


def rule_occupancy_range(df: DataFrame) -> tuple[str, Column]:
    return (
        "OCCUPANCY_RANGE",
        F.col("occupancy_pct").isNotNull()
        & ((F.col("occupancy_pct") < 0) | (F.col("occupancy_pct") > 100)),
    )


def rule_future_timestamp(df: DataFrame) -> tuple[str, Column]:
    return ("FUTURE_TIMESTAMP", F.col("event_ts") > F.current_timestamp())


def rule_bad_direction(df: DataFrame) -> tuple[str, Column]:
    return (
        "BAD_DIRECTION",
        F.col("direction").isNotNull()
        & ~F.col("direction").isin("NB", "SB", "EB", "WB"),
    )


ALL_EVENT_RULES = [
    rule_missing_keys,
    rule_speed_range,
    rule_occupancy_range,
    rule_future_timestamp,
    rule_bad_direction,
]


def apply_rules(df: DataFrame, rules=None) -> DataFrame:
    """Stamp each row with reject_reason (first failed rule) or NULL if clean."""
    rules = rules or ALL_EVENT_RULES
    reason = F.lit(None).cast("string")
    # build reverse so the FIRST rule in the list wins
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

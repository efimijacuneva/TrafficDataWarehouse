# Part 6 — Slowly Changing Dimensions (SCD) Strategy

> Implementations: [sql/etl/03_load_dimensions.sql](../sql/etl/03_load_dimensions.sql)

## Core Vocabulary

| Concept | Definition | In this project |
|---|---|---|
| **Business key (BK)** | Immutable identifier from the source system | `Sensor.SerialNumber`, `RoadSegment.SegmentCode`, `TrafficCamera.CameraCode` |
| **Surrogate key (SK)** | Warehouse-generated `INT IDENTITY`; what facts reference | `SensorKey`, `RoadSegmentKey`, … |
| **Current flag** | `IsCurrent BIT` — marks the active version of a BK | filter `IsCurrent = 1` for "as-is" reporting |
| **Effective / Expiration date** | `EffectiveDate` / `ExpirationDate` validity interval; current row expires at `9999-12-31` | enables "as-was" point-in-time joins |
| **Version number** | `VersionNumber INT` incremented per change | human-friendly audit ("3rd revision of segment 118") |
| **Hash diff** | `HashDiff BINARY(32)` = SHA2 over Type-2 attributes | change detection without comparing 10 columns |

Why surrogate keys (not BKs) on facts: BKs can be reused/recycled by sources; SKs allow
multiple historical versions of one BK; joins on 4-byte ints beat joins on varchar serials;
and the reserved members (−1 Unknown, inferred members) need keys that no source can collide with.

## Types Implemented in the Warehouse

### Type 0 — Retain original (`DimDate`, and attribute-level policy)
Attributes that must never change: the calendar, `DimSensor.InstallDate`,
`DimSensor.SerialNumber`. The ETL simply never updates them — enforced by omitting them
from every `UPDATE` clause and documented per column. *Use case:* "original speed limit
when segment was commissioned" (`OriginalSpeedLimitKmh` on DimRoadSegment).

### Type 1 — Overwrite (`DimTrafficCamera`, `DimVehicleType`, `DimIncidentType`)
Corrections and technical attributes with no analytical value in their history: camera
firmware version, a typo in a vehicle-type name. `MERGE … WHEN MATCHED THEN UPDATE`.
History is destroyed — acceptable because no report ever asks "what was the firmware in March".

### Type 2 — Add row (`DimRoadSegment`, `DimSensor`) — the workhorse
Changes that must not rewrite history: a **speed-limit change** on a segment, a **lane-count
change** after roadworks, a sensor being relocated. Facts loaded before the change keep
pointing at the old version, so "average speed vs. speed limit in 2024" uses the 2024 limit.

Mechanics (see the SQL — implemented with `MERGE` + `OUTPUT` trick):
1. New BK → insert row, `VersionNumber = 1`, `EffectiveDate = load date`, `ExpirationDate = 9999-12-31`, `IsCurrent = 1`.
2. Changed `HashDiff` on existing BK → expire the current row (`ExpirationDate = today, IsCurrent = 0`) **and** insert a new version.
3. Unchanged → no-op.

### Type 3 — Add column (`DimTrafficLight`)
The signal-engineering team retimes intersections and always wants **before/after
comparison of exactly the previous plan** — not the full history. `CurrentTimingPlan`,
`PreviousTimingPlan`, `TimingPlanChangeDate`. On change:
`PreviousTimingPlan = CurrentTimingPlan; CurrentTimingPlan = @new; TimingPlanChangeDate = @today`.
Type 3 is right when: (a) the change is rare and planned, (b) analysis needs *alternate
realities side-by-side in one row*, (c) only N previous states matter (here N = 1).

## Types Explained (documented, with sketches — not core to this model)

### Type 4 — Mini-dimension (history table)
Split rapidly changing attributes into their own small dimension keyed directly from the
fact. *Example here:* if `Sensor.Status` (OK/Degraded/Offline) flipped hourly, SCD2 on
DimSensor would explode. Instead build `DimSensorStatus` (a handful of rows) and put
`SensorStatusKey` on the fact:

```sql
CREATE TABLE dim.DimSensorStatus (
    SensorStatusKey INT IDENTITY PRIMARY KEY,
    Status          VARCHAR(20),  -- OK / Degraded / Offline
    HealthBand      VARCHAR(20)   -- Healthy / Warning / Critical
);
-- fact gains: SensorStatusKey INT NOT NULL REFERENCES dim.DimSensorStatus
```
The base DimSensor stays slow; the volatile state changes cost nothing.

### Type 5 — Type 4 + Type 1 outrigger (≈ "4 + 1")
Add a Type-1 FK on the *base dimension* pointing at the **current** mini-dimension row:
`DimSensor.CurrentSensorStatusKey` → `DimSensorStatus`. Reports can slice facts by
*historical* status (via the fact FK, Type 4) **or** by *current* status (via the outrigger,
Type 1) without touching the fact. The outrigger is overwritten on every change.

### Type 6 — Hybrid (1 + 2 + 3)
A Type-2 dimension that also carries a Type-1 "current value" column and Type-3 semantics.
*Example:* on Type-2 `DimRoadSegment` add `CurrentSpeedLimitKmh`, overwritten on **all**
historical rows whenever the limit changes, while `SpeedLimitKmh` stays frozen per version:

```sql
-- after a normal SCD2 versioning of SpeedLimitKmh:
UPDATE dim.DimRoadSegment
SET    CurrentSpeedLimitKmh = @newLimit          -- Type 1 across all versions
WHERE  SegmentCode = @bk;                        -- every version of the BK
```
Lets one query compare "speed vs. the limit **at the time**" and "speed vs. **today's**
limit" without a self-join. We include this column on DimRoadSegment as a demonstration.

## SCD Assignment Summary

| Dimension | Type | Trigger examples |
|---|---|---|
| DimDate / DimTime | 0 | never changes |
| DimVehicleType, DimIncidentType | 1 | name/description corrections |
| DimTrafficCamera | 1 | firmware, resolution upgrades |
| DimRoadSegment | **2** (+ Type-6 current column, + Type-0 original column) | speed limit, lane count, category |
| DimSensor | **2** | relocation, status class, ownership |
| DimTrafficLight | **3** | timing-plan replacement |
| (illustrative) DimSensorStatus | 4/5 | volatile health state |

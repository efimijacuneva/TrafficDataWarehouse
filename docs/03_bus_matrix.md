# Part 3 — Kimball Bus Matrix

The bus matrix is the contract of the warehouse: rows are **business processes** (each
becomes a fact table), columns are **conformed dimensions** shared across processes. It was
built *before* any table design, from the analytical requirements in doc 01.

## Bus Matrix

| Business Process ➜ Fact | Grain | Fact type | Date | Time | RoadSegment | Road* | Location* | Weather | VehicleType | Sensor | Camera | TrafficLight | IncidentType | EmergencyUnit |
|---|---|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **Vehicle detection** ➜ `FactTrafficEvent` | 1 row per vehicle detected by a sensor **or camera** | Transaction | ✔ | ✔ | ✔ | (✔) | (✔) | ✔ | ✔ | ✔ | ✔ | | | |
| **Hourly traffic monitoring** ➜ `FactHourlyTraffic` | 1 row per road segment per hour | Periodic snapshot | ✔ | hour | ✔ | (✔) | (✔) | ✔ | | | | | | |
| **Incident lifecycle** ➜ `FactIncidentLifecycle` | 1 row per incident, updated as milestones occur | Accumulating snapshot | ✔×5 | ✔×5 | ✔ | (✔) | (✔) | ✔ | | | | | ✔ | ✔ |

`✔×5` — the accumulating fact carries five role-playing date/time keys (detected,
dispatched, arrived, cleared, closed).
**`DimTrafficLight` is deliberately NOT conformed onto any fact.** No fact carries a
`TrafficLightKey`: signal controllers describe an *intersection*, not a segment-hour, so
there is no grain at which the key belongs. The traffic-light report reaches the
controller through the denormalized intersection name in the mart layer, which is a
report-layer join rather than a conformed relationship — and the assumption it depends on
(one controller per intersection name) is enforced by the `DUP_INTERSECTION_DIMTRAFFICLIGHT`
quality check.

`(✔)` — Road and Location attributes are reached **through** `DimRoadSegment` /
denormalized into it; they are conformed columns, not separate joins on the fact
(see doc 04 §"snowflake avoidance").

## Business Process Decisions (why these three, why this grain)

### 1. Vehicle detection — transaction fact
- **Grain statement:** *one row per individual vehicle detection event by one sensor.*
- The atomic, most granular event available. Kimball rule: capture the lowest grain you can
  afford — every other analysis (hourly, daily, weather cuts) is derivable from it, but the
  reverse is impossible.
- Measures: `SpeedKmh`, `HeadwaySeconds`, `OccupancyPct`, `SpeedOverLimitKmh` (derived).
  All fully additive except speed (additive only via weighted averaging — we store the
  count-weight implicitly as row count).
- `EventID` kept as a **degenerate dimension** (no attribute table needed; used for
  dedup/tracing).

### 2. Hourly traffic monitoring — periodic snapshot
- **Grain statement:** *one row per road segment per clock hour, present even for zero traffic.*
- Why a snapshot when we have the transaction fact? Three reasons:
  1. **Query economics** — 95% of dashboard queries are hourly or coarser; scanning 80M
     event rows/day for them is wasteful. The snapshot is ~2,000 segments × 24 h = 48K rows/day.
  2. **Density** — snapshots record *absence* (zero-traffic hours), which transaction facts
     cannot; "which segments were empty at 03:00" is a valid question.
  3. **Semi-additive measures** — `CongestionIndex`, `P85Speed` are precomputed correctly
     (volume-weighted) once, instead of being mis-averaged by report writers.
- Weather is attached at snapshot grain (dominant = worst condition reported across the
  city's stations that hour).

### 3. Incident lifecycle — accumulating snapshot
- **Grain statement:** *one row per incident, inserted at detection and UPDATEd as each
  milestone timestamp arrives.*
- The process has a defined pipeline with milestones:
  `Detected → Police dispatched → Police arrived → Road cleared → Closed`.
- An accumulating snapshot is the canonical Kimball design for pipeline processes: lag
  measures (`MinutesToDispatch`, `MinutesToArrive`, `MinutesToClear`, `TotalDurationMinutes`)
  are stored physically, so funnel/SLA analysis is a plain aggregate — no self-joins over an
  event log.
- This is the only fact table that is **updated in place** — acceptable because volume is
  low (dozens/day) and the row is short-lived until closure.

## Conformed Dimension Decisions

| Dimension | Conformed across | Notes |
|---|---|---|
| `DimDate`, `DimTime` | all facts | Classic role-playing (5 roles in the accumulating fact) |
| `DimRoadSegment` | all facts | The central conformed dimension; road + location attributes flattened in |
| `DimWeatherCondition` | all facts | Banded condition (not raw measurements) so it conforms across grains |
| `DimVehicleType` | detection fact | Sensor-classified category; individual `DimVehicle` reserved for the emergency fleet |
| `DimSensor`, `DimTrafficCamera` | detection fact | Two detector technologies, both conformed onto `FactTrafficEvent` via `SensorKey` / `CameraKey`; `DetectorType` says which applies and the other takes the unknown member (−1) |
| `DimTrafficLight` | *(not conformed — see note above)* | Reached through the mart layer only |
| `DimIncidentType`, `DimEmergencyUnit` | incident fact | |

**Measures summary**

| Fact | Additive | Semi-additive / derived | Degenerate |
|---|---|---|---|
| FactTrafficEvent | (row count), SpeedOverLimitKmh | SpeedKmh, HeadwaySeconds, OccupancyPct (avg-able w/ count) | EventID |
| FactHourlyTraffic | VehicleCount, IncidentCount, HeavyVehicleCount | AvgSpeedKmh, P85SpeedKmh, AvgOccupancyPct, CongestionIndex, AvgTempC, PrecipitationMm | — |
| FactIncidentLifecycle | (row count), LanesBlocked | MinutesToDispatch/Arrive/Clear, TotalDurationMinutes, SeverityScore | IncidentNumber |

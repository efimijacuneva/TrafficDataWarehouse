# Part 1 — Business Analysis

## 1. Business Problem

The city operates thousands of roadside assets — inductive-loop sensors, radar detectors,
ANPR traffic cameras, adaptive traffic lights and weather stations — that together produce
millions of operational records per day. Today this data lives in siloed operational systems:

- Sensor telemetry is written to flat CSV feeds and rotated away after days.
- Camera detections arrive as JSON event streams consumed only by the enforcement system.
- Incidents and police responses live in a transactional dispatch database.
- Weather observations sit in a third-party API archive.

**Consequence:** the traffic department can answer *"what is happening right now?"* but not
*"what has been happening, where, why, and what should we change?"*. Congestion mitigation,
signal-timing decisions, road investment planning and emergency-response staffing are made
on intuition rather than evidence.

The transactional systems cannot serve analytics directly:

- Analytical scans of billions of detection rows would degrade operational OLTP workloads.
- Data is normalized for write efficiency, not for slicing by time/road/weather.
- There is no conformed history — when a speed limit changes, the OLTP row is overwritten
  and the historical context is lost.

## 2. Business Goals

| # | Goal | How the DW enables it |
|---|------|----------------------|
| G1 | Reduce average network congestion 10% within 2 years | Identify recurring bottlenecks by segment × hour × weather |
| G2 | Cut emergency response time below 8 minutes (P90) | Accumulating incident lifecycle fact exposes every pipeline lag |
| G3 | Optimize signal timing at the 20 worst intersections | Traffic-light performance vs. throughput analysis |
| G4 | Evidence-based road investment | Multi-year traffic trend and capacity utilization reporting |
| G5 | Improve winter/storm readiness | Quantified weather impact on speed and volume |

## 3. Business Users

| User group | Role | Typical questions | Access pattern |
|------------|------|-------------------|----------------|
| Traffic Management Center operators | Tactical | Which corridors degraded this week vs. baseline? | Dashboards, hourly refresh |
| City transport planners | Strategic | Where do we add lanes / bus corridors? | Ad-hoc SQL, yearly trends |
| Police & emergency services command | Operational planning | Where are accident hotspots? Are we meeting response SLAs? | Weekly reports, KPI scorecards |
| Signal engineering team | Engineering | Which timing plans underperform? | Detailed drill-down queries |
| City leadership / mayor's office | Executive | Are smart-city KPIs improving? | Monthly executive dashboard |
| Data science team | Advanced analytics | Features for congestion prediction models | Direct Parquet/gold-layer access |

## 4. Analytical Requirements

The warehouse must answer, at minimum:

1. Which roads and road segments carry the most traffic (by day, month, year)?
2. Which intersections create the largest jams (lowest speed vs. speed limit ratio)?
3. How does traffic volume and speed vary across the hours of the day and days of the week?
4. What are the peak (rush) hours per road and per city district?
5. How do weather conditions (rain, snow, fog, temperature bands) change volume and speed?
6. Which vehicle categories (car, bus, truck, motorcycle) dominate congested segments?
7. Which segments have the highest incident/accident frequency and severity?
8. Average vehicle speed by hour × road × weather condition (three-way analysis).
9. Monthly and yearly traffic trends with year-over-year growth.
10. Historical-pattern-based peak prediction (85th percentile volumes by segment/hour/weekday).
11. Emergency response funnel: detection → dispatch → arrival → clearance → closure durations.
12. Traffic-light efficiency: throughput per green-cycle, before/after timing-plan changes.
13. Composite smart-city indicators (congestion index, network reliability, response SLA %).

Non-functional requirements:

- **History**: minimum 5 years of hourly aggregates; 13 months of event-grain detail online.
- **Latency**: warehouse refreshed nightly; hourly snapshot fact refreshed hourly.
- **Volume**: ~2,000 sensors × ~1 detection/2s peak ≈ 40–80M detection events/day → the
  transaction-grain fact must use columnstore + partitioning; heavy lifting happens in Spark.
- **Auditability**: every loaded row traceable to an ETL batch; rejected rows quarantined, never dropped silently.

## 5. KPIs

| KPI | Definition | Target |
|-----|-----------|--------|
| **Congestion Index** | 1 − (avg speed / speed limit), volume-weighted, per segment-hour | < 0.35 network avg |
| **Peak Hour Volume** | Max hourly vehicle count per segment | trend ↓ on treated corridors |
| **Average Travel Speed** | Volume-weighted mean speed by corridor | ≥ 70% of limit off-peak |
| **Incident Response Time (P50/P90)** | Dispatch→arrival minutes from accumulating fact | P90 < 8 min |
| **Incident Clearance Time** | Arrival→road-cleared minutes | P50 < 45 min |
| **Accident Rate** | Accidents per million vehicle-km per segment | trend ↓ YoY |
| **Signal Efficiency** | Vehicles served per signal cycle at intersection approaches | ↑ after retiming |
| **Weather Speed Penalty** | % speed drop in rain/snow vs. dry baseline | monitored |
| **Sensor Health** | % sensors delivering valid data per day (data-quality KPI) | > 97% |

## 6. Expected Dashboards

1. **Network Overview** — city map heat, congestion index gauge, today vs. typical profile.
2. **Corridor Deep Dive** — per-road hourly speed/volume curves, ranked worst segments.
3. **Incident & Response** — response funnel, SLA attainment, hotspot map, severity mix.
4. **Weather Impact** — speed/volume deltas by condition band, seasonal comparisons.
5. **Signal Performance** — intersection throughput, cycle efficiency, retiming before/after.
6. **Executive Scorecard** — the 9 KPIs above with monthly trend sparklines and RAG status.

## 7. Expected Reports (catalogue in `reports/`)

Top-10 busiest roads · average speed by road · monthly/daily traffic trends · rush-hour
analysis · accident hotspots · vehicle-category statistics · traffic by weather · traffic
by district · traffic by weekday · traffic-light performance · emergency response times ·
top congested intersections · YoY growth report.

## 8. Success Metrics for the Project Itself

- All 13 analytical requirements answerable with a single SQL query against the star schema.
- Nightly ETL end-to-end under 60 minutes for a full day of data; hourly snapshot under 5 minutes.
- 100% of fact rows resolve to valid dimension keys (unknown member −1 where unresolvable).
- Data-quality reject rate visible per batch; silent data loss = 0.
- Analysts self-serve: no query against OLTP systems for reporting.

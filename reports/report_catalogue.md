# Part 11 — Business Report Catalogue

Every report is backed by a `mart` view ([sql/warehouse/06_mart_views.sql](../sql/warehouse/06_mart_views.sql))
so BI tools, analysts and this catalogue share one definition. Frequencies assume the
nightly warehouse refresh; "hourly" reports additionally use the intraday snapshot refresh.

| # | Report | Mart view | Audience / decision it supports | Frequency |
|---|--------|-----------|--------------------------------|-----------|
| R01 | **Top 10 busiest roads** | `mart.vTopBusiestRoads` | Planners — corridor investment shortlist | Monthly |
| R02 | **Average speed by road** (volume-weighted + P85) | `mart.vAvgSpeedByRoad` | Signal engineers — speed-limit compliance review | Weekly |
| R03 | **Monthly traffic trend** | `mart.vMonthlyTraffic` | Executives — YoY/MoM growth, seasonality | Monthly |
| R04 | **Daily traffic** (weekend/holiday flagged) | `mart.vDailyTraffic` | TMC — daily anomaly review | Daily |
| R05 | **Rush-hour analysis** (24-h profile, weekday vs weekend) | `mart.vRushHourProfile` | TMC — staffing & signal-plan windows | Weekly |
| R06 | **Accident hotspots** | `mart.vAccidentHotspots` | Police + road safety board — enforcement placement | Weekly |
| R07 | **Vehicle category statistics** (incl. speeding by class) | `mart.vVehicleCategoryStats` | Policy — HGV routing, low-emission zones | Monthly |
| R08 | **Traffic by weather** | `mart.vTrafficByWeather` | Winter-service planning — weather speed penalty | Monthly |
| R09 | **Traffic by district** | `mart.vTrafficByDistrict` | City council — district-level equity of investment | Monthly |
| R10 | **Traffic by weekday** | `mart.vTrafficByWeekday` | Planners — weekly demand curve | Monthly |
| R11 | **Traffic-light performance** (vehicles/cycle, before-after retiming via SCD3) | `mart.vTrafficLightPerformance` | Signal engineering — retiming effectiveness | Weekly |
| R12 | **Emergency response funnel** (dispatch → arrive → clear, SLA %) | `mart.vEmergencyResponse` | Emergency services command — SLA management | Weekly |
| R13 | **Top congested intersections** | `mart.vTopCongestedIntersections` | Signal engineering — retiming candidate list | Weekly |
| R14 | **Executive daily rollup** | `mart.vFactDailyTraffic` | Mayor's office scorecard source | Daily |

## Ad-hoc analytical queries

Deeper, parameterized analyses live in [sql/analytics/](../sql/analytics/):
window-function analyses (per-district Top-N, running totals, moving averages,
day-over-day deltas, response-time percentiles), multi-level aggregations
(ROLLUP/CUBE/GROUPING SETS), road-network graph analysis (recursive CTEs), and
Top-N patterns.

## Sample invocation

```sql
SELECT * FROM mart.vTopBusiestRoads;
SELECT * FROM mart.vEmergencyResponse WHERE YearMonth = '2026-06' ORDER BY ResponseSLAPct;
SELECT * FROM mart.vRushHourProfile WHERE IsWeekend = 0 ORDER BY HourOfDay;
```

## KPI ↔ report traceability

| KPI (docs/01) | Report(s) |
|---|---|
| Congestion Index | R01, R04, R05, R09, R13 |
| Peak Hour Volume | R05 |
| Average Travel Speed | R02 |
| Incident Response Time P50/P90 | R12 + analytics Q7 |
| Incident Clearance Time | R06, R12 |
| Accident Rate | R06 |
| Signal Efficiency | R11 |
| Weather Speed Penalty | R08 |
| Sensor Health | Spark quality dataset (`gold/quality_daily`) |

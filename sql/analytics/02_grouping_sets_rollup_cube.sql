/* ============================================================================
   Part 10 — Advanced SQL: GROUPING SETS / ROLLUP / CUBE / GROUPING_ID
   Multi-level aggregation in a single scan — the SQL answer to "give me the
   report with subtotals and a grand total".
   ========================================================================== */
USE TrafficDW;
GO

/* ============================================================================
   Q1. "Monthly traffic per road category WITH category subtotals and a grand
        total, in one result set."  → ROLLUP (hierarchical subtotals).
       ROLLUP(A, B) produces: (A,B), (A), () — a drill-up path.
   ========================================================================== */
/* GROUPING(col) - not ISNULL(col) - distinguishes "NULL because this row is a
   SUBTOTAL" from "NULL because the underlying value is NULL". ISNULL conflates
   them: a genuinely NULL RoadCategory would be mislabelled as the grand total.
   Both columns are NOT NULL today, so ISNULL happened to work, but the pattern
   is wrong and the same file demonstrates GROUPING_ID correctly in Q3. */
SELECT CASE WHEN GROUPING(s.RoadCategory) = 1 THEN '== ALL CATEGORIES =='
            ELSE s.RoadCategory END                     AS RoadCategory,
       CASE WHEN GROUPING(d.YearMonth) = 1 THEN '== ALL MONTHS =='
            ELSE d.YearMonth END                        AS YearMonth,
       SUM(f.VehicleCount)                              AS TotalVehicles,
       CAST(AVG(f.AvgSpeedKmh) AS DECIMAL(5,1))         AS AvgSpeed
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
JOIN dim.DimDate d        ON d.DateKey = f.DateKey
GROUP BY ROLLUP (s.RoadCategory, d.YearMonth)
ORDER BY GROUPING(s.RoadCategory), s.RoadCategory,
         GROUPING(d.YearMonth),   d.YearMonth;
GO

/* ============================================================================
   Q2. "Cross-tab exploration: volume by weather condition × vehicle heaviness,
        including ALL marginal totals."  → CUBE (every combination).
       CUBE(A, B) produces: (A,B), (A), (B), ().
   ========================================================================== */
SELECT CASE WHEN GROUPING(w.ConditionName) = 1 THEN 'ALL WEATHER'
            ELSE w.ConditionName END                             AS Weather,
       CASE WHEN GROUPING(vt.IsHeavy) = 1 THEN 'ALL VEHICLES'
            WHEN vt.IsHeavy = 1 THEN 'Heavy' ELSE 'Light' END    AS VehicleClass,
       COUNT_BIG(*)                                              AS Detections,
       CAST(AVG(f.SpeedKmh) AS DECIMAL(5,1))                     AS AvgSpeed
FROM fact.FactTrafficEvent f
JOIN dim.DimWeatherCondition w ON w.WeatherKey = f.WeatherKey
JOIN dim.DimVehicleType vt     ON vt.VehicleTypeKey = f.VehicleTypeKey
GROUP BY CUBE (w.ConditionName, vt.IsHeavy);
GO

/* ============================================================================
   Q3. "One extract feeding three dashboard tiles: totals by district,
        by rush-hour flag, and by district × weekday — nothing else."
       → GROUPING SETS (precisely the sets you want, no noise)
       + GROUPING_ID to label which set each row belongs to.
   ========================================================================== */
SELECT GROUPING_ID(s.District, d.DayName, t.RushFlag)  AS GroupingID,
       CASE GROUPING_ID(s.District, d.DayName, t.RushFlag)
            WHEN 3 THEN 'By District'                  -- (District)         : 0 1 1
            WHEN 6 THEN 'By RushFlag'                  -- (RushFlag)         : 1 1 0
            WHEN 1 THEN 'District × Weekday'           -- (District, DayName): 0 0 1
       END                                             AS AggregationLevel,
       s.District, d.DayName,
       t.RushFlag,
       SUM(f.VehicleCount)                             AS TotalVehicles,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3))    AS AvgCongestion
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
JOIN dim.DimDate d        ON d.DateKey = f.DateKey
CROSS APPLY (SELECT CASE WHEN f.HourOfDay IN (7,8,16,17,18) THEN 'Rush' ELSE 'Off-peak' END AS RushFlag) t
GROUP BY GROUPING SETS (
    (s.District),
    (t.RushFlag),
    (s.District, d.DayName)
)
ORDER BY GroupingID, s.District, d.DayName;
GO

/* ============================================================================
   Q4. "Incident severity report: counts and response SLA by incident category
        and severity with drill-up subtotals."  → ROLLUP over the accumulating fact.
   ========================================================================== */
SELECT CASE WHEN GROUPING(it.Category) = 1 THEN 'ALL CATEGORIES'
            ELSE it.Category END                        AS IncidentCategory,
       CASE WHEN GROUPING(f.Severity) = 1 THEN NULL ELSE f.Severity END AS Severity,
       COUNT(*)                                         AS Incidents,
       CAST(AVG(1.0 * f.MinutesToArrive) AS DECIMAL(6,1)) AS AvgResponseMin,
       SUM(CASE WHEN f.MinutesToArrive <= 8 THEN 1 ELSE 0 END) AS WithinSLA,
       CAST(100.0 * SUM(CASE WHEN f.MinutesToArrive <= 8 THEN 1 ELSE 0 END)
            / NULLIF(COUNT(f.MinutesToArrive), 0) AS DECIMAL(5,1)) AS SLAPct
FROM fact.FactIncidentLifecycle f
JOIN dim.DimIncidentType it ON it.IncidentTypeKey = f.IncidentTypeKey
GROUP BY ROLLUP (it.Category, f.Severity)
ORDER BY GROUPING(it.Category), it.Category, GROUPING(f.Severity), f.Severity;
GO

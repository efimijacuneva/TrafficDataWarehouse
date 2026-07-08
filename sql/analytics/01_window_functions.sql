/* ============================================================================
   Part 10 — Advanced SQL: WINDOW FUNCTIONS
   Each query answers a concrete business question from docs/01_business_analysis.md
   ========================================================================== */
USE TrafficDW;
GO

/* ============================================================================
   Q1. "Which are the 3 busiest road segments in EACH district?"
       ROW_NUMBER + PARTITION BY — per-group Top-N.
   ========================================================================== */
WITH segment_volume AS (
    SELECT s.District, s.RoadName, s.SegmentCode,
           SUM(f.VehicleCount) AS TotalVehicles
    FROM fact.FactHourlyTraffic f
    JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
    JOIN dim.DimDate d        ON d.DateKey = f.DateKey
    WHERE d.[Year] = 2026 AND d.[MonthNumber] = 6
    GROUP BY s.District, s.RoadName, s.SegmentCode
),
ranked AS (
    SELECT District, RoadName, SegmentCode, TotalVehicles,
           ROW_NUMBER() OVER (PARTITION BY District ORDER BY TotalVehicles DESC) AS RankInDistrict
    FROM segment_volume
)
SELECT District, RoadName, SegmentCode, TotalVehicles, RankInDistrict
FROM ranked
WHERE RankInDistrict <= 3          -- window functions can't sit in WHERE → outer CTE filter
ORDER BY District, RankInDistrict;
GO

/* ============================================================================
   Q2. "Rank roads by average congestion — and show how ties behave."
       RANK vs DENSE_RANK side by side.
   ========================================================================== */
SELECT s.RoadName,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3))                       AS AvgCongestion,
       RANK()       OVER (ORDER BY AVG(f.CongestionIndex) DESC)           AS RankWithGaps,
       DENSE_RANK() OVER (ORDER BY AVG(f.CongestionIndex) DESC)           AS RankNoGaps
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
WHERE f.CongestionIndex IS NOT NULL
GROUP BY s.RoadName
ORDER BY AvgCongestion DESC;
GO

/* ============================================================================
   Q3. "Split all segments into 4 congestion quartiles for maintenance
        prioritization (Q1 = worst 25%)."
       NTILE.
   ========================================================================== */
SELECT s.SegmentCode, s.RoadName, s.District,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3)) AS AvgCongestion,
       NTILE(4) OVER (ORDER BY AVG(f.CongestionIndex) DESC) AS CongestionQuartile
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
WHERE f.CongestionIndex IS NOT NULL
GROUP BY s.SegmentCode, s.RoadName, s.District;
GO

/* ============================================================================
   Q4. "Cumulative traffic through the day — when has a segment reached
        half of its daily volume?"
       RUNNING TOTAL: SUM OVER (ORDER BY … ROWS UNBOUNDED PRECEDING).
   ========================================================================== */
SELECT d.FullDate, f.HourOfDay, s.SegmentCode,
       f.VehicleCount,
       SUM(f.VehicleCount) OVER (
            PARTITION BY f.DateKey, f.RoadSegmentKey
            ORDER BY f.HourOfDay
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)          AS RunningVehicles,
       CAST(100.0 * SUM(f.VehicleCount) OVER (
            PARTITION BY f.DateKey, f.RoadSegmentKey
            ORDER BY f.HourOfDay
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            / NULLIF(SUM(f.VehicleCount) OVER (
                     PARTITION BY f.DateKey, f.RoadSegmentKey), 0) AS DECIMAL(5,1)) AS PctOfDay
FROM fact.FactHourlyTraffic f
JOIN dim.DimDate d        ON d.DateKey = f.DateKey
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
WHERE d.FullDate = '2026-06-15'
ORDER BY s.SegmentCode, f.HourOfDay;
GO

/* ============================================================================
   Q5. "Smooth noisy hourly speeds: 7-hour centered MOVING AVERAGE per segment."
       AVG OVER (… ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING).
   ========================================================================== */
SELECT d.FullDate, f.HourOfDay, s.SegmentCode,
       f.AvgSpeedKmh,
       CAST(AVG(f.AvgSpeedKmh) OVER (
            PARTITION BY f.RoadSegmentKey
            ORDER BY d.FullDate, f.HourOfDay
            ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS DECIMAL(5,1)) AS MovingAvgSpeed7h
FROM fact.FactHourlyTraffic f
JOIN dim.DimDate d        ON d.DateKey = f.DateKey
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
WHERE f.AvgSpeedKmh IS NOT NULL
ORDER BY s.SegmentCode, d.FullDate, f.HourOfDay;
GO

/* ============================================================================
   Q6. "Day-over-day traffic change per road — where did volume jump?"
       LAG + percentage delta.
   ========================================================================== */
WITH daily AS (
    SELECT s.RoadName, d.FullDate, SUM(f.VehicleCount) AS DayVolume
    FROM fact.FactHourlyTraffic f
    JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
    JOIN dim.DimDate d        ON d.DateKey = f.DateKey
    GROUP BY s.RoadName, d.FullDate
)
SELECT RoadName, FullDate, DayVolume,
       LAG(DayVolume) OVER (PARTITION BY RoadName ORDER BY FullDate)   AS PrevDayVolume,
       CAST(100.0 * (DayVolume - LAG(DayVolume) OVER (PARTITION BY RoadName ORDER BY FullDate))
            / NULLIF(LAG(DayVolume) OVER (PARTITION BY RoadName ORDER BY FullDate), 0)
            AS DECIMAL(6,1))                                           AS PctChange
FROM daily
ORDER BY RoadName, FullDate;
GO

/* ============================================================================
   Q7. "Emergency response percentiles: P50/P90 response time by incident type."
       PERCENTILE_CONT window + FIRST_VALUE demonstration.
   ========================================================================== */
SELECT DISTINCT
       it.TypeName,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.MinutesToArrive)
           OVER (PARTITION BY it.TypeName) AS MedianResponseMin,
       PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.MinutesToArrive)
           OVER (PARTITION BY it.TypeName) AS P90ResponseMin,
       FIRST_VALUE(f.IncidentNumber) OVER (
           PARTITION BY it.TypeName ORDER BY f.MinutesToArrive DESC) AS SlowestIncident
FROM fact.FactIncidentLifecycle f
JOIN dim.DimIncidentType it ON it.IncidentTypeKey = f.IncidentTypeKey
WHERE f.MinutesToArrive IS NOT NULL;
GO

/* ============================================================================
   Q8. "Detect rush-hour onset: first hour each day when a segment's speed
        drops below 60% of the limit."
       Windowed flagging + MIN OVER.
   ========================================================================== */
WITH flagged AS (
    SELECT f.DateKey, f.HourOfDay, s.SegmentCode, s.SpeedLimitKmh, f.AvgSpeedKmh,
           CASE WHEN f.AvgSpeedKmh < 0.6 * s.SpeedLimitKmh THEN f.HourOfDay END AS CongestedHour
    FROM fact.FactHourlyTraffic f
    JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
    WHERE f.AvgSpeedKmh IS NOT NULL
)
SELECT DISTINCT DateKey, SegmentCode,
       MIN(CongestedHour) OVER (PARTITION BY DateKey, SegmentCode) AS FirstCongestedHour
FROM flagged
ORDER BY DateKey, SegmentCode;
GO

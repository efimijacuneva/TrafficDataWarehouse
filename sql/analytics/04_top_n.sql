/* ============================================================================
   Part 10 — Advanced SQL: TOP-N ANALYSIS
   Three canonical Top-N patterns, each on a real business question:
     1. simple TOP (N) WITH TIES          — global leaderboard
     2. ROW_NUMBER per partition          — Top-N per group
     3. TOP (N) PERCENT + APPLY           — proportional cut & per-row drill
   ========================================================================== */
USE TrafficDW;
GO

/* ============================================================================
   Q1. "Top 10 busiest roads this month" — the flagship report.
       WITH TIES so two equally busy roads in 10th place both appear.
   ========================================================================== */
SELECT TOP (10) WITH TIES
       s.RoadName,
       s.RoadCategory,
       SUM(f.VehicleCount)                          AS TotalVehicles,
       SUM(f.HeavyVehicleCount)                     AS HeavyVehicles,
       CAST(SUM(f.AvgSpeedKmh     * f.VehicleCount)
            / NULLIF(SUM(f.VehicleCount), 0) AS DECIMAL(5,1)) AS AvgSpeed,      -- volume-weighted
       CAST(SUM(f.CongestionIndex * f.VehicleCount)
            / NULLIF(SUM(f.VehicleCount), 0) AS DECIMAL(4,3)) AS AvgCongestion  -- volume-weighted
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
JOIN dim.DimDate d        ON d.DateKey = f.DateKey
WHERE d.YearMonth = '2026-06'
GROUP BY s.RoadName, s.RoadCategory
ORDER BY TotalVehicles DESC;
GO

/* ============================================================================
   Q2. "Top 5 most congested INTERSECTION APPROACHES per district"
       (worst end-intersections by volume-weighted congestion) — Top-N per group.
   ========================================================================== */
WITH approach AS (
    SELECT s.District, s.EndIntersection,
           SUM(f.VehicleCount)                                          AS Volume,
           SUM(f.CongestionIndex * f.VehicleCount) / NULLIF(SUM(f.VehicleCount), 0)
                                                                        AS WeightedCongestion
    FROM fact.FactHourlyTraffic f
    JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
    WHERE f.CongestionIndex IS NOT NULL
    GROUP BY s.District, s.EndIntersection
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY District
                                 ORDER BY WeightedCongestion DESC) AS rn
    FROM approach
    WHERE Volume > 0
)
SELECT District, EndIntersection,
       CAST(WeightedCongestion AS DECIMAL(4,3)) AS WeightedCongestion, Volume
FROM ranked
WHERE rn <= 5
ORDER BY District, rn;
GO

/* ============================================================================
   Q3. "The worst 5 PERCENT of segment-hours by congestion — and what the
        weather was during them." TOP PERCENT + dimensional context.
   ========================================================================== */
SELECT TOP (5) PERCENT
       d.FullDate, f.HourOfDay, s.RoadName, s.SegmentCode,
       f.CongestionIndex, f.AvgSpeedKmh, s.SpeedLimitKmh,
       w.ConditionName, w.PrecipBand
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s      ON s.RoadSegmentKey = f.RoadSegmentKey
JOIN dim.DimDate d             ON d.DateKey = f.DateKey
JOIN dim.DimWeatherCondition w ON w.WeatherKey = f.WeatherKey
WHERE f.CongestionIndex IS NOT NULL AND f.VehicleCount >= 50   -- ignore empty-road noise
ORDER BY f.CongestionIndex DESC;
GO

/* ============================================================================
   Q4. "For each of the 5 busiest roads, its 3 worst hours of the day."
       Nested Top-N: TOP in an APPLY per outer row.
   ========================================================================== */
WITH top_roads AS (
    SELECT TOP (5) s.RoadName, SUM(f.VehicleCount) AS TotalVehicles
    FROM fact.FactHourlyTraffic f
    JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
    GROUP BY s.RoadName
    ORDER BY TotalVehicles DESC
)
SELECT tr.RoadName, tr.TotalVehicles, worst.HourOfDay, worst.AvgCongestion
FROM top_roads tr
CROSS APPLY (
    SELECT TOP (3) f.HourOfDay,
           CAST(SUM(f.CongestionIndex * f.VehicleCount)
                / NULLIF(SUM(f.VehicleCount), 0) AS DECIMAL(4,3)) AS AvgCongestion
    FROM fact.FactHourlyTraffic f
    JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
    WHERE s.RoadName = tr.RoadName AND f.CongestionIndex IS NOT NULL
    GROUP BY f.HourOfDay
    HAVING SUM(f.VehicleCount) > 0
    ORDER BY SUM(f.CongestionIndex * f.VehicleCount) / NULLIF(SUM(f.VehicleCount), 0) DESC
) worst
ORDER BY tr.TotalVehicles DESC, worst.AvgCongestion DESC;
GO

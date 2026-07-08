/* ============================================================================
   TrafficDW — mart layer: presentation views for BI tools (Power BI / SSRS)
   One view per catalogued business report (reports/report_catalogue.md).
   Views also provide the conformed ROLLUPS of DimRoadSegment (road, location)
   promised by the bus matrix.
   ========================================================================== */
USE TrafficDW;
GO

/* ------------------------- conformed rollup 'dimensions' (no snowflake) --- */
CREATE OR ALTER VIEW mart.vDimRoad AS
SELECT DISTINCT RoadCode, RoadName, RoadCategory, City
FROM dim.DimRoadSegment WHERE IsCurrent = 1 AND RoadSegmentKey <> -1;
GO

CREATE OR ALTER VIEW mart.vDimLocation AS
SELECT DISTINCT City, District
FROM dim.DimRoadSegment WHERE IsCurrent = 1 AND RoadSegmentKey <> -1;
GO

/* --------------------------------------------- R01 Top busiest roads ------ */
CREATE OR ALTER VIEW mart.vTopBusiestRoads AS
SELECT TOP (10) WITH TIES
       s.RoadName, s.RoadCategory,
       SUM(f.VehicleCount)                          AS TotalVehicles,
       CAST(AVG(f.AvgSpeedKmh)     AS DECIMAL(5,1)) AS AvgSpeedKmh,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3)) AS AvgCongestion
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
GROUP BY s.RoadName, s.RoadCategory
ORDER BY TotalVehicles DESC;
GO

/* --------------------------------------------- R02 Average speed by road -- */
CREATE OR ALTER VIEW mart.vAvgSpeedByRoad AS
SELECT s.RoadName,
       CAST(SUM(f.AvgSpeedKmh * f.VehicleCount) / NULLIF(SUM(f.VehicleCount),0) AS DECIMAL(5,1))
                                                    AS VolumeWeightedSpeed,
       CAST(AVG(f.P85SpeedKmh) AS DECIMAL(5,1))     AS AvgP85Speed,
       MAX(s.SpeedLimitKmh)                         AS MaxSpeedLimit
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
WHERE f.AvgSpeedKmh IS NOT NULL
GROUP BY s.RoadName;
GO

/* ------------------------------------- R03 Monthly traffic trend ---------- */
CREATE OR ALTER VIEW mart.vMonthlyTraffic AS
SELECT d.[Year], d.MonthNumber, d.YearMonth,
       SUM(f.VehicleCount)                          AS TotalVehicles,
       SUM(f.HeavyVehicleCount)                     AS HeavyVehicles,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3)) AS AvgCongestion,
       SUM(f.IncidentCount)                         AS Incidents
FROM fact.FactHourlyTraffic f
JOIN dim.DimDate d ON d.DateKey = f.DateKey
GROUP BY d.[Year], d.MonthNumber, d.YearMonth;
GO

/* --------------------------------------- R04 Daily traffic profile -------- */
CREATE OR ALTER VIEW mart.vDailyTraffic AS
SELECT d.FullDate, d.DayName, d.IsWeekend, d.IsHoliday,
       SUM(f.VehicleCount)                          AS TotalVehicles,
       CAST(AVG(f.AvgSpeedKmh) AS DECIMAL(5,1))     AS AvgSpeedKmh,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3)) AS AvgCongestion
FROM fact.FactHourlyTraffic f
JOIN dim.DimDate d ON d.DateKey = f.DateKey
GROUP BY d.FullDate, d.DayName, d.IsWeekend, d.IsHoliday;
GO

/* ------------------------------------------ R05 Rush-hour analysis -------- */
CREATE OR ALTER VIEW mart.vRushHourProfile AS
SELECT f.HourOfDay,
       d.IsWeekend,
       SUM(f.VehicleCount)                          AS TotalVehicles,
       CAST(AVG(f.AvgSpeedKmh) AS DECIMAL(5,1))     AS AvgSpeedKmh,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3)) AS AvgCongestion
FROM fact.FactHourlyTraffic f
JOIN dim.DimDate d ON d.DateKey = f.DateKey
GROUP BY f.HourOfDay, d.IsWeekend;
GO

/* --------------------------------------- R06 Accident hotspots ------------ */
CREATE OR ALTER VIEW mart.vAccidentHotspots AS
SELECT s.RoadName, s.SegmentCode, s.District,
       COUNT(*)                                       AS Accidents,
       CAST(AVG(1.0 * f.Severity) AS DECIMAL(3,1))    AS AvgSeverity,
       SUM(f.LanesBlocked)                            AS TotalLanesBlocked,
       CAST(AVG(1.0 * f.MinutesToClear) AS DECIMAL(6,1)) AS AvgClearanceMin
FROM fact.FactIncidentLifecycle f
JOIN dim.DimRoadSegment s   ON s.RoadSegmentKey = f.RoadSegmentKey
JOIN dim.DimIncidentType it ON it.IncidentTypeKey = f.IncidentTypeKey
WHERE it.Category = 'Accident'
GROUP BY s.RoadName, s.SegmentCode, s.District;
GO

/* ---------------------------------- R07 Vehicle category statistics ------- */
CREATE OR ALTER VIEW mart.vVehicleCategoryStats AS
SELECT vt.Category, vt.TypeName,
       COUNT_BIG(*)                                  AS Detections,
       CAST(AVG(f.SpeedKmh) AS DECIMAL(5,1))         AS AvgSpeedKmh,
       CAST(AVG(f.SpeedOverLimitKmh) AS DECIMAL(5,1)) AS AvgSpeedOverLimit,
       SUM(CASE WHEN f.SpeedOverLimitKmh > 10 THEN 1 ELSE 0 END) AS SpeedingDetections
FROM fact.FactTrafficEvent f
JOIN dim.DimVehicleType vt ON vt.VehicleTypeKey = f.VehicleTypeKey
GROUP BY vt.Category, vt.TypeName;
GO

/* --------------------------------------- R08 Traffic by weather ----------- */
CREATE OR ALTER VIEW mart.vTrafficByWeather AS
SELECT w.ConditionName, w.PrecipBand, w.IsSevere,
       SUM(f.VehicleCount)                          AS TotalVehicles,
       CAST(AVG(f.AvgSpeedKmh) AS DECIMAL(5,1))     AS AvgSpeedKmh,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3)) AS AvgCongestion
FROM fact.FactHourlyTraffic f
JOIN dim.DimWeatherCondition w ON w.WeatherKey = f.WeatherKey
GROUP BY w.ConditionName, w.PrecipBand, w.IsSevere;
GO

/* --------------------------------------- R09 Traffic by district ---------- */
CREATE OR ALTER VIEW mart.vTrafficByDistrict AS
SELECT s.City, s.District,
       SUM(f.VehicleCount)                          AS TotalVehicles,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3)) AS AvgCongestion,
       SUM(f.IncidentCount)                         AS Incidents
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
GROUP BY s.City, s.District;
GO

/* --------------------------------------- R10 Traffic by weekday ----------- */
CREATE OR ALTER VIEW mart.vTrafficByWeekday AS
SELECT d.DayOfWeek, d.DayName,
       SUM(f.VehicleCount)                          AS TotalVehicles,
       CAST(AVG(f.AvgSpeedKmh) AS DECIMAL(5,1))     AS AvgSpeedKmh,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3)) AS AvgCongestion
FROM fact.FactHourlyTraffic f
JOIN dim.DimDate d ON d.DateKey = f.DateKey
GROUP BY d.DayOfWeek, d.DayName;
GO

/* ------------------------------ R11 Traffic-light performance ------------- */
/* Throughput at signalized end-intersections vs. the controller's plan;
   PreviousTimingPlan (SCD3) enables before/after comparison in one row.    */
CREATE OR ALTER VIEW mart.vTrafficLightPerformance AS
SELECT tl.ControllerCode, tl.IntersectionName,
       tl.CurrentTimingPlan, tl.PreviousTimingPlan, tl.TimingPlanChangeDate,
       tl.CycleSeconds,
       SUM(f.VehicleCount)                              AS ApproachVolume,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3))     AS AvgApproachCongestion,
       CAST(SUM(1.0 * f.VehicleCount) /
            NULLIF(SUM(3600.0 / tl.CycleSeconds), 0) AS DECIMAL(8,2)) AS VehiclesPerCycle
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s   ON s.RoadSegmentKey = f.RoadSegmentKey
JOIN dim.DimTrafficLight tl ON tl.IntersectionName = s.EndIntersection
GROUP BY tl.ControllerCode, tl.IntersectionName, tl.CurrentTimingPlan,
         tl.PreviousTimingPlan, tl.TimingPlanChangeDate, tl.CycleSeconds;
GO

/* --------------------------------- R12 Emergency response funnel ---------- */
CREATE OR ALTER VIEW mart.vEmergencyResponse AS
SELECT dd.YearMonth,
       it.Category                                      AS IncidentCategory,
       eu.UnitType,
       COUNT(*)                                         AS Incidents,
       CAST(AVG(1.0 * f.MinutesToDispatch) AS DECIMAL(6,1)) AS AvgDispatchMin,
       CAST(AVG(1.0 * f.MinutesToArrive)   AS DECIMAL(6,1)) AS AvgResponseMin,
       CAST(AVG(1.0 * f.MinutesToClear)    AS DECIMAL(6,1)) AS AvgClearanceMin,
       CAST(AVG(1.0 * f.TotalDurationMinutes) AS DECIMAL(7,1)) AS AvgTotalMin,
       CAST(100.0 * SUM(CASE WHEN f.MinutesToArrive <= 8 THEN 1 ELSE 0 END)
            / NULLIF(COUNT(f.MinutesToArrive), 0) AS DECIMAL(5,1)) AS ResponseSLAPct
FROM fact.FactIncidentLifecycle f
JOIN dim.DimDate dd          ON dd.DateKey = f.DetectedDateKey
JOIN dim.DimIncidentType it  ON it.IncidentTypeKey = f.IncidentTypeKey
JOIN dim.DimEmergencyUnit eu ON eu.EmergencyUnitKey = f.EmergencyUnitKey
GROUP BY dd.YearMonth, it.Category, eu.UnitType;
GO

/* ------------------------------- R13 Top congested intersections ---------- */
CREATE OR ALTER VIEW mart.vTopCongestedIntersections AS
SELECT TOP (10) WITH TIES
       s.EndIntersection                                AS Intersection,
       s.District,
       SUM(f.VehicleCount)                              AS ApproachVolume,
       CAST(SUM(f.CongestionIndex * f.VehicleCount)
            / NULLIF(SUM(f.VehicleCount), 0) AS DECIMAL(4,3)) AS WeightedCongestion
FROM fact.FactHourlyTraffic f
JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
WHERE f.CongestionIndex IS NOT NULL
GROUP BY s.EndIntersection, s.District
ORDER BY WeightedCongestion DESC;
GO

/* ------------------------------- R14 Executive daily rollup --------------- */
CREATE OR ALTER VIEW mart.vFactDailyTraffic AS
SELECT f.DateKey, d.FullDate, d.DayName, d.IsWeekend,
       SUM(f.VehicleCount)                           AS TotalVehicles,
       SUM(f.HeavyVehicleCount)                      AS HeavyVehicles,
       CAST(AVG(f.AvgSpeedKmh) AS DECIMAL(5,1))      AS AvgSpeedKmh,
       CAST(AVG(f.CongestionIndex) AS DECIMAL(4,3))  AS AvgCongestionIndex,
       SUM(f.IncidentCount)                          AS Incidents
FROM fact.FactHourlyTraffic f
JOIN dim.DimDate d ON d.DateKey = f.DateKey
GROUP BY f.DateKey, d.FullDate, d.DayName, d.IsWeekend;
GO

PRINT 'Mart views created.';
GO

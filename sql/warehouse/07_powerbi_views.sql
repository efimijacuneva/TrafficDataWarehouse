/* ============================================================================
   TrafficDW — POWER BI SEMANTIC LAYER (docs/12_powerbi_dashboards.md)

   mart.vPbi* views are the ONLY objects the Power BI model imports — a stable
   contract between the warehouse and the report layer (renames/refactors in
   dim/fact never break the .pbix). All views read warehouse star-schema
   objects (dim/fact) plus the ETL framework's own audit tables; the OLTP
   database is never touched.

   Model conventions (why the views look the way they do):
     - SCD2 dimension views expose ALL versions: facts reference historical
       surrogate keys, so filtering to IsCurrent would orphan fact rows.
       IsCurrentVersion is exposed for "as-is" slicing instead.
     - vPbiDate EXCLUDES the -1 member: Power BI's "mark as date table"
       requires a gap-free, non-null date column. Facts keyed to -1 surface
       under the automatic (Blank) member — the standard, documented behavior.
     - Other dimensions KEEP the -1 member so unresolved facts show as an
       explicit 'Unknown' bucket rather than silently vanishing.
     - The event-grain fact (80M rows/day at target scale) is imported only as
       daily aggregates (vPbiTrafficDaily / vPbiSensorDaily) — the Kimball
       aggregate-table pattern; hourly and incident grains import fully.
   ========================================================================== */
USE TrafficDW;
GO

/* ============================================================================
   DIMENSION VIEWS
   ========================================================================== */

/* ---- Date (mark as date table in Power BI; -1 excluded, see header) ------ */
CREATE OR ALTER VIEW mart.vPbiDate AS
SELECT DateKey,
       FullDate       AS [Date],
       [Year],
       CONCAT('Q', [Quarter]) AS [Quarter],
       [MonthNumber],
       MonthName      AS [Month],
       YearMonth,
       WeekOfYear,
       DayOfMonth,
       DayOfWeek      AS DayOfWeekNumber,   -- 1 = Monday (ISO): sort key for Day
       DayName        AS [Day],
       IsWeekend,
       IsHoliday,
       HolidayName,
       Season
FROM dim.DimDate
WHERE DateKey <> -1;
GO

/* ---- Hour (24 rows) — conforms FactHourlyTraffic.HourOfDay --------------- */
CREATE OR ALTER VIEW mart.vPbiHour AS
SELECT [Hour]      AS HourOfDay,
       TimeBK      AS HourLabel,       -- '07:00'
       HourBand,
       DayPart,
       IsRushHour
FROM dim.DimTime
WHERE [Minute] = 0 AND TimeKey >= 0;
GO

/* ---- Time of day (1,440 rows, minute grain) — incident milestone times --- */
CREATE OR ALTER VIEW mart.vPbiTimeOfDay AS
SELECT TimeKey,
       TimeBK     AS [Time],
       [Hour],
       [Minute],
       HourBand,
       DayPart,
       IsRushHour
FROM dim.DimTime;
GO

/* ---- Road segment (all SCD2 versions + as-is flag) ------------------------ */
CREATE OR ALTER VIEW mart.vPbiRoadSegment AS
SELECT RoadSegmentKey,
       SegmentCode,
       RoadCode,
       RoadName,
       RoadCategory,
       City,
       District,
       Direction,
       StartIntersection,
       EndIntersection,
       Latitude,
       Longitude,
       LengthM,
       LaneCount,
       SpeedLimitKmh,                        -- limit of THIS version (as-was)
       CurrentSpeedLimitKmh,                 -- today's limit on every version (Type 6)
       IsCurrent      AS IsCurrentVersion,
       VersionNumber,
       IsInferred
FROM dim.DimRoadSegment;
GO

/* ---- Sensor (all SCD2 versions) ------------------------------------------ */
CREATE OR ALTER VIEW mart.vPbiSensor AS
SELECT SensorKey,
       SerialNumber,
       SensorTypeName,
       Technology,
       SegmentCode,
       StatusClass,
       InstallDate,
       IsCurrent      AS IsCurrentVersion,
       VersionNumber,
       IsInferred
FROM dim.DimSensor;
GO

/* ---- Remaining dimensions ------------------------------------------------- */
CREATE OR ALTER VIEW mart.vPbiWeather AS
SELECT WeatherKey, ConditionCode, ConditionName, TempBand, PrecipBand, VisibilityBand, IsSevere
FROM dim.DimWeatherCondition;
GO

CREATE OR ALTER VIEW mart.vPbiVehicleType AS
SELECT VehicleTypeKey, TypeCode, TypeName, Category, IsHeavy
FROM dim.DimVehicleType;
GO

CREATE OR ALTER VIEW mart.vPbiIncidentType AS
SELECT IncidentTypeKey, TypeCode, TypeName, Category, DefaultSeverity
FROM dim.DimIncidentType;
GO

CREATE OR ALTER VIEW mart.vPbiEmergencyUnit AS
SELECT EmergencyUnitKey, UnitCode, UnitType, HomeStation, VehicleMake, VehicleModel
FROM dim.DimEmergencyUnit;
GO

CREATE OR ALTER VIEW mart.vPbiTrafficLight AS
SELECT TrafficLightKey, ControllerCode, IntersectionName,
       CurrentTimingPlan, PreviousTimingPlan, TimingPlanChangeDate,   -- SCD3 pair → retiming before/after
       CycleSeconds, Status
FROM dim.DimTrafficLight;
GO

/* ============================================================================
   FACT VIEWS
   ========================================================================== */

/* ---- Hourly traffic snapshot (imported at native grain) ------------------- */
CREATE OR ALTER VIEW mart.vPbiHourlyTraffic AS
SELECT DateKey,
       HourOfDay,
       RoadSegmentKey,
       WeatherKey,
       VehicleCount,
       HeavyVehicleCount,
       IncidentCount,
       AvgSpeedKmh,
       P85SpeedKmh,
       AvgOccupancyPct,
       CongestionIndex,
       AvgTempC,
       PrecipitationMm
FROM fact.FactHourlyTraffic;
GO

/* ---- Incident lifecycle (native grain + SLA convenience flags) ------------ */
CREATE OR ALTER VIEW mart.vPbiIncidents AS
SELECT IncidentKey,
       IncidentNumber,
       IncidentTypeKey,
       RoadSegmentKey,
       EmergencyUnitKey,
       WeatherKey,
       DetectedDateKey, DetectedTimeKey,
       DispatchedDateKey, DispatchedTimeKey,
       ArrivedDateKey, ArrivedTimeKey,
       ClearedDateKey, ClearedTimeKey,
       ClosedDateKey, ClosedTimeKey,
       CurrentStatus,
       Severity,
       LanesBlocked,
       MinutesToDispatch,
       MinutesToArrive,
       MinutesToClear,
       TotalDurationMinutes,
       CASE WHEN MinutesToArrive IS NULL THEN NULL
            WHEN MinutesToArrive <= 8 THEN 1 ELSE 0 END AS MetResponseSla8Min,
       CASE WHEN MinutesToClear IS NULL THEN NULL
            WHEN MinutesToClear <= 45 THEN 1 ELSE 0 END AS MetClearanceSla45Min,
       CASE WHEN ClosedDateKey <> -1 THEN 1 ELSE 0 END  AS IsClosed
FROM fact.FactIncidentLifecycle;
GO

/* ---- Event fact, daily aggregate (Kimball aggregate table for import) ----- */
CREATE OR ALTER VIEW mart.vPbiTrafficDaily AS
SELECT DateKey,
       RoadSegmentKey,
       VehicleTypeKey,
       WeatherKey,
       COUNT_BIG(*)                                          AS Detections,
       CAST(AVG(SpeedKmh)        AS DECIMAL(5,1))            AS AvgSpeedKmh,
       CAST(MAX(SpeedKmh)        AS DECIMAL(5,1))            AS MaxSpeedKmh,
       SUM(CASE WHEN SpeedOverLimitKmh > 0 THEN 1 ELSE 0 END) AS SpeedingDetections,
       CAST(AVG(OccupancyPct)    AS DECIMAL(5,2))            AS AvgOccupancyPct,
       CAST(AVG(HeadwaySeconds)  AS DECIMAL(6,2))            AS AvgHeadwaySeconds
FROM fact.FactTrafficEvent
GROUP BY DateKey, RoadSegmentKey, VehicleTypeKey, WeatherKey;
GO

/* ---- Sensor performance, daily grain (Sensor Performance dashboard) ------- */
CREATE OR ALTER VIEW mart.vPbiSensorDaily AS
SELECT f.DateKey,
       f.SensorKey,
       COUNT_BIG(*)                                          AS Detections,
       COUNT(DISTINCT CASE WHEN f.TimeKey >= 0
                           THEN f.TimeKey / 60 END)          AS ActiveHours,
       CAST(COUNT(DISTINCT CASE WHEN f.TimeKey >= 0
                                THEN f.TimeKey / 60 END)
            * 100.0 / 24 AS DECIMAL(5,1))                    AS UptimePct,   -- hours reporting / 24
       CAST(AVG(f.SpeedKmh) AS DECIMAL(5,1))                 AS AvgSpeedKmh,
       SUM(CASE WHEN f.SpeedOverLimitKmh > 0 THEN 1 ELSE 0 END) AS SpeedingDetections
FROM fact.FactTrafficEvent f
WHERE f.DetectorType = 'SENSOR'    -- camera detections carry SensorKey = -1 by design
GROUP BY f.DateKey, f.SensorKey;
GO

/* ---- Camera performance, daily grain (the mirror of vPbiSensorDaily) ------ */
CREATE OR ALTER VIEW mart.vPbiCameraDaily AS
SELECT f.DateKey,
       f.CameraKey,
       COUNT_BIG(*)                                          AS Detections,
       COUNT(DISTINCT CASE WHEN f.TimeKey >= 0
                           THEN f.TimeKey / 60 END)          AS ActiveHours,
       CAST(AVG(f.SpeedKmh) AS DECIMAL(5,1))                 AS AvgSpeedKmh,
       SUM(CASE WHEN f.SpeedOverLimitKmh > 0 THEN 1 ELSE 0 END) AS SpeedingDetections
FROM fact.FactTrafficEvent f
WHERE f.DetectorType = 'CAMERA'
GROUP BY f.DateKey, f.CameraKey;
GO

/* ---- Traffic camera dimension (now referenced by the transaction fact) ---- */
CREATE OR ALTER VIEW mart.vPbiTrafficCamera AS
SELECT CameraKey, CameraCode, IntersectionName, Model, Resolution, FirmwareVersion, Status
FROM dim.DimTrafficCamera;
GO

/* ---- Executive daily KPI scorecard (Executive dashboard cards/trends) ----- */
CREATE OR ALTER VIEW mart.vPbiKpiDaily AS
WITH traffic AS (
    SELECT DateKey,
           SUM(VehicleCount)      AS TotalVehicles,
           SUM(HeavyVehicleCount) AS HeavyVehicles,
           CAST(SUM(AvgSpeedKmh     * VehicleCount) / NULLIF(SUM(VehicleCount), 0) AS DECIMAL(5,1)) AS NetworkAvgSpeedKmh,
           CAST(SUM(CongestionIndex * VehicleCount) / NULLIF(SUM(VehicleCount), 0) AS DECIMAL(4,3)) AS NetworkCongestionIndex,
           COUNT(DISTINCT CASE WHEN VehicleCount > 0 THEN RoadSegmentKey END)                       AS SegmentsWithTraffic
    FROM fact.FactHourlyTraffic
    GROUP BY DateKey
),
incidents AS (
    SELECT DetectedDateKey AS DateKey,
           COUNT(*) AS Incidents,
           CAST(AVG(CASE WHEN MinutesToArrive <= 8 THEN 100.0
                         WHEN MinutesToArrive IS NOT NULL THEN 0.0 END) AS DECIMAL(5,1)) AS ResponseSla8Pct,
           AVG(MinutesToClear) AS AvgClearanceMin
    FROM fact.FactIncidentLifecycle
    GROUP BY DetectedDateKey
),
response_pcts AS (
    SELECT DISTINCT DetectedDateKey AS DateKey,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY MinutesToArrive)
               OVER (PARTITION BY DetectedDateKey) AS MedianResponseMin,
           PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY MinutesToArrive)
               OVER (PARTITION BY DetectedDateKey) AS P90ResponseMin
    FROM fact.FactIncidentLifecycle
    WHERE MinutesToArrive IS NOT NULL
),
sensors AS (
    SELECT DateKey, COUNT(DISTINCT SensorKey) AS ReportingSensors
    FROM fact.FactTrafficEvent
    WHERE SensorKey <> -1
    GROUP BY DateKey
)
SELECT t.DateKey,
       t.TotalVehicles,
       t.HeavyVehicles,
       t.NetworkAvgSpeedKmh,
       t.NetworkCongestionIndex,
       t.SegmentsWithTraffic,
       ISNULL(i.Incidents, 0)   AS Incidents,
       i.ResponseSla8Pct,
       r.MedianResponseMin,
       r.P90ResponseMin,
       i.AvgClearanceMin,
       s.ReportingSensors,
       CAST(s.ReportingSensors * 100.0 /
            NULLIF((SELECT COUNT(*) FROM dim.DimSensor WHERE IsCurrent = 1 AND SensorKey <> -1), 0)
            AS DECIMAL(5,1))    AS SensorHealthPct
FROM traffic t
LEFT JOIN incidents     i ON i.DateKey = t.DateKey
LEFT JOIN response_pcts r ON r.DateKey = t.DateKey
LEFT JOIN sensors       s ON s.DateKey = t.DateKey;
GO

/* ============================================================================
   OPERATIONAL / DATA-QUALITY VIEWS (Sensor Performance dashboard, page 2)
   Sourced from the ETL framework's audit trail — still warehouse-side only.
   ========================================================================== */

/* ---- Quarantined rows by day / reason / sensor ---------------------------- */
CREATE OR ALTER VIEW mart.vPbiRejectDaily AS
SELECT ISNULL(YEAR(r.EventTimestamp) * 10000
            + MONTH(r.EventTimestamp) * 100
            + DAY(r.EventTimestamp), -1)   AS DateKey,
       r.RejectReason,
       r.SensorSerial,
       COUNT_BIG(*)                        AS RejectedRows
FROM stg.RejectTrafficEvent r
GROUP BY ISNULL(YEAR(r.EventTimestamp) * 10000
              + MONTH(r.EventTimestamp) * 100
              + DAY(r.EventTimestamp), -1),
         r.RejectReason, r.SensorSerial;
GO

/* ---- ETL batch health ------------------------------------------------------ */
CREATE OR ALTER VIEW mart.vPbiEtlBatches AS
SELECT b.ETLBatchID,
       b.PipelineName,
       b.LoadDate,
       YEAR(b.LoadDate) * 10000 + MONTH(b.LoadDate) * 100 + DAY(b.LoadDate) AS DateKey,
       b.Status,
       b.StartedAt,
       b.FinishedAt,
       DATEDIFF(SECOND, b.StartedAt, ISNULL(b.FinishedAt, b.StartedAt)) AS DurationSec,
       ISNULL(SUM(r.RowsExtracted), 0) AS RowsExtracted,
       ISNULL(SUM(r.RowsInserted), 0)  AS RowsInserted,
       ISNULL(SUM(r.RowsUpdated), 0)   AS RowsUpdated,
       ISNULL(SUM(r.RowsRejected), 0)  AS RowsRejected
FROM etl.BatchLog b
LEFT JOIN etl.RowLog r ON r.ETLBatchID = b.ETLBatchID
GROUP BY b.ETLBatchID, b.PipelineName, b.LoadDate, b.Status, b.StartedAt, b.FinishedAt;
GO

/* ---- Quality-gate results (post-load validation, docs/14) ------------------ */
CREATE OR ALTER VIEW mart.vPbiQualityChecks AS
SELECT q.QualityCheckLogID,
       q.ETLBatchID,
       YEAR(b.LoadDate) * 10000 + MONTH(b.LoadDate) * 100 + DAY(b.LoadDate) AS DateKey,
       q.CheckName,
       q.Category,
       q.TargetTable,
       q.Severity,
       q.Status,
       q.FailedRows,
       q.DurationMs,
       q.CheckedAt
FROM etl.QualityCheckLog q
LEFT JOIN etl.BatchLog b ON b.ETLBatchID = q.ETLBatchID;
GO

PRINT 'Power BI semantic views created (mart.vPbi*).';
GO

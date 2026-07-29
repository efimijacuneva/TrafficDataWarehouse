/* ============================================================================
   TrafficDW — post-load DATA QUALITY FRAMEWORK (docs/14_quality_framework.md)

   Catalog-driven: every validation is a row in etl.QualityCheckCatalog with a
   scalar CountQuery returning the number of FAILED rows (0 = pass). The runner
   etl.usp_RunQualityChecks executes all enabled checks, times each one, and
   logs results to etl.QualityCheckLog.

   Contract (mirrors the ETL framework conventions of 02_etl_framework.sql):
     - checks never mutate data — read-only against dim/fact/stg/etl
     - a broken check (SQL error) is itself logged as a failure, never swallowed
     - severity 'Error'  → etl.usp_AssertQuality THROWs (orchestrator gate)
       severity 'Warning'→ logged and reported, never blocks the pipeline
     - checks may reference @ETLBatchID / @LoadDate / @DateKey; high-volume
       fact checks are scoped to the loaded date (validate what you loaded)

   Execution points:
     1. etl.usp_RunNightlyPipeline calls the runner log-only after each load
     2. the Airflow DAG's quality task calls etl.usp_AssertQuality (-b gate)
     3. ad hoc: EXEC etl.usp_RunQualityChecks @LoadDate = '2026-06-15'
   ========================================================================== */
USE TrafficDW;
GO

/* ------------------------------------------------------------ result log --- */
IF OBJECT_ID('etl.QualityCheckLog') IS NULL
CREATE TABLE etl.QualityCheckLog (
    QualityCheckLogID INT IDENTITY(1,1) CONSTRAINT PK_QualityCheckLog PRIMARY KEY,
    ETLBatchID   INT          NULL,           -- NULL = ad-hoc run
    CheckName    VARCHAR(100) NOT NULL,
    Category     VARCHAR(40)  NOT NULL,
    TargetTable  VARCHAR(200) NOT NULL,
    Severity     VARCHAR(10)  NOT NULL,       -- Error / Warning
    Status       VARCHAR(4)   NOT NULL,       -- Pass / Fail
    FailedRows   INT          NOT NULL,       -- -1 = the check itself errored
    DurationMs   INT          NOT NULL,
    CheckedAt    DATETIME2(3) NOT NULL CONSTRAINT DF_QCL_At DEFAULT SYSUTCDATETIME()
);
GO

/* ------------------------------------------------------------ rule catalog -- */
IF OBJECT_ID('etl.QualityCheckCatalog') IS NULL
CREATE TABLE etl.QualityCheckCatalog (
    CheckName   VARCHAR(100)  NOT NULL CONSTRAINT PK_QualityCheckCatalog PRIMARY KEY,
    Category    VARCHAR(40)   NOT NULL,   -- Integrity / Uniqueness / SCD / Validity / Completeness / Reconciliation
    TargetTable VARCHAR(200)  NOT NULL,
    Severity    VARCHAR(10)   NOT NULL CONSTRAINT CK_QCC_Sev CHECK (Severity IN ('Error','Warning')),
    Rationale   NVARCHAR(400) NOT NULL,   -- WHY the rule exists (see docs/14)
    CountQuery  NVARCHAR(MAX) NOT NULL,   -- scalar SELECT → failed-row count; params: @ETLBatchID, @LoadDate, @DateKey
    IsEnabled   BIT           NOT NULL CONSTRAINT DF_QCC_On DEFAULT 1,
    SortOrder   SMALLINT      NOT NULL CONSTRAINT DF_QCC_Sort DEFAULT 100
);
GO

/* The catalog is code, not data: redeploying this script resets it. */
DELETE FROM etl.QualityCheckCatalog;
GO

/* ============================================================================
   1. INTEGRITY — orphan surrogate keys & constraint health.
   FKs are enforced, so orphans "cannot happen" — until someone bulk-loads with
   constraints disabled or switches a partition in. These checks are the
   independent proof the constraints still hold (and stay trusted).
   ========================================================================== */
INSERT INTO etl.QualityCheckCatalog (CheckName, Category, TargetTable, Severity, Rationale, CountQuery, SortOrder) VALUES
('ORPHAN_FTE_DATE', 'Integrity', 'fact.FactTrafficEvent', 'Error',
 N'Every fact row must resolve to a calendar row; an orphan DateKey silently drops rows from every date-joined report.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE f.DateKey = @DateKey AND NOT EXISTS (SELECT 1 FROM dim.DimDate d WHERE d.DateKey = f.DateKey)', 10),
('ORPHAN_FTE_TIME', 'Integrity', 'fact.FactTrafficEvent', 'Error',
 N'TimeKey is computed arithmetically during load (hour*60+minute); a bug there produces keys outside 0..1439 that no constraint of DimTime''s seeding would catch.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE f.DateKey = @DateKey AND NOT EXISTS (SELECT 1 FROM dim.DimTime t WHERE t.TimeKey = f.TimeKey)', 11),
('ORPHAN_FTE_SEGMENT', 'Integrity', 'fact.FactTrafficEvent', 'Error',
 N'Segment is the central conformed dimension; an unresolvable key here breaks every corridor/district report.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE f.DateKey = @DateKey AND NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment d WHERE d.RoadSegmentKey = f.RoadSegmentKey)', 12),
('ORPHAN_FTE_SENSOR', 'Integrity', 'fact.FactTrafficEvent', 'Error',
 N'Sensor keys come from point-in-time SCD2 lookups; a wrong validity join produces keys of expired versions that were deleted or never existed.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE f.DateKey = @DateKey AND NOT EXISTS (SELECT 1 FROM dim.DimSensor d WHERE d.SensorKey = f.SensorKey)', 13),
('ORPHAN_FTE_VEHICLETYPE', 'Integrity', 'fact.FactTrafficEvent', 'Error',
 N'Vehicle-type lookups fall back to the unknown member (-1); any other unresolved value means the fallback logic regressed.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE f.DateKey = @DateKey AND NOT EXISTS (SELECT 1 FROM dim.DimVehicleType d WHERE d.VehicleTypeKey = f.VehicleTypeKey)', 14),
('ORPHAN_FTE_WEATHER', 'Integrity', 'fact.FactTrafficEvent', 'Error',
 N'Weather keys are resolved through the banding function; a banding mismatch between Spark and SQL surfaces here first.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE f.DateKey = @DateKey AND NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition d WHERE d.WeatherKey = f.WeatherKey)', 15),
('ORPHAN_FHT_DATE', 'Integrity', 'fact.FactHourlyTraffic', 'Error',
 N'The snapshot is replaced per load date; a bad DateKey written by any load sits outside every reload window and persists silently.',
 N'SELECT COUNT(*) FROM fact.FactHourlyTraffic f WHERE NOT EXISTS (SELECT 1 FROM dim.DimDate d WHERE d.DateKey = f.DateKey)', 20),
('ORPHAN_FHT_SEGMENT', 'Integrity', 'fact.FactHourlyTraffic', 'Error',
 N'The density spine is built from current dimension rows; a segment deleted from the dimension would strand its historical snapshot rows.',
 N'SELECT COUNT(*) FROM fact.FactHourlyTraffic f WHERE NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment d WHERE d.RoadSegmentKey = f.RoadSegmentKey)', 21),
('ORPHAN_FHT_WEATHER', 'Integrity', 'fact.FactHourlyTraffic', 'Error',
 N'Zero-traffic spine rows take WeatherKey -1 by design; anything else unresolved is a load defect.',
 N'SELECT COUNT(*) FROM fact.FactHourlyTraffic f WHERE NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition d WHERE d.WeatherKey = f.WeatherKey)', 22),
('ORPHAN_FIL_DIMS', 'Integrity', 'fact.FactIncidentLifecycle', 'Error',
 N'The accumulating fact is UPDATEd in place; a bad key introduced by one milestone update corrupts the whole incident row, not just one event.',
 N'SELECT COUNT(*) FROM fact.FactIncidentLifecycle f
   WHERE NOT EXISTS (SELECT 1 FROM dim.DimIncidentType  d WHERE d.IncidentTypeKey  = f.IncidentTypeKey)
      OR NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment   d WHERE d.RoadSegmentKey   = f.RoadSegmentKey)
      OR NOT EXISTS (SELECT 1 FROM dim.DimEmergencyUnit d WHERE d.EmergencyUnitKey = f.EmergencyUnitKey)
      OR NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition d WHERE d.WeatherKey    = f.WeatherKey)', 30),
('ORPHAN_FIL_MILESTONE_DATES', 'Integrity', 'fact.FactIncidentLifecycle', 'Error',
 N'Five role-playing date keys are computed per milestone; each must be a real calendar day or the reserved -1 (= not yet reached).',
 N'SELECT COUNT(*) FROM fact.FactIncidentLifecycle f
   WHERE NOT EXISTS (SELECT 1 FROM dim.DimDate d WHERE d.DateKey = f.DetectedDateKey)
      OR NOT EXISTS (SELECT 1 FROM dim.DimDate d WHERE d.DateKey = f.DispatchedDateKey)
      OR NOT EXISTS (SELECT 1 FROM dim.DimDate d WHERE d.DateKey = f.ArrivedDateKey)
      OR NOT EXISTS (SELECT 1 FROM dim.DimDate d WHERE d.DateKey = f.ClearedDateKey)
      OR NOT EXISTS (SELECT 1 FROM dim.DimDate d WHERE d.DateKey = f.ClosedDateKey)', 31),
('FK_CONSTRAINTS_TRUSTED', 'Integrity', 'sys.foreign_keys (TrafficDW)', 'Error',
 N'Bulk loads sometimes disable FKs and re-enable them WITH NOCHECK, leaving them untrusted — the optimizer stops using them and violations can hide inside. Referential integrity must be provably intact.',
 N'SELECT COUNT(*) FROM sys.foreign_keys WHERE is_not_trusted = 1 OR is_disabled = 1', 32);
GO

/* ============================================================================
   2. UNIQUENESS — duplicate business keys and duplicate facts.
   FactTrafficEvent is a clustered columnstore WITHOUT a unique constraint
   (deliberate, docs/09) → the duplicate-fact check is the ONLY guard there.
   ========================================================================== */
INSERT INTO etl.QualityCheckCatalog (CheckName, Category, TargetTable, Severity, Rationale, CountQuery, SortOrder) VALUES
('DUP_BK_TYPE1_DIMS', 'Uniqueness', 'dim.* (Type-1 dimensions)', 'Error',
 N'Type-1 dimensions must hold exactly one row per business key — a duplicate BK doubles every fact joined through it. Unique indexes enforce this; the check proves they were not dropped/disabled.',
 N'SELECT (SELECT COUNT(*) FROM (SELECT TypeCode   FROM dim.DimVehicleType   WHERE VehicleTypeKey   <> -1 GROUP BY TypeCode   HAVING COUNT(*) > 1) a)
       + (SELECT COUNT(*) FROM (SELECT CameraCode  FROM dim.DimTrafficCamera WHERE CameraKey        <> -1 GROUP BY CameraCode  HAVING COUNT(*) > 1) b)
       + (SELECT COUNT(*) FROM (SELECT TypeCode    FROM dim.DimIncidentType  WHERE IncidentTypeKey  <> -1 GROUP BY TypeCode    HAVING COUNT(*) > 1) c)
       + (SELECT COUNT(*) FROM (SELECT UnitCode    FROM dim.DimEmergencyUnit WHERE EmergencyUnitKey <> -1 GROUP BY UnitCode    HAVING COUNT(*) > 1) d)
       + (SELECT COUNT(*) FROM (SELECT ControllerCode FROM dim.DimTrafficLight WHERE TrafficLightKey <> -1 GROUP BY ControllerCode HAVING COUNT(*) > 1) e)', 40),
('DUP_CURRENT_DIMROADSEGMENT', 'SCD', 'dim.DimRoadSegment', 'Error',
 N'SCD2 invariant: exactly one IsCurrent = 1 row per business key. Two current rows double-count facts joined on the current filter; the filtered unique index enforces it, this check proves it.',
 N'SELECT COUNT(*) FROM (SELECT SegmentCode FROM dim.DimRoadSegment WHERE IsCurrent = 1 AND RoadSegmentKey <> -1 GROUP BY SegmentCode HAVING COUNT(*) > 1) q', 41),
('DUP_CURRENT_DIMSENSOR', 'SCD', 'dim.DimSensor', 'Error',
 N'Same SCD2 single-current invariant as DimRoadSegment, for the sensor dimension.',
 N'SELECT COUNT(*) FROM (SELECT SerialNumber FROM dim.DimSensor WHERE IsCurrent = 1 AND SensorKey <> -1 GROUP BY SerialNumber HAVING COUNT(*) > 1) q', 42),
('DUP_FACT_TRAFFICEVENT', 'Uniqueness', 'fact.FactTrafficEvent', 'Error',
 N'The columnstore fact has no unique constraint by design (docs/09) — this check is the ONLY duplicate guard. A duplicated EventID means the batch delete-then-insert idempotency failed.',
 N'SELECT COUNT(*) FROM (SELECT EventID FROM fact.FactTrafficEvent WHERE DateKey = @DateKey GROUP BY EventID HAVING COUNT(*) > 1) q', 43);
GO

/* ============================================================================
   3. SCD — validity-interval sanity for the Type-2 dimensions.
   ========================================================================== */
INSERT INTO etl.QualityCheckCatalog (CheckName, Category, TargetTable, Severity, Rationale, CountQuery, SortOrder) VALUES
('SCD2_OVERLAP_DIMROADSEGMENT', 'SCD', 'dim.DimRoadSegment', 'Error',
 N'Overlapping validity intervals make point-in-time fact lookups ambiguous: one event date would match two versions and double-insert fact rows.',
 N'SELECT COUNT(*) FROM dim.DimRoadSegment a
   JOIN dim.DimRoadSegment b ON b.SegmentCode = a.SegmentCode AND b.RoadSegmentKey > a.RoadSegmentKey
   WHERE a.RoadSegmentKey <> -1 AND a.EffectiveDate <= b.ExpirationDate AND b.EffectiveDate <= a.ExpirationDate', 50),
('SCD2_OVERLAP_DIMSENSOR', 'SCD', 'dim.DimSensor', 'Error',
 N'Same ambiguity risk as segment overlaps, for the sensor dimension.',
 N'SELECT COUNT(*) FROM dim.DimSensor a
   JOIN dim.DimSensor b ON b.SerialNumber = a.SerialNumber AND b.SensorKey > a.SensorKey
   WHERE a.SensorKey <> -1 AND a.EffectiveDate <= b.ExpirationDate AND b.EffectiveDate <= a.ExpirationDate', 51),
('SCD2_NOCURRENT_DIMROADSEGMENT', 'SCD', 'dim.DimRoadSegment', 'Error',
 N'A business key whose every version is expired has fallen out of "as-is" reporting entirely — usually a MERGE that expired the old row but failed before inserting the new version.',
 N'SELECT COUNT(*) FROM (SELECT SegmentCode FROM dim.DimRoadSegment WHERE RoadSegmentKey <> -1 GROUP BY SegmentCode HAVING SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) = 0) q', 52),
('SCD2_NOCURRENT_DIMSENSOR', 'SCD', 'dim.DimSensor', 'Error',
 N'Same expire-without-reinsert failure mode as DimRoadSegment, for sensors.',
 N'SELECT COUNT(*) FROM (SELECT SerialNumber FROM dim.DimSensor WHERE SensorKey <> -1 GROUP BY SerialNumber HAVING SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) = 0) q', 53);
GO

/* ============================================================================
   4. VALIDITY — measure ranges and business rules.
   Some duplicate CHECK constraints on purpose: the constraint prevents, the
   check PROVES (and keeps protecting if a constraint is dropped in a
   performance experiment and never restored).
   ========================================================================== */
INSERT INTO etl.QualityCheckCatalog (CheckName, Category, TargetTable, Severity, Rationale, CountQuery, SortOrder) VALUES
('NEGATIVE_COUNTS_FHT', 'Validity', 'fact.FactHourlyTraffic', 'Error',
 N'Traffic counts are physical quantities: negative counts, or more heavy vehicles than vehicles, mean the Spark aggregation or the reload arithmetic broke.',
 N'SELECT COUNT(*) FROM fact.FactHourlyTraffic WHERE VehicleCount < 0 OR HeavyVehicleCount < 0 OR IncidentCount < 0 OR HeavyVehicleCount > VehicleCount', 60),
('SPEED_RANGE_FTE', 'Validity', 'fact.FactTrafficEvent', 'Error',
 N'Speeds outside (0, 250] km/h should have been quarantined at the silver gate AND at staging validation; one appearing in the fact means both gates were bypassed.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent WHERE DateKey = @DateKey AND SpeedKmh IS NOT NULL AND (SpeedKmh <= 0 OR SpeedKmh > 250)', 61),
('SPEED_VS_LIMIT_FHT', 'Validity', 'fact.FactHourlyTraffic', 'Warning',
 N'Hourly average speed above 1.5x the segment limit is physically implausible (docs/10 invariant) — usually a mis-calibrated sensor or a unit error, worth investigating but not blocking.',
 N'SELECT COUNT(*) FROM fact.FactHourlyTraffic f JOIN dim.DimRoadSegment s ON s.RoadSegmentKey = f.RoadSegmentKey
   WHERE f.AvgSpeedKmh IS NOT NULL AND s.SpeedLimitKmh > 0 AND f.AvgSpeedKmh > 1.5 * s.SpeedLimitKmh', 62),
('CONGESTION_RANGE_FHT', 'Validity', 'fact.FactHourlyTraffic', 'Error',
 N'CongestionIndex is defined on [0,1]; out-of-range values poison every averaged KPI downstream.',
 N'SELECT COUNT(*) FROM fact.FactHourlyTraffic WHERE CongestionIndex IS NOT NULL AND (CongestionIndex < 0 OR CongestionIndex > 1)', 63),
('FUTURE_DATE_FTE', 'Validity', 'fact.FactTrafficEvent', 'Error',
 N'Future-dated events escaped both the silver FUTURE_TIMESTAMP rule and the staging DATE_MISMATCH rule — they inflate "today" dashboards with data that has not happened.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent WHERE DateKey > CONVERT(INT, FORMAT(SYSUTCDATETIME(), ''yyyyMMdd''))', 64),
('MILESTONE_ORDER_FIL', 'Validity', 'fact.FactIncidentLifecycle', 'Error',
 N'Accumulating-snapshot invariant: Detected <= Dispatched <= Arrived <= Cleared <= Closed for every populated milestone. Violations produce negative lags and nonsense SLA percentiles.',
 N'SELECT COUNT(*) FROM fact.FactIncidentLifecycle f
   WHERE (f.DispatchedDateKey <> -1 AND CAST(f.DispatchedDateKey AS BIGINT) * 1440 + f.DispatchedTimeKey < CAST(f.DetectedDateKey AS BIGINT) * 1440 + f.DetectedTimeKey)
      OR (f.ArrivedDateKey  <> -1 AND f.DispatchedDateKey <> -1 AND CAST(f.ArrivedDateKey  AS BIGINT) * 1440 + f.ArrivedTimeKey  < CAST(f.DispatchedDateKey AS BIGINT) * 1440 + f.DispatchedTimeKey)
      OR (f.ClearedDateKey  <> -1 AND f.ArrivedDateKey   <> -1 AND CAST(f.ClearedDateKey  AS BIGINT) * 1440 + f.ClearedTimeKey  < CAST(f.ArrivedDateKey   AS BIGINT) * 1440 + f.ArrivedTimeKey)
      OR (f.ClosedDateKey   <> -1 AND f.ClearedDateKey   <> -1 AND CAST(f.ClosedDateKey   AS BIGINT) * 1440 + f.ClosedTimeKey   < CAST(f.ClearedDateKey   AS BIGINT) * 1440 + f.ClearedTimeKey)', 65),
('NEGATIVE_LAGS_FIL', 'Validity', 'fact.FactIncidentLifecycle', 'Error',
 N'Lag measures are stored physically for SLA reporting; a negative lag means the DATEDIFF derivation and the milestone keys disagree.',
 N'SELECT COUNT(*) FROM fact.FactIncidentLifecycle
   WHERE MinutesToDispatch < 0 OR MinutesToArrive < 0 OR MinutesToClear < 0 OR TotalDurationMinutes < 0', 66);
GO

/* ============================================================================
   5. COMPLETENESS — mandatory members, mandatory dimensions, snapshot density.
   ========================================================================== */
INSERT INTO etl.QualityCheckCatalog (CheckName, Category, TargetTable, Severity, Rationale, CountQuery, SortOrder) VALUES
('UNKNOWN_MEMBER_MISSING', 'Completeness', 'dim.* (all dimensions)', 'Error',
 N'Every dimension must carry the reserved Unknown member (-1); without it the fact loads'' ISNULL(key, -1) fallback violates FKs and the batch dies mid-load.',
 N'SELECT (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimDate             WHERE DateKey          = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimTime             WHERE TimeKey          = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment      WHERE RoadSegmentKey   = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimSensor           WHERE SensorKey        = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimVehicleType      WHERE VehicleTypeKey   = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition WHERE WeatherKey       = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimTrafficCamera    WHERE CameraKey        = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimTrafficLight     WHERE TrafficLightKey  = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimIncidentType     WHERE IncidentTypeKey  = -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimEmergencyUnit    WHERE EmergencyUnitKey = -1) THEN 1 ELSE 0 END)', 70),
('MANDATORY_DIMS_POPULATED', 'Completeness', 'dim.* (core dimensions)', 'Error',
 N'A fact load against an empty core dimension maps every row to Unknown and the warehouse "works" while being analytically useless. Fails when a mandatory dimension holds no real (non -1) members.',
 N'SELECT (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimDate             WHERE DateKey          <> -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimTime             WHERE TimeKey          <> -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment      WHERE RoadSegmentKey   <> -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimSensor           WHERE SensorKey        <> -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimVehicleType      WHERE VehicleTypeKey   <> -1) THEN 1 ELSE 0 END)
       + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition WHERE WeatherKey       <> -1) THEN 1 ELSE 0 END)', 71),
('SNAPSHOT_DENSITY_FHT', 'Completeness', 'fact.FactHourlyTraffic', 'Warning',
 N'Periodic-snapshot contract: current segments x 24 rows per loaded day, including zero-traffic hours. A mismatch usually means the current segment set changed after the load (new/retired business keys). Warning because it presumes the pipeline ran for @LoadDate.',
 N'SELECT ABS((SELECT COUNT(*) FROM dim.DimRoadSegment WHERE IsCurrent = 1 AND RoadSegmentKey <> -1) * 24
            - (SELECT COUNT(*) FROM fact.FactHourlyTraffic WHERE DateKey = @DateKey))', 72),
('UNKNOWN_RATE_FTE', 'Completeness', 'fact.FactTrafficEvent', 'Warning',
 N'Rows keyed to Unknown members are legal (that is the -1 design) but each one is analytically blind; a spike signals upstream master-data gaps or a broken lookup. Monitoring signal, not a gate.',
 N'SELECT COUNT(*) FROM fact.FactTrafficEvent
   WHERE DateKey = @DateKey AND (RoadSegmentKey = -1 OR SensorKey = -1 OR VehicleTypeKey = -1 OR WeatherKey = -1)', 73);
GO

/* ============================================================================
   6. RECONCILIATION — no row unaccounted for between staging and warehouse.
   ========================================================================== */
INSERT INTO etl.QualityCheckCatalog (CheckName, Category, TargetTable, Severity, Rationale, CountQuery, SortOrder) VALUES
('RECON_ROWLOG_BATCH', 'Reconciliation', 'etl.RowLog', 'Error',
 N'The framework contract (docs/06): extracted = inserted + rejected on the fully-instrumented fact step. A mismatch is silent data loss or double-counting inside the batch. (SCD steps are exempt: unchanged rows are legitimate no-ops there.)',
 N'SELECT COUNT(*) FROM etl.RowLog
   WHERE ETLBatchID = @ETLBatchID AND StepName = ''Fact.TrafficEvent''
     AND RowsExtracted IS NOT NULL
     AND RowsExtracted <> ISNULL(RowsInserted, 0) + ISNULL(RowsUpdated, 0) + ISNULL(RowsRejected, 0)', 80),
('RECON_STG_FACT_EVENTS', 'Reconciliation', 'stg.TrafficEvent -> fact.FactTrafficEvent', 'Error',
 N'Independent cross-check of the RowLog: staged rows for the load date must equal fact rows loaded plus rows quarantined in this batch. Skipped (0) when staging has already been truncated by a later run.',
 N'SELECT CASE WHEN (SELECT COUNT(*) FROM stg.TrafficEvent WHERE LoadDate = @LoadDate) = 0 THEN 0
          ELSE ABS((SELECT COUNT(*) FROM stg.TrafficEvent WHERE LoadDate = @LoadDate)
                 - (SELECT COUNT(*) FROM fact.FactTrafficEvent WHERE DateKey = @DateKey)
                 - (SELECT COUNT(*) FROM stg.RejectTrafficEvent WHERE ETLBatchID = @ETLBatchID)) END', 81),
('RECON_STG_FACT_HOURLY', 'Reconciliation', 'stg.HourlyTraffic -> fact.FactHourlyTraffic', 'Error',
 N'Every staged segment-hour aggregate must land in the snapshot (the spine only ADDS zero rows, never drops staged ones). A missing row means the reload quietly skipped data.',
 N'SELECT COUNT(*) FROM stg.HourlyTraffic s
   JOIN dim.DimRoadSegment d ON d.SegmentCode = s.SegmentCode AND d.IsCurrent = 1
   WHERE s.EventDate = @LoadDate
     AND NOT EXISTS (SELECT 1 FROM fact.FactHourlyTraffic f
                     WHERE f.DateKey = @DateKey AND f.HourOfDay = s.HourOfDay
                       AND f.RoadSegmentKey = d.RoadSegmentKey)', 82);
GO

/* ============================================================================
   RUNNER — executes every enabled check, times it, logs Pass/Fail.
   A check that itself errors is logged as Fail (FailedRows = -1) + ErrorLog.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_RunQualityChecks
    @ETLBatchID  INT  = NULL,     -- NULL = ad-hoc run (batch-scoped checks return 0)
    @LoadDate    DATE = NULL,     -- default: the batch's LoadDate, else yesterday
    @FailOnError BIT  = 1         -- THROW after the run if any Error-severity check failed
AS
BEGIN
    SET NOCOUNT ON;

    IF @LoadDate IS NULL AND @ETLBatchID IS NOT NULL
        SET @LoadDate = (SELECT LoadDate FROM etl.BatchLog WHERE ETLBatchID = @ETLBatchID);
    IF @LoadDate IS NULL
        SET @LoadDate = CAST(DATEADD(DAY, -1, SYSUTCDATETIME()) AS DATE);
    DECLARE @DateKey INT = CONVERT(INT, FORMAT(@LoadDate, 'yyyyMMdd'));

    DECLARE @CheckName VARCHAR(100), @Category VARCHAR(40), @TargetTable VARCHAR(200),
            @Severity VARCHAR(10), @CountQuery NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX), @failed INT, @t0 DATETIME2(3);

    DECLARE check_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT CheckName, Category, TargetTable, Severity, CountQuery
        FROM etl.QualityCheckCatalog
        WHERE IsEnabled = 1
        ORDER BY SortOrder, CheckName;

    OPEN check_cursor;
    FETCH NEXT FROM check_cursor INTO @CheckName, @Category, @TargetTable, @Severity, @CountQuery;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @t0 = SYSUTCDATETIME();
        SET @failed = -1;
        SET @sql = N'SELECT @cnt = ISNULL((' + @CountQuery + N'), 0)';
        BEGIN TRY
            EXEC sp_executesql @sql,
                 N'@cnt INT OUTPUT, @ETLBatchID INT, @LoadDate DATE, @DateKey INT',
                 @cnt = @failed OUTPUT, @ETLBatchID = @ETLBatchID,
                 @LoadDate = @LoadDate, @DateKey = @DateKey;
        END TRY
        BEGIN CATCH
            SET @failed = -1;                      -- the check itself is broken
            /* log the evidence directly — deliberately NOT etl.usp_LogError,
               whose EndBatch('Failed') side effect must not fire from a
               log-only quality run */
            INSERT INTO etl.ErrorLog (ETLBatchID, ProcedureName, ErrorNumber, ErrorLine, ErrorMessage)
            VALUES (@ETLBatchID, CONCAT('usp_RunQualityChecks:', @CheckName),
                    ERROR_NUMBER(), ERROR_LINE(), ERROR_MESSAGE());
        END CATCH

        INSERT INTO etl.QualityCheckLog
              (ETLBatchID, CheckName, Category, TargetTable, Severity, Status, FailedRows, DurationMs)
        VALUES (@ETLBatchID, @CheckName, @Category, @TargetTable, @Severity,
                CASE WHEN @failed = 0 THEN 'Pass' ELSE 'Fail' END,
                @failed,
                DATEDIFF(MILLISECOND, @t0, SYSUTCDATETIME()));

        FETCH NEXT FROM check_cursor INTO @CheckName, @Category, @TargetTable, @Severity, @CountQuery;
    END
    CLOSE check_cursor; DEALLOCATE check_cursor;

    IF @FailOnError = 1
        EXEC etl.usp_AssertQuality @ETLBatchID;
END
GO

/* ============================================================================
   ASSERT — the orchestrator gate. THROWs (=> sqlcmd -b exits non-zero, the
   Airflow task fails) when the most recent run of the batch has failed
   Error-severity checks. Warnings never block.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_AssertQuality
    @ETLBatchID INT = NULL       -- NULL = most recent batch that has check results
AS
BEGIN
    SET NOCOUNT ON;

    IF @ETLBatchID IS NULL
        SET @ETLBatchID = (SELECT MAX(ETLBatchID) FROM etl.QualityCheckLog);

    /* only the LATEST result per check counts — a re-run after a fix clears the gate */
    DECLARE @failedList NVARCHAR(2000);
    WITH latest AS (
        SELECT CheckName, Severity, Status,
               ROW_NUMBER() OVER (PARTITION BY CheckName
                                  ORDER BY CheckedAt DESC, QualityCheckLogID DESC) AS rn
        FROM etl.QualityCheckLog
        WHERE ETLBatchID = @ETLBatchID OR (@ETLBatchID IS NULL AND ETLBatchID IS NULL)
    )
    SELECT @failedList = STRING_AGG(CAST(CheckName AS NVARCHAR(MAX)), ', ')
    FROM latest
    WHERE rn = 1 AND Severity = 'Error' AND Status = 'Fail';

    IF @failedList IS NOT NULL
    BEGIN
        DECLARE @msg NVARCHAR(2048) = CONCAT(
            'Data quality gate FAILED for ETLBatchID ', ISNULL(CAST(@ETLBatchID AS VARCHAR(12)), '(ad-hoc)'),
            '. Failed checks: ', @failedList,
            '. Details: EXEC etl.usp_GetQualityReport ', ISNULL(CAST(@ETLBatchID AS VARCHAR(12)), ''), ';');
        THROW 50040, @msg, 1;
    END
END
GO

/* ============================================================================
   REPORT — the validation report (docs/14):
   result set 1: run summary; result set 2: per-check detail, failures first.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_GetQualityReport
    @ETLBatchID INT = NULL       -- NULL = most recent batch that has check results
AS
BEGIN
    SET NOCOUNT ON;

    IF @ETLBatchID IS NULL
        SET @ETLBatchID = (SELECT MAX(ETLBatchID) FROM etl.QualityCheckLog);

    /* latest result per check (a fixed + re-run check reports its new state) */
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY CheckName
                                     ORDER BY CheckedAt DESC, QualityCheckLogID DESC) AS rn
        FROM etl.QualityCheckLog
        WHERE ETLBatchID = @ETLBatchID OR (@ETLBatchID IS NULL AND ETLBatchID IS NULL)
    )
    SELECT @ETLBatchID                                                   AS ETLBatchID,
           MIN(CheckedAt)                                                AS RunStartedAt,
           COUNT(*)                                                      AS ChecksRun,
           SUM(CASE WHEN Status = 'Pass' THEN 1 ELSE 0 END)              AS Passed,
           SUM(CASE WHEN Status = 'Fail' AND Severity = 'Error'   THEN 1 ELSE 0 END) AS FailedErrors,
           SUM(CASE WHEN Status = 'Fail' AND Severity = 'Warning' THEN 1 ELSE 0 END) AS FailedWarnings,
           SUM(DurationMs)                                               AS TotalDurationMs,
           CASE WHEN SUM(CASE WHEN Status = 'Fail' AND Severity = 'Error' THEN 1 ELSE 0 END) = 0
                THEN 'PASS' ELSE 'FAIL' END                              AS GateResult
    FROM latest
    WHERE rn = 1;

    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY CheckName
                                     ORDER BY CheckedAt DESC, QualityCheckLogID DESC) AS rn
        FROM etl.QualityCheckLog
        WHERE ETLBatchID = @ETLBatchID OR (@ETLBatchID IS NULL AND ETLBatchID IS NULL)
    )
    SELECT CheckName            AS ValidationName,
           Category,
           Severity,
           Status               AS PassFail,
           TargetTable          AS AffectedTable,
           FailedRows           AS FailedRowCount,   -- -1 = the check itself errored
           DurationMs           AS ExecutionTimeMs,
           CheckedAt
    FROM latest
    WHERE rn = 1
    ORDER BY CASE WHEN Status = 'Fail' AND Severity = 'Error' THEN 0
                  WHEN Status = 'Fail'                        THEN 1
                  ELSE 2 END,
             Category, CheckName;
END
GO

PRINT 'Data quality framework created: catalog seeded, runner/assert/report procedures ready.';
GO

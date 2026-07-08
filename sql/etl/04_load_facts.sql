/* ============================================================================
   TrafficDW — fact loading
   Demonstrates: surrogate-key pipeline, POINT-IN-TIME SCD2 lookups,
   unknown member (-1), inferred (late-arriving) members, dedup, rejects,
   idempotent reprocessing, and the update pattern of accumulating snapshots.
   ========================================================================== */
USE TrafficDW;
GO

/* ============================================================================
   Late-arriving dimension support: create inferred skeleton rows for any
   business key present in staging facts but missing from the dimension.
   The SCD2 proc completes them (Type-1 in place) when real attributes arrive.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_CreateInferredMembers
    @ETLBatchID INT,
    @LoadDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @seg INT, @sns INT;

    INSERT INTO dim.DimRoadSegment (SegmentCode, RoadCode, RoadName, RoadCategory, City, District,
            Direction, StartIntersection, EndIntersection, LengthM, LaneCount,
            SpeedLimitKmh, CurrentSpeedLimitKmh, OriginalSpeedLimitKmh,
            EffectiveDate, ExpirationDate, IsCurrent, VersionNumber, IsInferred, HashDiff, ETLBatchID)
    SELECT DISTINCT s.SegmentCode, 'UNKNOWN', N'Inferred (awaiting master data)', 'Local',
            N'Unknown', N'Unknown', 'NB', N'Unknown', N'Unknown', 0, 1,
            50, 50, 50, @LoadDate, '9999-12-31', 1, 1, 1, 0x0, @ETLBatchID
    FROM stg.TrafficEvent s
    WHERE s.SegmentCode IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment d
                      WHERE d.SegmentCode = s.SegmentCode AND d.IsCurrent = 1);
    SET @seg = @@ROWCOUNT;

    INSERT INTO dim.DimSensor (SerialNumber, SensorTypeName, Technology, SegmentCode, StatusClass,
            InstallDate, FirmwareVersion, EffectiveDate, ExpirationDate,
            IsCurrent, VersionNumber, IsInferred, HashDiff, ETLBatchID)
    SELECT DISTINCT s.SensorSerial, N'Inferred', N'Unknown', ISNULL(s.SegmentCode,'UNKNOWN'), 'Unknown',
            NULL, NULL, @LoadDate, '9999-12-31', 1, 1, 1, 0x0, @ETLBatchID
    FROM stg.TrafficEvent s
    WHERE s.SensorSerial IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dim.DimSensor d
                      WHERE d.SerialNumber = s.SensorSerial AND d.IsCurrent = 1);
    SET @sns = @@ROWCOUNT;

    INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsInserted)
    VALUES (@ETLBatchID, 'InferredMembers (Segment)', @seg),
           (@ETLBatchID, 'InferredMembers (Sensor)',  @sns);
END
GO

/* ============================================================================
   1. TRANSACTION FACT — fact.FactTrafficEvent
   Idempotent per batch/date: reprocessing a load date first deletes its rows
   (partition-friendly: the delete hits one monthly partition).
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_LoadFactTrafficEvent
    @ETLBatchID INT,
    @LoadDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        DECLARE @DateKey INT = CONVERT(INT, FORMAT(@LoadDate, 'yyyyMMdd'));
        DECLARE @extracted INT = (SELECT COUNT(*) FROM stg.TrafficEvent WHERE LoadDate = @LoadDate);
        DECLARE @rejected INT = 0, @inserted INT = 0;

        BEGIN TRAN;

        /* ---------- idempotency: wipe any previous load of this date ---------- */
        DELETE FROM fact.FactTrafficEvent WHERE DateKey = @DateKey;

        /* ---------- cleanse + dedup + validate into a temp shape -------------- */
        ;WITH dedup AS (
            SELECT s.*,
                   ROW_NUMBER() OVER (PARTITION BY s.EventID          -- duplicate removal:
                                      ORDER BY s.EventTimestamp DESC) -- keep latest arrival
                       AS rn
            FROM stg.TrafficEvent s
            WHERE s.LoadDate = @LoadDate
        ),
        validated AS (
            SELECT d.*,
                   CASE
                       WHEN d.EventID IS NULL OR d.EventTimestamp IS NULL THEN 'MISSING_KEY'
                       WHEN d.SpeedKmh IS NOT NULL AND (d.SpeedKmh <= 0 OR d.SpeedKmh > 250) THEN 'SPEED_RANGE'
                       WHEN d.OccupancyPct IS NOT NULL AND (d.OccupancyPct < 0 OR d.OccupancyPct > 100) THEN 'OCCUPANCY_RANGE'
                       WHEN CAST(d.EventTimestamp AS DATE) <> @LoadDate THEN 'DATE_MISMATCH'
                       ELSE NULL
                   END AS RejectReason
            FROM dedup d
            WHERE d.rn = 1
        )
        SELECT * INTO #events FROM validated;

        /* rn > 1 duplicates are counted as rejected (reason DUPLICATE) */
        INSERT INTO stg.RejectTrafficEvent (ETLBatchID, RejectReason, EventID, EventTimestamp, SensorSerial, SegmentCode, RawPayload)
        SELECT @ETLBatchID, 'DUPLICATE', s.EventID, s.EventTimestamp, s.SensorSerial, s.SegmentCode, NULL
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY EventID ORDER BY EventTimestamp DESC) rn
              FROM stg.TrafficEvent WHERE LoadDate = @LoadDate) s
        WHERE s.rn > 1;
        SET @rejected = @@ROWCOUNT;

        INSERT INTO stg.RejectTrafficEvent (ETLBatchID, RejectReason, EventID, EventTimestamp, SensorSerial, SegmentCode, RawPayload)
        SELECT @ETLBatchID, e.RejectReason, e.EventID, e.EventTimestamp, e.SensorSerial, e.SegmentCode,
               CONCAT('speed=', e.SpeedKmh, ';occ=', e.OccupancyPct)
        FROM #events e
        WHERE e.RejectReason IS NOT NULL;
        SET @rejected = @rejected + @@ROWCOUNT;

        /* ---------- surrogate-key pipeline + insert --------------------------- */
        INSERT INTO fact.FactTrafficEvent
              (DateKey, TimeKey, RoadSegmentKey, SensorKey, VehicleTypeKey, WeatherKey,
               SpeedKmh, HeadwaySeconds, OccupancyPct, SpeedOverLimitKmh, EventID, ETLBatchID)
        SELECT
            @DateKey,
            DATEPART(HOUR, e.EventTimestamp) * 60 + DATEPART(MINUTE, e.EventTimestamp),
            ISNULL(seg.RoadSegmentKey, -1),                    -- unknown member fallback
            ISNULL(sns.SensorKey, -1),
            ISNULL(vt.VehicleTypeKey, -1),
            ISNULL(w.WeatherKey, -1),
            e.SpeedKmh,
            e.HeadwaySeconds,
            e.OccupancyPct,
            CASE WHEN e.SpeedKmh IS NOT NULL AND seg.SpeedLimitKmh IS NOT NULL
                 THEN e.SpeedKmh - seg.SpeedLimitKmh END,      -- vs the limit AT EVENT TIME
            e.EventID,
            @ETLBatchID
        FROM #events e
        /* POINT-IN-TIME SCD2 lookup: version valid on the event date, so
           historical events attach to historical attribute versions */
        LEFT JOIN dim.DimRoadSegment seg
               ON seg.SegmentCode = e.SegmentCode
              AND @LoadDate >= seg.EffectiveDate AND @LoadDate <= seg.ExpirationDate
        LEFT JOIN dim.DimSensor sns
               ON sns.SerialNumber = e.SensorSerial
              AND @LoadDate >= sns.EffectiveDate AND @LoadDate <= sns.ExpirationDate
        LEFT JOIN dim.DimVehicleType vt
               ON vt.TypeCode = e.VehicleTypeCode
        LEFT JOIN dim.DimWeatherCondition w
               ON  w.ConditionCode  = ISNULL(e.ConditionCode, 'UNK')
              AND w.TempBand       = (SELECT TempBand       FROM etl.fn_WeatherBands(e.TempC, e.PrecipitationMm, e.VisibilityM))
              AND w.PrecipBand     = (SELECT PrecipBand     FROM etl.fn_WeatherBands(e.TempC, e.PrecipitationMm, e.VisibilityM))
              AND w.VisibilityBand = (SELECT VisibilityBand FROM etl.fn_WeatherBands(e.TempC, e.PrecipitationMm, e.VisibilityM))
        WHERE e.RejectReason IS NULL;
        SET @inserted = @@ROWCOUNT;

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted, RowsInserted, RowsRejected)
        VALUES (@ETLBatchID, 'Fact.TrafficEvent', @extracted, @inserted, @rejected);

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

/* ============================================================================
   2. PERIODIC SNAPSHOT — fact.FactHourlyTraffic
   Dense grain: segment × hour, INCLUDING zero-traffic hours (spine cross join).
   Idempotent upsert on the natural key (DateKey, HourOfDay, RoadSegmentKey).
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_LoadFactHourlyTraffic
    @ETLBatchID INT,
    @LoadDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        DECLARE @DateKey INT = CONVERT(INT, FORMAT(@LoadDate, 'yyyyMMdd'));

        BEGIN TRAN;

        /* spine: every current segment × 24 hours → density guaranteed */
        ;WITH hours AS (
            SELECT TOP (24) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS HourOfDay
            FROM sys.all_objects
        ),
        spine AS (
            SELECT d.RoadSegmentKey, d.SegmentCode, d.SpeedLimitKmh, h.HourOfDay
            FROM dim.DimRoadSegment d CROSS JOIN hours h
            WHERE d.IsCurrent = 1 AND d.RoadSegmentKey <> -1
        ),
        agg AS (
            SELECT s.SegmentCode, s.HourOfDay, s.VehicleCount, s.HeavyVehicleCount,
                   s.AvgSpeedKmh, s.P85SpeedKmh, s.AvgOccupancyPct,
                   s.ConditionCode, s.TempBandSource, s.PrecipitationMm, s.VisibilityM
            FROM stg.HourlyTraffic s
            WHERE s.EventDate = @LoadDate
        ),
        merged AS (
            SELECT sp.RoadSegmentKey,
                   sp.HourOfDay,
                   ISNULL(a.VehicleCount, 0)        AS VehicleCount,
                   ISNULL(a.HeavyVehicleCount, 0)   AS HeavyVehicleCount,
                   a.AvgSpeedKmh,
                   a.P85SpeedKmh,
                   a.AvgOccupancyPct,
                   /* congestion index vs. the CURRENT limit of the segment version */
                   CASE WHEN a.AvgSpeedKmh IS NOT NULL AND sp.SpeedLimitKmh > 0
                        THEN CAST(
                             CASE WHEN 1.0 - a.AvgSpeedKmh / sp.SpeedLimitKmh < 0 THEN 0
                                  WHEN 1.0 - a.AvgSpeedKmh / sp.SpeedLimitKmh > 1 THEN 1
                                  ELSE 1.0 - a.AvgSpeedKmh / sp.SpeedLimitKmh END AS DECIMAL(4,3))
                        END                          AS CongestionIndex,
                   a.TempBandSource                  AS AvgTempC,
                   a.PrecipitationMm,
                   ISNULL(w.WeatherKey, -1)          AS WeatherKey
            FROM spine sp
            LEFT JOIN agg a ON a.SegmentCode = sp.SegmentCode AND a.HourOfDay = sp.HourOfDay
            OUTER APPLY etl.fn_WeatherBands(a.TempBandSource, a.PrecipitationMm, a.VisibilityM) b
            LEFT JOIN dim.DimWeatherCondition w
                   ON w.ConditionCode  = ISNULL(a.ConditionCode, 'UNK')
                  AND w.TempBand = b.TempBand AND w.PrecipBand = b.PrecipBand
                  AND w.VisibilityBand = b.VisibilityBand
        )
        MERGE fact.FactHourlyTraffic AS tgt
        USING merged AS src
           ON tgt.DateKey = @DateKey
          AND tgt.HourOfDay = src.HourOfDay
          AND tgt.RoadSegmentKey = src.RoadSegmentKey
        WHEN MATCHED THEN
            UPDATE SET WeatherKey = src.WeatherKey,
                       VehicleCount = src.VehicleCount, HeavyVehicleCount = src.HeavyVehicleCount,
                       AvgSpeedKmh = src.AvgSpeedKmh, P85SpeedKmh = src.P85SpeedKmh,
                       AvgOccupancyPct = src.AvgOccupancyPct, CongestionIndex = src.CongestionIndex,
                       AvgTempC = src.AvgTempC, PrecipitationMm = src.PrecipitationMm,
                       ETLBatchID = @ETLBatchID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DateKey, HourOfDay, RoadSegmentKey, WeatherKey, VehicleCount, HeavyVehicleCount,
                    IncidentCount, AvgSpeedKmh, P85SpeedKmh, AvgOccupancyPct, CongestionIndex,
                    AvgTempC, PrecipitationMm, ETLBatchID)
            VALUES (@DateKey, src.HourOfDay, src.RoadSegmentKey, src.WeatherKey, src.VehicleCount,
                    src.HeavyVehicleCount, 0, src.AvgSpeedKmh, src.P85SpeedKmh, src.AvgOccupancyPct,
                    src.CongestionIndex, src.AvgTempC, src.PrecipitationMm, @ETLBatchID);

        /* incident counts stamped from the accumulating fact */
        UPDATE f
        SET    IncidentCount = x.Cnt
        FROM   fact.FactHourlyTraffic f
        JOIN  (SELECT i.RoadSegmentKey, t.[Hour] AS HourOfDay, COUNT(*) AS Cnt
               FROM fact.FactIncidentLifecycle i
               JOIN dim.DimTime t ON t.TimeKey = i.DetectedTimeKey
               WHERE i.DetectedDateKey = @DateKey
               GROUP BY i.RoadSegmentKey, t.[Hour]) x
          ON x.RoadSegmentKey = f.RoadSegmentKey AND x.HourOfDay = f.HourOfDay
        WHERE f.DateKey = @DateKey;

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted, RowsInserted)
        VALUES (@ETLBatchID, 'Fact.HourlyTraffic',
                (SELECT COUNT(*) FROM stg.HourlyTraffic WHERE EventDate = @LoadDate),
                (SELECT COUNT(*) FROM fact.FactHourlyTraffic WHERE DateKey = @DateKey));

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

/* ============================================================================
   3. ACCUMULATING SNAPSHOT — fact.FactIncidentLifecycle
   INSERT on first sight, UPDATE milestones as they arrive.
   Unreached milestones stay at the Unknown member (-1); lags computed only
   when both endpoints exist.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_LoadFactIncidentLifecycle
    @ETLBatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRAN;

        ;WITH src AS (
            SELECT s.*,
                   ISNULL(it.IncidentTypeKey, -1)  AS IncidentTypeKey,
                   ISNULL(seg.RoadSegmentKey, -1)  AS RoadSegmentKey,
                   ISNULL(eu.EmergencyUnitKey, -1) AS EmergencyUnitKey,
                   ISNULL(w.WeatherKey, -1)        AS WeatherKey,
                   /* milestone keys: -1 until the milestone happens */
                   CONVERT(INT, FORMAT(s.DetectedAt, 'yyyyMMdd'))                                        AS DetD,
                   CONVERT(SMALLINT, DATEPART(HOUR, s.DetectedAt) * 60 + DATEPART(MINUTE, s.DetectedAt)) AS DetT,
                   ISNULL(CONVERT(INT, FORMAT(s.DispatchedAt, 'yyyyMMdd')), -1)                          AS DisD,
                   ISNULL(CONVERT(SMALLINT, DATEPART(HOUR, s.DispatchedAt) * 60 + DATEPART(MINUTE, s.DispatchedAt)), -1) AS DisT,
                   ISNULL(CONVERT(INT, FORMAT(s.ArrivedAt, 'yyyyMMdd')), -1)                             AS ArrD,
                   ISNULL(CONVERT(SMALLINT, DATEPART(HOUR, s.ArrivedAt) * 60 + DATEPART(MINUTE, s.ArrivedAt)), -1)       AS ArrT,
                   ISNULL(CONVERT(INT, FORMAT(s.RoadClearedAt, 'yyyyMMdd')), -1)                         AS ClrD,
                   ISNULL(CONVERT(SMALLINT, DATEPART(HOUR, s.RoadClearedAt) * 60 + DATEPART(MINUTE, s.RoadClearedAt)), -1) AS ClrT,
                   ISNULL(CONVERT(INT, FORMAT(s.ClosedAt, 'yyyyMMdd')), -1)                              AS CloD,
                   ISNULL(CONVERT(SMALLINT, DATEPART(HOUR, s.ClosedAt) * 60 + DATEPART(MINUTE, s.ClosedAt)), -1)         AS CloT,
                   DATEDIFF(MINUTE, s.DetectedAt,   s.DispatchedAt)  AS MinToDispatch,
                   DATEDIFF(MINUTE, s.DispatchedAt, s.ArrivedAt)     AS MinToArrive,
                   DATEDIFF(MINUTE, s.ArrivedAt,    s.RoadClearedAt) AS MinToClear,
                   DATEDIFF(MINUTE, s.DetectedAt,   s.ClosedAt)      AS TotalMin
            FROM stg.IncidentLifecycle s
            LEFT JOIN dim.DimIncidentType it ON it.TypeCode = s.IncidentTypeCode
            LEFT JOIN dim.DimRoadSegment seg
                   ON seg.SegmentCode = s.SegmentCode
                  AND CAST(s.DetectedAt AS DATE) >= seg.EffectiveDate
                  AND CAST(s.DetectedAt AS DATE) <= seg.ExpirationDate
            LEFT JOIN dim.DimEmergencyUnit eu ON eu.UnitCode = s.UnitCode
            LEFT JOIN dim.DimWeatherCondition w
                   ON w.ConditionCode = ISNULL(s.ConditionCode,'UNK')
                  AND w.TempBand = 'Unknown' AND w.PrecipBand = 'Unknown' AND w.VisibilityBand = 'Unknown'
        )
        MERGE fact.FactIncidentLifecycle AS tgt
        USING src
           ON tgt.IncidentNumber = src.IncidentNumber
        WHEN MATCHED THEN
            UPDATE SET
                /* milestones only move forward; a milestone once set is stable */
                DispatchedDateKey = src.DisD, DispatchedTimeKey = src.DisT,
                ArrivedDateKey    = src.ArrD, ArrivedTimeKey    = src.ArrT,
                ClearedDateKey    = src.ClrD, ClearedTimeKey    = src.ClrT,
                ClosedDateKey     = src.CloD, ClosedTimeKey     = src.CloT,
                EmergencyUnitKey  = src.EmergencyUnitKey,
                CurrentStatus     = src.CurrentStatus,
                Severity          = src.Severity,
                LanesBlocked      = src.LanesBlocked,
                MinutesToDispatch = src.MinToDispatch,
                MinutesToArrive   = src.MinToArrive,
                MinutesToClear    = src.MinToClear,
                TotalDurationMinutes = src.TotalMin,
                ETLBatchID        = @ETLBatchID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (IncidentNumber, IncidentTypeKey, RoadSegmentKey, EmergencyUnitKey, WeatherKey,
                    DetectedDateKey, DetectedTimeKey, DispatchedDateKey, DispatchedTimeKey,
                    ArrivedDateKey, ArrivedTimeKey, ClearedDateKey, ClearedTimeKey,
                    ClosedDateKey, ClosedTimeKey, CurrentStatus, Severity, LanesBlocked,
                    MinutesToDispatch, MinutesToArrive, MinutesToClear, TotalDurationMinutes, ETLBatchID)
            VALUES (src.IncidentNumber, src.IncidentTypeKey, src.RoadSegmentKey, src.EmergencyUnitKey, src.WeatherKey,
                    src.DetD, src.DetT, src.DisD, src.DisT, src.ArrD, src.ArrT, src.ClrD, src.ClrT,
                    src.CloD, src.CloT, src.CurrentStatus, src.Severity, src.LanesBlocked,
                    src.MinToDispatch, src.MinToArrive, src.MinToClear, src.TotalMin, @ETLBatchID);

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted)
        VALUES (@ETLBatchID, 'Fact.IncidentLifecycle', (SELECT COUNT(*) FROM stg.IncidentLifecycle));

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

/* ============================================================================
   MASTER PIPELINE — one call runs the whole warehouse refresh for a date.
   @FullLoad = 1 → watermark reset (initial load / recovery).
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_RunNightlyPipeline
    @LoadDate DATE = NULL,
    @FullLoad BIT  = 0
AS
BEGIN
    SET NOCOUNT ON;
    IF @LoadDate IS NULL SET @LoadDate = CAST(DATEADD(DAY, -1, SYSUTCDATETIME()) AS DATE);

    DECLARE @ETLBatchID INT;
    EXEC etl.usp_StartBatch 'NightlyWarehouseRefresh', @LoadDate, @ETLBatchID OUTPUT;

    BEGIN TRY
        /* E */ EXEC etl.usp_ExtractFromOLTP        @ETLBatchID, @FullLoad;
        /* T+L: dimensions BEFORE facts (SK dependency) */
                EXEC etl.usp_LoadAllDimensions      @ETLBatchID, @LoadDate;
                EXEC etl.usp_CreateInferredMembers  @ETLBatchID, @LoadDate;
        /* facts */
                EXEC etl.usp_LoadFactTrafficEvent      @ETLBatchID, @LoadDate;
                EXEC etl.usp_LoadFactIncidentLifecycle @ETLBatchID;
                EXEC etl.usp_LoadFactHourlyTraffic     @ETLBatchID, @LoadDate;
        /* housekeeping */
                EXEC etl.usp_UpdateFactStatistics;

        EXEC etl.usp_EndBatch @ETLBatchID, 'Succeeded';
        EXEC etl.usp_ValidateBatch @ETLBatchID;   -- reconciliation report
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        /* usp_LogError already ran in the failing proc; ensure batch is closed */
        IF EXISTS (SELECT 1 FROM etl.BatchLog WHERE ETLBatchID = @ETLBatchID AND Status = 'Running')
            EXEC etl.usp_EndBatch @ETLBatchID, 'Failed';
        THROW;
    END CATCH
END
GO

PRINT 'Fact load procedures and master pipeline created.';
GO

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

    ;WITH segment_src AS (
        SELECT s.SegmentCode,
               ROW_NUMBER() OVER (PARTITION BY s.SegmentCode ORDER BY s.SegmentCode) AS rn
        FROM stg.TrafficEvent s
        WHERE s.SegmentCode IS NOT NULL
    ),
    segment_dedup AS (
        SELECT SegmentCode
        FROM segment_src
        WHERE rn = 1
    )
    INSERT INTO dim.DimRoadSegment (SegmentCode, RoadCode, RoadName, RoadCategory, City, District,
            Direction, StartIntersection, EndIntersection, LengthM, LaneCount,
            SpeedLimitKmh, CurrentSpeedLimitKmh, OriginalSpeedLimitKmh,
            EffectiveDate, ExpirationDate, IsCurrent, VersionNumber, IsInferred, HashDiff, ETLBatchID)
    SELECT s.SegmentCode, 'UNKNOWN', N'Inferred (awaiting master data)', 'Local',
            N'Unknown', N'Unknown', 'NB', N'Unknown', N'Unknown', 0, 1,
            50, 50, 50, @LoadDate, '9999-12-31', 1, 1, 1, 0x0, @ETLBatchID
    FROM segment_dedup s
    WHERE NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment d
                      WHERE d.SegmentCode = s.SegmentCode AND d.IsCurrent = 1);
    SET @seg = @@ROWCOUNT;

    /* Only SENSOR-sourced rows may create inferred DimSensor members.
       Camera detections carry CameraCode and resolve against DimTrafficCamera;
       inferring them as sensors (the earlier behaviour, when camera_code was
       aliased into sensor_serial) created skeleton "sensors" that no master
       data could ever complete, so IsInferred stayed 1 forever. */
    ;WITH sensor_src AS (
        SELECT s.SensorSerial,
               ISNULL(s.SegmentCode,'UNKNOWN') AS SegmentCode,
               ROW_NUMBER() OVER (PARTITION BY s.SensorSerial ORDER BY s.SensorSerial) AS rn
        FROM stg.TrafficEvent s
        WHERE s.SensorSerial IS NOT NULL
          AND ISNULL(s.DetectorType, 'SENSOR') = 'SENSOR'
    ),
    sensor_dedup AS (
        SELECT SensorSerial, SegmentCode
        FROM sensor_src
        WHERE rn = 1
    )
    INSERT INTO dim.DimSensor (SerialNumber, SensorTypeName, Technology, SegmentCode, StatusClass,
            InstallDate, FirmwareVersion, EffectiveDate, ExpirationDate,
            IsCurrent, VersionNumber, IsInferred, HashDiff, ETLBatchID)
    SELECT d.SensorSerial, N'Inferred', N'Unknown', d.SegmentCode, 'Unknown',
            NULL, NULL, @LoadDate, '9999-12-31', 1, 1, 1, 0x0, @ETLBatchID
    FROM sensor_dedup d
    WHERE NOT EXISTS (SELECT 1 FROM dim.DimSensor existing
                      WHERE existing.SerialNumber = d.SensorSerial AND existing.IsCurrent = 1);
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

        /* ---------- idempotency: wipe any previous load of this date ----------
           The quarantine must be wiped by DATE too, not just the fact. It is
           append-only otherwise, so re-running a date left the previous run's
           rejects in place under an older ETLBatchID: batch-scoped checks stayed
           correct, but etl.usp_ExecutionSummary and mart.vPbiRejectDaily
           double-counted rejects on every rerun. Deleting by the event date the
           rejects belong to keeps "rows rejected for 2026-06-01" a single
           truthful number however many times the date is reprocessed. */
        DELETE FROM fact.FactTrafficEvent WHERE DateKey = @DateKey;
        DELETE FROM stg.RejectTrafficEvent
        WHERE CAST(EventTimestamp AS DATE) = @LoadDate
           OR (EventTimestamp IS NULL AND ETLBatchID IN
               (SELECT ETLBatchID FROM etl.BatchLog WHERE LoadDate = @LoadDate));

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
        SELECT @ETLBatchID, 'DUPLICATE', s.EventID, s.EventTimestamp,
               COALESCE(s.SensorSerial, s.CameraCode), s.SegmentCode, NULL
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY EventID ORDER BY EventTimestamp DESC) rn
              FROM stg.TrafficEvent WHERE LoadDate = @LoadDate) s
        WHERE s.rn > 1;
        SET @rejected = @@ROWCOUNT;

        INSERT INTO stg.RejectTrafficEvent (ETLBatchID, RejectReason, EventID, EventTimestamp, SensorSerial, SegmentCode, RawPayload)
        SELECT @ETLBatchID, e.RejectReason, e.EventID, e.EventTimestamp,
               COALESCE(e.SensorSerial, e.CameraCode), e.SegmentCode,
               CONCAT('detector=', ISNULL(e.DetectorType,'?'), ';speed=', e.SpeedKmh, ';occ=', e.OccupancyPct)
        FROM #events e
        WHERE e.RejectReason IS NOT NULL;
        SET @rejected = @rejected + @@ROWCOUNT;

        /* ---------- surrogate-key pipeline + insert --------------------------- */
        INSERT INTO fact.FactTrafficEvent
              (DateKey, TimeKey, RoadSegmentKey, SensorKey, CameraKey, VehicleTypeKey, WeatherKey,
               DetectorType, SpeedKmh, HeadwaySeconds, OccupancyPct, SpeedOverLimitKmh, EventID, ETLBatchID)
        SELECT
            @DateKey,
            DATEPART(HOUR, e.EventTimestamp) * 60 + DATEPART(MINUTE, e.EventTimestamp),
            ISNULL(seg.RoadSegmentKey, -1),                    -- unknown member fallback
            ISNULL(sns.SensorKey, -1),                         -- -1 for camera detections
            ISNULL(cam.CameraKey, -1),                         -- -1 for sensor detections
            ISNULL(vt.VehicleTypeKey, -1),
            ISNULL(w.WeatherKey, -1),
            ISNULL(e.DetectorType, 'SENSOR'),
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
        /* DimTrafficCamera is SCD 1, so no validity predicate — there is only
           ever one row per CameraCode. Resolves only for DetectorType='CAMERA'
           rows; sensor rows have CameraCode NULL and fall back to -1. */
        LEFT JOIN dim.DimTrafficCamera cam
               ON cam.CameraCode = e.CameraCode
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
   Idempotent replace-by-date (delete + insert, like the transaction fact):
   an upsert keyed on RoadSegmentKey would leave rows of older SCD2 segment
   versions behind when a date is reprocessed after a version change,
   double-counting that segment-hour.
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

        /* ---------- idempotency: wipe any previous load of this date ----------
           (a keyed upsert cannot do this safely: after an SCD2 change the new
            spine carries new RoadSegmentKeys, so old-version rows would never
            match and would survive as duplicates of the same segment-hour) */
        DELETE FROM fact.FactHourlyTraffic WHERE DateKey = @DateKey;

        /* ---------- DENSITY IS PER LOADED DAY, NOT PER CALENDAR DAY ----------
           The spine guarantees a row for every segment-hour so that ABSENCE is
           measurable: "which segments were empty at 03:00" is a real question
           and only a dense snapshot can answer it.

           That contract applies WITHIN a day that was actually fed. It does not
           license fabricating an ENTIRE day that has no source data at all.
           Running the pipeline for a date beyond the feed - which
           scripts/run_scd2_demo.ps1 legitimately does, because a new SCD2
           version needs a load date later than the last one - used to insert
           2,880 rows of pure zeros keyed to the Unknown weather member. Those
           rows are indistinguishable from real zero-traffic hours downstream,
           so they surfaced as an 'Unknown / 0 vehicles / NULL speed' bucket in
           mart.vTrafficByWeather and every other analytics view, and a second
           phantom day was added on every demo run.

           A day with no staged aggregates is a day that was never fed, so there
           is nothing to snapshot. The DELETE above still runs, so re-running a
           date cleans up any phantom rows a previous version created. */
        IF NOT EXISTS (SELECT 1 FROM stg.HourlyTraffic WHERE EventDate = @LoadDate)
        BEGIN
            INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted, RowsInserted)
            VALUES (@ETLBatchID, 'Fact.HourlyTraffic', 0, 0);

            COMMIT;
            PRINT CONCAT('Fact.HourlyTraffic: no staged aggregates for ',
                         CONVERT(CHAR(10), @LoadDate, 120),
                         ' - snapshot skipped (nothing was fed for this date).');
            RETURN;
        END

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
        INSERT INTO fact.FactHourlyTraffic
              (DateKey, HourOfDay, RoadSegmentKey, WeatherKey, VehicleCount, HeavyVehicleCount,
               IncidentCount, AvgSpeedKmh, P85SpeedKmh, AvgOccupancyPct, CongestionIndex,
               AvgTempC, PrecipitationMm, ETLBatchID)
        SELECT @DateKey, src.HourOfDay, src.RoadSegmentKey, src.WeatherKey, src.VehicleCount,
               src.HeavyVehicleCount, 0, src.AvgSpeedKmh, src.P85SpeedKmh, src.AvgOccupancyPct,
               src.CongestionIndex, src.AvgTempC, src.PrecipitationMm, @ETLBatchID
        FROM merged AS src;

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
                /* milestones are refreshed from the source snapshot: OLTP is
                   the system of record, and the extract always carries the
                   full milestone history of each modified incident */
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

    DECLARE @ETLBatchID INT, @WatermarkUpper DATETIME2(3);
    EXEC etl.usp_StartBatch 'NightlyWarehouseRefresh', @LoadDate, @ETLBatchID OUTPUT;

    BEGIN TRY
        /* E */ EXEC etl.usp_ExtractFromOLTP        @ETLBatchID, @FullLoad,
                                                    @WatermarkUpper OUTPUT;
        /* T+L: dimensions BEFORE facts (SK dependency) */
                EXEC etl.usp_LoadAllDimensions      @ETLBatchID, @LoadDate;
                EXEC etl.usp_CreateInferredMembers  @ETLBatchID, @LoadDate;
        /* facts */
                EXEC etl.usp_LoadFactTrafficEvent      @ETLBatchID, @LoadDate;
                EXEC etl.usp_LoadFactIncidentLifecycle @ETLBatchID;
                EXEC etl.usp_LoadFactHourlyTraffic     @ETLBatchID, @LoadDate;
        /* housekeeping */
                EXEC etl.usp_UpdateFactStatistics;
        /* post-load data-quality run (sql/etl/05_quality_checks.sql, docs/14):
           log-only inside the batch so Status reflects the LOAD outcome;
           orchestrators enforce the gate separately via etl.usp_AssertQuality */
                EXEC etl.usp_RunQualityChecks @ETLBatchID = @ETLBatchID,
                                              @LoadDate = @LoadDate, @FailOnError = 0;
        /* advance watermarks LAST: a failure anywhere above leaves them
           untouched, so the rerun re-extracts the same OLTP delta (docs/06) */
                EXEC etl.usp_AdvanceWatermarks @ETLBatchID, @WatermarkUpper;

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

/* ============================================================================
   TrafficDW — ETL framework: batch logging, error handling, reconciliation,
   and incremental extraction from TrafficOLTP into staging.
   Every load proc follows the same contract:
     - runs inside a TRY/CATCH,
     - logs row counts to etl.RowLog,
     - on error: logs to etl.ErrorLog, marks batch Failed, rethrows,
     - is idempotent per batch (safe rerun after failure).
   ========================================================================== */
USE TrafficDW;
GO

/* ------------------------------------------------------------------ logging */
CREATE TABLE etl.BatchLog (
    ETLBatchID   INT IDENTITY(1,1) CONSTRAINT PK_BatchLog PRIMARY KEY,
    PipelineName VARCHAR(100) NOT NULL,
    LoadDate     DATE         NOT NULL,
    Status       VARCHAR(20)  NOT NULL CONSTRAINT DF_Batch_Status DEFAULT 'Running',
       -- Running / Succeeded / Failed
    StartedAt    DATETIME2(3) NOT NULL CONSTRAINT DF_Batch_Start DEFAULT SYSUTCDATETIME(),
    FinishedAt   DATETIME2(3) NULL
);

CREATE TABLE etl.RowLog (
    RowLogID     INT IDENTITY(1,1) CONSTRAINT PK_RowLog PRIMARY KEY,
    ETLBatchID   INT          NOT NULL CONSTRAINT FK_RowLog_Batch REFERENCES etl.BatchLog(ETLBatchID),
    StepName     VARCHAR(100) NOT NULL,
    RowsExtracted INT NULL,
    RowsInserted  INT NULL,
    RowsUpdated   INT NULL,
    RowsRejected  INT NULL,
    LoggedAt     DATETIME2(3) NOT NULL CONSTRAINT DF_RowLog_At DEFAULT SYSUTCDATETIME()
);

CREATE TABLE etl.ErrorLog (
    ErrorLogID   INT IDENTITY(1,1) CONSTRAINT PK_ErrorLog PRIMARY KEY,
    ETLBatchID   INT           NULL,
    ProcedureName NVARCHAR(200) NULL,
    ErrorNumber  INT           NULL,
    ErrorLine    INT           NULL,
    ErrorMessage NVARCHAR(4000) NULL,
    OccurredAt   DATETIME2(3)  NOT NULL CONSTRAINT DF_Err_At DEFAULT SYSUTCDATETIME()
);
GO

/* ------------------------------------------------------------ batch control */
CREATE OR ALTER PROCEDURE etl.usp_StartBatch
    @PipelineName VARCHAR(100),
    @LoadDate     DATE,
    @ETLBatchID   INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO etl.BatchLog (PipelineName, LoadDate) VALUES (@PipelineName, @LoadDate);
    SET @ETLBatchID = SCOPE_IDENTITY();
END
GO

CREATE OR ALTER PROCEDURE etl.usp_EndBatch
    @ETLBatchID INT,
    @Status     VARCHAR(20)   -- Succeeded / Failed
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE etl.BatchLog
    SET Status = @Status, FinishedAt = SYSUTCDATETIME()
    WHERE ETLBatchID = @ETLBatchID;
END
GO

CREATE OR ALTER PROCEDURE etl.usp_LogError
    @ETLBatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO etl.ErrorLog (ETLBatchID, ProcedureName, ErrorNumber, ErrorLine, ErrorMessage)
    VALUES (@ETLBatchID, ERROR_PROCEDURE(), ERROR_NUMBER(), ERROR_LINE(), ERROR_MESSAGE());
    EXEC etl.usp_EndBatch @ETLBatchID, 'Failed';
END
GO

/* Reconciliation: extracted must equal inserted + updated + rejected per step */
CREATE OR ALTER PROCEDURE etl.usp_ValidateBatch
    @ETLBatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    SELECT StepName,
           RowsExtracted,
           ISNULL(RowsInserted,0) + ISNULL(RowsUpdated,0) + ISNULL(RowsRejected,0) AS RowsAccounted,
           CASE WHEN RowsExtracted IS NULL
                  OR RowsExtracted = ISNULL(RowsInserted,0) + ISNULL(RowsUpdated,0) + ISNULL(RowsRejected,0)
                THEN 'OK' ELSE 'MISMATCH' END AS Reconciliation
    FROM etl.RowLog
    WHERE ETLBatchID = @ETLBatchID
    ORDER BY RowLogID;
END
GO

/* ============================================================================
   EXTRACT — incremental, high-watermark pattern.
   @FullLoad = 1 resets the watermark (initial population / recovery).
   The upper bound is captured ONCE, BEFORE reading (consistent snapshot), and
   returned to the caller via @WatermarkUpper. This proc does NOT advance the
   stored watermarks — etl.usp_AdvanceWatermarks does, called by
   usp_RunNightlyPipeline as its FINAL step, so a failure anywhere in the
   pipeline (dims, facts) leaves watermarks untouched and a rerun re-extracts
   the same OLTP delta (restart-safe).
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_ExtractFromOLTP
    @ETLBatchID     INT,
    @FullLoad       BIT = 0,
    @WatermarkUpper DATETIME2(3) = NULL OUTPUT   -- pass to usp_AdvanceWatermarks on success
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @WatermarkUpper = SYSUTCDATETIME();   -- one consistent upper bound for ALL extracts
    DECLARE @rows INT;

    IF @FullLoad = 1
        UPDATE etl.WatermarkControl SET LastWatermark = '1900-01-01';

    BEGIN TRY
        /* -------- RoadSegment (denormalized during extract: road + geography) */
        DECLARE @wmSeg DATETIME2(3) = (SELECT LastWatermark FROM etl.WatermarkControl WHERE SourceTable = 'oltp.RoadSegment');
        TRUNCATE TABLE stg.RoadSegment;
        INSERT INTO stg.RoadSegment (SegmentCode, RoadCode, RoadName, RoadCategory, City, District,
                                     Direction, StartIntersection, EndIntersection,
                                     Latitude, Longitude, LengthM, LaneCount, SpeedLimitKmh, SourceModifiedAt)
        SELECT s.SegmentCode, r.RoadCode, r.RoadName, r.RoadCategory, c.CityName, d.DistrictName,
               s.Direction, i1.IntersectionName, i2.IntersectionName,
               l.Latitude, l.Longitude, s.LengthM, s.LaneCount, s.SpeedLimitKmh, s.ModifiedAt
        FROM TrafficOLTP.oltp.RoadSegment s
        JOIN TrafficOLTP.oltp.Road r          ON r.RoadID = s.RoadID
        JOIN TrafficOLTP.oltp.City c          ON c.CityID = r.CityID
        JOIN TrafficOLTP.oltp.Intersection i1 ON i1.IntersectionID = s.StartIntersectionID
        JOIN TrafficOLTP.oltp.Intersection i2 ON i2.IntersectionID = s.EndIntersectionID
        JOIN TrafficOLTP.oltp.Location l      ON l.LocationID = i1.LocationID
        JOIN TrafficOLTP.oltp.District d      ON d.DistrictID = l.DistrictID
        WHERE s.ModifiedAt > @wmSeg AND s.ModifiedAt <= @WatermarkUpper;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted) VALUES (@ETLBatchID, 'Extract.RoadSegment', @rows);

        /* ------------------------------------------------------------ Sensor */
        DECLARE @wmSns DATETIME2(3) = (SELECT LastWatermark FROM etl.WatermarkControl WHERE SourceTable = 'oltp.Sensor');
        TRUNCATE TABLE stg.Sensor;
        INSERT INTO stg.Sensor (SerialNumber, SensorTypeName, Technology, SegmentCode,
                                StatusClass, InstallDate, FirmwareVersion, SourceModifiedAt)
        SELECT sn.SerialNumber, st.TypeName, st.Technology, seg.SegmentCode,
               sn.Status, sn.InstallDate, sn.FirmwareVersion, sn.ModifiedAt
        FROM TrafficOLTP.oltp.Sensor sn
        JOIN TrafficOLTP.oltp.SensorType st  ON st.SensorTypeID = sn.SensorTypeID
        JOIN TrafficOLTP.oltp.RoadSegment seg ON seg.RoadSegmentID = sn.RoadSegmentID
        WHERE sn.ModifiedAt > @wmSns AND sn.ModifiedAt <= @WatermarkUpper;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted) VALUES (@ETLBatchID, 'Extract.Sensor', @rows);

        /* ------------------------------------------------------- VehicleType */
        TRUNCATE TABLE stg.VehicleType;
        INSERT INTO stg.VehicleType (TypeCode, TypeName, Category, IsHeavy, SourceModifiedAt)
        SELECT vt.TypeCode, vt.TypeName, vt.Category, vt.IsHeavy, vt.ModifiedAt
        FROM TrafficOLTP.oltp.VehicleType vt
        WHERE vt.ModifiedAt > (SELECT LastWatermark FROM etl.WatermarkControl WHERE SourceTable = 'oltp.VehicleType')
          AND vt.ModifiedAt <= @WatermarkUpper;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted) VALUES (@ETLBatchID, 'Extract.VehicleType', @rows);

        /* ----------------------------------------------------- TrafficCamera */
        TRUNCATE TABLE stg.TrafficCamera;
        INSERT INTO stg.TrafficCamera (CameraCode, IntersectionName, Model, Resolution, FirmwareVersion, Status, SourceModifiedAt)
        SELECT cm.CameraCode, i.IntersectionName, cm.Model, cm.Resolution, cm.FirmwareVersion, cm.Status, cm.ModifiedAt
        FROM TrafficOLTP.oltp.TrafficCamera cm
        JOIN TrafficOLTP.oltp.Intersection i ON i.IntersectionID = cm.IntersectionID
        WHERE cm.ModifiedAt > (SELECT LastWatermark FROM etl.WatermarkControl WHERE SourceTable = 'oltp.TrafficCamera')
          AND cm.ModifiedAt <= @WatermarkUpper;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted) VALUES (@ETLBatchID, 'Extract.TrafficCamera', @rows);

        /* ------------------------------------------------------ TrafficLight */
        TRUNCATE TABLE stg.TrafficLight;
        INSERT INTO stg.TrafficLight (ControllerCode, IntersectionName, TimingPlan, CycleSeconds, Status, SourceModifiedAt)
        SELECT tl.ControllerCode, i.IntersectionName, tl.TimingPlan, tl.CycleSeconds, tl.Status, tl.ModifiedAt
        FROM TrafficOLTP.oltp.TrafficLight tl
        JOIN TrafficOLTP.oltp.Intersection i ON i.IntersectionID = tl.IntersectionID
        WHERE tl.ModifiedAt > (SELECT LastWatermark FROM etl.WatermarkControl WHERE SourceTable = 'oltp.TrafficLight')
          AND tl.ModifiedAt <= @WatermarkUpper;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted) VALUES (@ETLBatchID, 'Extract.TrafficLight', @rows);

        /* ------------------------------------------------------ IncidentType */
        TRUNCATE TABLE stg.IncidentType;
        INSERT INTO stg.IncidentType (TypeCode, TypeName, Category, DefaultSeverity, SourceModifiedAt)
        SELECT it.TypeCode, it.TypeName, it.Category, it.DefaultSeverity, it.ModifiedAt
        FROM TrafficOLTP.oltp.IncidentType it
        WHERE it.ModifiedAt > (SELECT LastWatermark FROM etl.WatermarkControl WHERE SourceTable = 'oltp.IncidentType')
          AND it.ModifiedAt <= @WatermarkUpper;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted) VALUES (@ETLBatchID, 'Extract.IncidentType', @rows);

        /* ----------------------------------------------------- EmergencyUnit */
        TRUNCATE TABLE stg.EmergencyUnit;
        INSERT INTO stg.EmergencyUnit (UnitCode, UnitType, HomeStation, VehicleMake, VehicleModel, SourceModifiedAt)
        SELECT ev.UnitCode, ev.UnitType, ev.HomeStation, v.Make, v.Model, ev.ModifiedAt
        FROM TrafficOLTP.oltp.EmergencyVehicle ev
        JOIN TrafficOLTP.oltp.Vehicle v ON v.VehicleID = ev.VehicleID
        WHERE ev.ModifiedAt > (SELECT LastWatermark FROM etl.WatermarkControl WHERE SourceTable = 'oltp.EmergencyVehicle')
          AND ev.ModifiedAt <= @WatermarkUpper;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted) VALUES (@ETLBatchID, 'Extract.EmergencyUnit', @rows);

        /* ---------------------------------------- Incident lifecycle snapshot
           First response per incident (earliest dispatch); milestone columns
           flattened for the accumulating fact MERGE. */
        DECLARE @wmInc DATETIME2(3) = (SELECT LastWatermark FROM etl.WatermarkControl WHERE SourceTable = 'oltp.Incident');
        TRUNCATE TABLE stg.IncidentLifecycle;
        INSERT INTO stg.IncidentLifecycle (IncidentNumber, IncidentTypeCode, SegmentCode, UnitCode,
                                           ConditionCode, DetectedAt, DispatchedAt, ArrivedAt,
                                           RoadClearedAt, ClosedAt, CurrentStatus, Severity, LanesBlocked, SourceModifiedAt)
        SELECT inc.IncidentNumber, it.TypeCode, seg.SegmentCode, ev.UnitCode,
               w.ConditionCode,
               inc.DetectedAt, pr.DispatchedAt, pr.ArrivedAt, inc.RoadClearedAt, inc.ClosedAt,
               inc.Status, inc.Severity, inc.LanesBlocked, inc.ModifiedAt
        FROM TrafficOLTP.oltp.Incident inc
        JOIN TrafficOLTP.oltp.IncidentType it ON it.IncidentTypeID = inc.IncidentTypeID
        JOIN TrafficOLTP.oltp.RoadSegment seg ON seg.RoadSegmentID = inc.RoadSegmentID
        OUTER APPLY (SELECT TOP (1) p.DispatchedAt, p.ArrivedAt, p.EmergencyVehicleID
                     FROM TrafficOLTP.oltp.PoliceResponse p
                     WHERE p.IncidentID = inc.IncidentID
                     ORDER BY p.DispatchedAt) pr
        LEFT JOIN TrafficOLTP.oltp.EmergencyVehicle ev ON ev.EmergencyVehicleID = pr.EmergencyVehicleID
        OUTER APPLY (SELECT TOP (1) o.ConditionCode
                     FROM TrafficOLTP.oltp.WeatherObservation o
                     WHERE o.ObservedAt <= inc.DetectedAt
                     ORDER BY o.ObservedAt DESC) w
        WHERE inc.ModifiedAt > @wmInc AND inc.ModifiedAt <= @WatermarkUpper;
        SET @rows = @@ROWCOUNT;
        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted) VALUES (@ETLBatchID, 'Extract.IncidentLifecycle', @rows);

        /* watermarks are deliberately NOT advanced here — see usp_AdvanceWatermarks */
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

/* ============================================================================
   ADVANCE WATERMARKS — the restart-safety anchor.
   Called by usp_RunNightlyPipeline as the LAST step of a successful run,
   with the upper bound the extract actually used. Any failure before this
   point leaves LastWatermark unchanged, so the failed batch's OLTP delta is
   re-extracted on the next run instead of being skipped forever.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_AdvanceWatermarks
    @ETLBatchID     INT,
    @WatermarkUpper DATETIME2(3)
AS
BEGIN
    SET NOCOUNT ON;
    IF @WatermarkUpper IS NULL RETURN;   -- nothing was extracted this run

    DECLARE @rows INT;
    UPDATE etl.WatermarkControl
    SET LastWatermark = @WatermarkUpper, UpdatedAt = SYSUTCDATETIME();
    SET @rows = @@ROWCOUNT;

    INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsUpdated)
    VALUES (@ETLBatchID, 'AdvanceWatermarks', @rows);
END
GO

PRINT 'ETL framework and extraction created.';
GO

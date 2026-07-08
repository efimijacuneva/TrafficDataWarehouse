/* ============================================================================
   TrafficDW — dimension loading (SCD Types 0/1/2/3, hash-diff, inferred members)
   See docs/05_scd_strategy.md for the full rationale.
   ========================================================================== */
USE TrafficDW;
GO

/* ============================================================================
   DimWeatherCondition — banded dimension, seeded by cross join (SCD 0/1).
   The ETL never changes rows; new combinations would be added Type-1 style.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_SeedDimWeatherCondition
AS
BEGIN
    SET NOCOUNT ON;
    ;WITH cond AS (SELECT * FROM (VALUES
            ('DRY','Dry',0),('RAIN','Rain',0),('SNOW','Snow',1),
            ('FOG','Fog',1),('ICE','Ice',1),('STORM','Storm',1)) c(Code, Name, Severe)),
    temp AS (SELECT * FROM (VALUES ('Below -5'),('-5 to 5'),('5 to 15'),('15 to 25'),('Above 25')) t(Band)),
    prec AS (SELECT * FROM (VALUES ('None'),('Light'),('Moderate'),('Heavy')) p(Band)),
    vis  AS (SELECT * FROM (VALUES ('Good'),('Reduced'),('Poor')) v(Band))
    INSERT INTO dim.DimWeatherCondition (ConditionCode, ConditionName, TempBand, PrecipBand, VisibilityBand, IsSevere)
    SELECT c.Code, c.Name, t.Band, p.Band, v.Band,
           CASE WHEN c.Severe = 1 OR p.Band = 'Heavy' OR v.Band = 'Poor' THEN 1 ELSE 0 END
    FROM cond c CROSS JOIN temp t CROSS JOIN prec p CROSS JOIN vis v
    WHERE NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition d
                      WHERE d.ConditionCode = c.Code AND d.TempBand = t.Band
                        AND d.PrecipBand = p.Band AND d.VisibilityBand = v.Band);

    /* one generic row per condition (bands unknown) — used by facts that only
       know the condition code, e.g. the incident lifecycle */
    INSERT INTO dim.DimWeatherCondition (ConditionCode, ConditionName, TempBand, PrecipBand, VisibilityBand, IsSevere)
    SELECT c.Code, c.Name, 'Unknown', 'Unknown', 'Unknown', c.Severe
    FROM (VALUES ('DRY','Dry',0),('RAIN','Rain',0),('SNOW','Snow',1),
                 ('FOG','Fog',1),('ICE','Ice',1),('STORM','Storm',1)) c(Code, Name, Severe)
    WHERE NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition d
                      WHERE d.ConditionCode = c.Code AND d.TempBand = 'Unknown'
                        AND d.PrecipBand = 'Unknown' AND d.VisibilityBand = 'Unknown');
END
GO

/* Shared banding logic — single definition so Spark loads and SQL loads band
   identically (mirrored in spark/utils/quality.py). */
CREATE OR ALTER FUNCTION etl.fn_WeatherBands (
    @TempC DECIMAL(4,1), @PrecipMm DECIMAL(5,1), @VisibilityM INT)
RETURNS TABLE
AS RETURN
SELECT
    CASE WHEN @TempC IS NULL   THEN 'Unknown'
         WHEN @TempC < -5      THEN 'Below -5'
         WHEN @TempC < 5       THEN '-5 to 5'
         WHEN @TempC < 15      THEN '5 to 15'
         WHEN @TempC < 25      THEN '15 to 25'
         ELSE 'Above 25' END AS TempBand,
    CASE WHEN @PrecipMm IS NULL OR @PrecipMm = 0 THEN 'None'
         WHEN @PrecipMm < 2.5  THEN 'Light'
         WHEN @PrecipMm < 7.5  THEN 'Moderate'
         ELSE 'Heavy' END AS PrecipBand,
    CASE WHEN @VisibilityM IS NULL OR @VisibilityM >= 2000 THEN 'Good'
         WHEN @VisibilityM >= 500 THEN 'Reduced'
         ELSE 'Poor' END AS VisibilityBand;
GO

/* ============================================================================
   SCD TYPE 1 — overwrite. Pattern: MERGE upsert, no history columns.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_LoadDimVehicleType
    @ETLBatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE dim.DimVehicleType AS tgt
        USING (SELECT TypeCode, TypeName, Category, IsHeavy FROM stg.VehicleType) AS src
           ON tgt.TypeCode = src.TypeCode
        WHEN MATCHED AND (tgt.TypeName <> src.TypeName
                       OR tgt.Category <> src.Category
                       OR tgt.IsHeavy  <> src.IsHeavy) THEN
            UPDATE SET TypeName   = src.TypeName,      -- Type 1: history destroyed by design
                       Category   = src.Category,
                       IsHeavy    = src.IsHeavy,
                       ETLBatchID = @ETLBatchID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (TypeCode, TypeName, Category, IsHeavy, ETLBatchID)
            VALUES (src.TypeCode, src.TypeName, src.Category, src.IsHeavy, @ETLBatchID);

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted)
        VALUES (@ETLBatchID, 'Dim.VehicleType', (SELECT COUNT(*) FROM stg.VehicleType));
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl.usp_LoadDimTrafficCamera
    @ETLBatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE dim.DimTrafficCamera AS tgt
        USING stg.TrafficCamera AS src
           ON tgt.CameraCode = src.CameraCode
        WHEN MATCHED THEN
            UPDATE SET IntersectionName = src.IntersectionName,
                       Model            = src.Model,
                       Resolution       = src.Resolution,      -- Type 1: upgrades overwrite
                       FirmwareVersion  = src.FirmwareVersion, -- Type 1
                       Status           = src.Status,
                       ETLBatchID       = @ETLBatchID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (CameraCode, IntersectionName, Model, Resolution, FirmwareVersion, Status, ETLBatchID)
            VALUES (src.CameraCode, src.IntersectionName, src.Model, src.Resolution, src.FirmwareVersion, src.Status, @ETLBatchID);

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted)
        VALUES (@ETLBatchID, 'Dim.TrafficCamera', (SELECT COUNT(*) FROM stg.TrafficCamera));
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl.usp_LoadDimIncidentType
    @ETLBatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE dim.DimIncidentType AS tgt
        USING stg.IncidentType AS src ON tgt.TypeCode = src.TypeCode
        WHEN MATCHED THEN
            UPDATE SET TypeName = src.TypeName, Category = src.Category,
                       DefaultSeverity = src.DefaultSeverity, ETLBatchID = @ETLBatchID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (TypeCode, TypeName, Category, DefaultSeverity, ETLBatchID)
            VALUES (src.TypeCode, src.TypeName, src.Category, src.DefaultSeverity, @ETLBatchID);

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted)
        VALUES (@ETLBatchID, 'Dim.IncidentType', (SELECT COUNT(*) FROM stg.IncidentType));
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE etl.usp_LoadDimEmergencyUnit
    @ETLBatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE dim.DimEmergencyUnit AS tgt
        USING stg.EmergencyUnit AS src ON tgt.UnitCode = src.UnitCode
        WHEN MATCHED THEN
            UPDATE SET UnitType = src.UnitType, HomeStation = src.HomeStation,
                       VehicleMake = src.VehicleMake, VehicleModel = src.VehicleModel,
                       ETLBatchID = @ETLBatchID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (UnitCode, UnitType, HomeStation, VehicleMake, VehicleModel, ETLBatchID)
            VALUES (src.UnitCode, src.UnitType, src.HomeStation, src.VehicleMake, src.VehicleModel, @ETLBatchID);

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted)
        VALUES (@ETLBatchID, 'Dim.EmergencyUnit', (SELECT COUNT(*) FROM stg.EmergencyUnit));
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

/* ============================================================================
   SCD TYPE 2 — dim.DimRoadSegment
   Pattern: MERGE with hash-diff change detection.
     - brand-new BK                      → INSERT version 1
     - existing BK, IsInferred = 1      → Type-1 overwrite of the skeleton row
                                           (late-arriving dimension completion)
     - existing BK, HashDiff changed    → expire current row; the OUTPUT of the
                                           MERGE feeds the INSERT of the new version
   Also demonstrates:
     - Type 0 attribute: OriginalSpeedLimitKmh carried over, never re-derived
     - Type 6 column:    CurrentSpeedLimitKmh overwritten on ALL versions
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_LoadDimRoadSegment
    @ETLBatchID INT,
    @LoadDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRAN;

        /* source with computed hash over the Type-2 attribute set */
        ;WITH src AS (
            SELECT s.*,
                   HASHBYTES('SHA2_256', CONCAT_WS('|',
                        s.RoadCode, s.RoadName, s.RoadCategory, s.City, s.District,
                        s.Direction, s.StartIntersection, s.EndIntersection,
                        s.LengthM, s.LaneCount, s.SpeedLimitKmh)) AS HashDiff
            FROM stg.RoadSegment s
        )
        SELECT * INTO #src FROM src;

        /* late-arriving completion FIRST (separate statement — T-SQL MERGE only
           allows one UPDATE-matched branch): overwrite inferred skeletons in
           place, Type-1 style, so the MERGE below sees them as ordinary rows */
        UPDATE tgt
        SET    RoadCode = src.RoadCode, RoadName = src.RoadName,
               RoadCategory = src.RoadCategory, City = src.City, District = src.District,
               Direction = src.Direction,
               StartIntersection = src.StartIntersection, EndIntersection = src.EndIntersection,
               Latitude = src.Latitude, Longitude = src.Longitude,
               LengthM = src.LengthM, LaneCount = src.LaneCount,
               SpeedLimitKmh = src.SpeedLimitKmh,
               CurrentSpeedLimitKmh = src.SpeedLimitKmh,
               OriginalSpeedLimitKmh = src.SpeedLimitKmh,
               HashDiff = src.HashDiff, IsInferred = 0, ETLBatchID = @ETLBatchID
        FROM   dim.DimRoadSegment tgt
        JOIN   #src src ON src.SegmentCode = tgt.SegmentCode
        WHERE  tgt.IsCurrent = 1 AND tgt.IsInferred = 1;

        DECLARE @actions TABLE (MergeAction NVARCHAR(10), SegmentCode VARCHAR(30),
                                OldVersion INT, OldOriginalLimit SMALLINT);

        MERGE dim.DimRoadSegment AS tgt
        USING #src AS src
           ON tgt.SegmentCode = src.SegmentCode AND tgt.IsCurrent = 1
        /* genuine change: expire the current version */
        WHEN MATCHED AND tgt.HashDiff <> src.HashDiff THEN
            UPDATE SET IsCurrent = 0,
                       ExpirationDate = DATEADD(DAY, -1, @LoadDate),
                       ETLBatchID = @ETLBatchID
        /* brand-new business key: version 1 */
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SegmentCode, RoadCode, RoadName, RoadCategory, City, District, Direction,
                    StartIntersection, EndIntersection, Latitude, Longitude, LengthM, LaneCount,
                    SpeedLimitKmh, CurrentSpeedLimitKmh, OriginalSpeedLimitKmh,
                    EffectiveDate, ExpirationDate, IsCurrent, VersionNumber, IsInferred, HashDiff, ETLBatchID)
            VALUES (src.SegmentCode, src.RoadCode, src.RoadName, src.RoadCategory, src.City, src.District,
                    src.Direction, src.StartIntersection, src.EndIntersection, src.Latitude, src.Longitude,
                    src.LengthM, src.LaneCount,
                    src.SpeedLimitKmh, src.SpeedLimitKmh, src.SpeedLimitKmh,
                    @LoadDate, '9999-12-31', 1, 1, 0, src.HashDiff, @ETLBatchID)
        OUTPUT $action, ISNULL(inserted.SegmentCode, deleted.SegmentCode),
               deleted.VersionNumber, deleted.OriginalSpeedLimitKmh
        INTO @actions (MergeAction, SegmentCode, OldVersion, OldOriginalLimit);

        /* insert the NEW current version for every expired row.
           OriginalSpeedLimitKmh is Type 0: carried from the previous version. */
        INSERT INTO dim.DimRoadSegment
               (SegmentCode, RoadCode, RoadName, RoadCategory, City, District, Direction,
                StartIntersection, EndIntersection, Latitude, Longitude, LengthM, LaneCount,
                SpeedLimitKmh, CurrentSpeedLimitKmh, OriginalSpeedLimitKmh,
                EffectiveDate, ExpirationDate, IsCurrent, VersionNumber, IsInferred, HashDiff, ETLBatchID)
        SELECT src.SegmentCode, src.RoadCode, src.RoadName, src.RoadCategory, src.City, src.District,
               src.Direction, src.StartIntersection, src.EndIntersection, src.Latitude, src.Longitude,
               src.LengthM, src.LaneCount,
               src.SpeedLimitKmh, src.SpeedLimitKmh, a.OldOriginalLimit,
               @LoadDate, '9999-12-31', 1, a.OldVersion + 1, 0, src.HashDiff, @ETLBatchID
        FROM @actions a
        JOIN #src src ON src.SegmentCode = a.SegmentCode
        WHERE a.MergeAction = 'UPDATE' AND a.OldVersion IS NOT NULL
          /* exclude the inferred-member completions (updated in place, no new version) */
          AND NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment d
                          WHERE d.SegmentCode = a.SegmentCode AND d.IsCurrent = 1);

        /* Type 6: current-value column synchronized across ALL versions of a BK */
        UPDATE d
        SET    d.CurrentSpeedLimitKmh = cur.SpeedLimitKmh
        FROM   dim.DimRoadSegment d
        JOIN   dim.DimRoadSegment cur
               ON cur.SegmentCode = d.SegmentCode AND cur.IsCurrent = 1
        WHERE  d.CurrentSpeedLimitKmh <> cur.SpeedLimitKmh;

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted, RowsInserted, RowsUpdated)
        SELECT @ETLBatchID, 'Dim.RoadSegment (SCD2)',
               (SELECT COUNT(*) FROM #src),
               (SELECT COUNT(*) FROM @actions WHERE MergeAction = 'INSERT'),
               (SELECT COUNT(*) FROM @actions WHERE MergeAction = 'UPDATE');

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
   SCD TYPE 2 — dim.DimSensor  (same pattern; InstallDate is Type 0,
   FirmwareVersion deliberately excluded from the hash → Type 1 in place)
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_LoadDimSensor
    @ETLBatchID INT,
    @LoadDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRAN;

        ;WITH src AS (
            SELECT s.*,
                   HASHBYTES('SHA2_256', CONCAT_WS('|',
                        s.SensorTypeName, s.Technology, s.SegmentCode, s.StatusClass)) AS HashDiff
            FROM stg.Sensor s
        )
        SELECT * INTO #srcSensor FROM src;

        /* late-arriving completion first (separate statement, see DimRoadSegment) */
        UPDATE tgt
        SET    SensorTypeName = src.SensorTypeName, Technology = src.Technology,
               SegmentCode = src.SegmentCode, StatusClass = src.StatusClass,
               InstallDate = src.InstallDate,          -- first real value; never touched again
               FirmwareVersion = src.FirmwareVersion,
               HashDiff = src.HashDiff, IsInferred = 0, ETLBatchID = @ETLBatchID
        FROM   dim.DimSensor tgt
        JOIN   #srcSensor src ON src.SerialNumber = tgt.SerialNumber
        WHERE  tgt.IsCurrent = 1 AND tgt.IsInferred = 1;

        DECLARE @actions TABLE (MergeAction NVARCHAR(10), SerialNumber VARCHAR(30), OldVersion INT);

        MERGE dim.DimSensor AS tgt
        USING #srcSensor AS src
           ON tgt.SerialNumber = src.SerialNumber AND tgt.IsCurrent = 1
        WHEN MATCHED AND tgt.HashDiff <> src.HashDiff THEN
            UPDATE SET IsCurrent = 0,
                       ExpirationDate = DATEADD(DAY, -1, @LoadDate),
                       ETLBatchID = @ETLBatchID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SerialNumber, SensorTypeName, Technology, SegmentCode, StatusClass,
                    InstallDate, FirmwareVersion, EffectiveDate, ExpirationDate,
                    IsCurrent, VersionNumber, IsInferred, HashDiff, ETLBatchID)
            VALUES (src.SerialNumber, src.SensorTypeName, src.Technology, src.SegmentCode, src.StatusClass,
                    src.InstallDate, src.FirmwareVersion, @LoadDate, '9999-12-31',
                    1, 1, 0, src.HashDiff, @ETLBatchID)
        OUTPUT $action, ISNULL(inserted.SerialNumber, deleted.SerialNumber), deleted.VersionNumber
        INTO @actions (MergeAction, SerialNumber, OldVersion);

        INSERT INTO dim.DimSensor
               (SerialNumber, SensorTypeName, Technology, SegmentCode, StatusClass,
                InstallDate, FirmwareVersion, EffectiveDate, ExpirationDate,
                IsCurrent, VersionNumber, IsInferred, HashDiff, ETLBatchID)
        SELECT src.SerialNumber, src.SensorTypeName, src.Technology, src.SegmentCode, src.StatusClass,
               d0.InstallDate,                                  -- Type 0: from first version
               src.FirmwareVersion, @LoadDate, '9999-12-31',
               1, a.OldVersion + 1, 0, src.HashDiff, @ETLBatchID
        FROM @actions a
        JOIN #srcSensor src ON src.SerialNumber = a.SerialNumber
        CROSS APPLY (SELECT TOP (1) InstallDate FROM dim.DimSensor d
                     WHERE d.SerialNumber = a.SerialNumber ORDER BY d.VersionNumber) d0
        WHERE a.MergeAction = 'UPDATE' AND a.OldVersion IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM dim.DimSensor d
                          WHERE d.SerialNumber = a.SerialNumber AND d.IsCurrent = 1);

        /* Type 1 attribute on a Type 2 dimension: firmware is technical detail,
           excluded from HashDiff → overwritten on the current row, no versioning */
        UPDATE tgt
        SET    FirmwareVersion = src.FirmwareVersion, ETLBatchID = @ETLBatchID
        FROM   dim.DimSensor tgt
        JOIN   #srcSensor src ON src.SerialNumber = tgt.SerialNumber
        WHERE  tgt.IsCurrent = 1
          AND  ISNULL(tgt.FirmwareVersion,'') <> ISNULL(src.FirmwareVersion,'');

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted, RowsInserted, RowsUpdated)
        SELECT @ETLBatchID, 'Dim.Sensor (SCD2)',
               (SELECT COUNT(*) FROM #srcSensor),
               (SELECT COUNT(*) FROM @actions WHERE MergeAction = 'INSERT'),
               (SELECT COUNT(*) FROM @actions WHERE MergeAction = 'UPDATE');
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
   SCD TYPE 3 — dim.DimTrafficLight
   Only the timing plan gets 'previous value' semantics; other attributes Type 1.
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_LoadDimTrafficLight
    @ETLBatchID INT,
    @LoadDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE dim.DimTrafficLight AS tgt
        USING stg.TrafficLight AS src
           ON tgt.ControllerCode = src.ControllerCode
        /* Type 3 shift: current → previous, new value becomes current */
        WHEN MATCHED AND tgt.CurrentTimingPlan <> src.TimingPlan THEN
            UPDATE SET PreviousTimingPlan   = tgt.CurrentTimingPlan,
                       CurrentTimingPlan    = src.TimingPlan,
                       TimingPlanChangeDate = @LoadDate,
                       IntersectionName     = src.IntersectionName,
                       CycleSeconds         = src.CycleSeconds,
                       Status               = src.Status,
                       ETLBatchID           = @ETLBatchID
        WHEN MATCHED THEN
            UPDATE SET IntersectionName = src.IntersectionName,   -- non-Type-3 attributes: Type 1
                       CycleSeconds     = src.CycleSeconds,
                       Status           = src.Status,
                       ETLBatchID       = @ETLBatchID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (ControllerCode, IntersectionName, CurrentTimingPlan, PreviousTimingPlan,
                    TimingPlanChangeDate, CycleSeconds, Status, ETLBatchID)
            VALUES (src.ControllerCode, src.IntersectionName, src.TimingPlan, NULL,
                    NULL, src.CycleSeconds, src.Status, @ETLBatchID);

        INSERT INTO etl.RowLog (ETLBatchID, StepName, RowsExtracted)
        VALUES (@ETLBatchID, 'Dim.TrafficLight (SCD3)', (SELECT COUNT(*) FROM stg.TrafficLight));
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError @ETLBatchID;
        THROW;
    END CATCH
END
GO

/* ============================================================================
   Master dimension load — dependency-ordered
   ========================================================================== */
CREATE OR ALTER PROCEDURE etl.usp_LoadAllDimensions
    @ETLBatchID INT,
    @LoadDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    EXEC etl.usp_SeedDimWeatherCondition;
    EXEC etl.usp_LoadDimVehicleType   @ETLBatchID;
    EXEC etl.usp_LoadDimIncidentType  @ETLBatchID;
    EXEC etl.usp_LoadDimEmergencyUnit @ETLBatchID;
    EXEC etl.usp_LoadDimTrafficCamera @ETLBatchID;
    EXEC etl.usp_LoadDimRoadSegment   @ETLBatchID, @LoadDate;
    EXEC etl.usp_LoadDimSensor        @ETLBatchID, @LoadDate;
    EXEC etl.usp_LoadDimTrafficLight  @ETLBatchID, @LoadDate;
END
GO

PRINT 'Dimension load procedures created.';
GO

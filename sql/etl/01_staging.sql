/* ============================================================================
   TrafficDW — staging layer
   Contract: truncate-and-load per batch, typed columns, NO constraints/indexes
   (bulk-load speed; validation happens in the load procs & Spark).
   Two producers write here:
     - T-SQL extraction procs (OLTP reference data, incremental by watermark)
     - Spark job 05 via JDBC (gold traffic events / hourly aggregates)
   ========================================================================== */
USE TrafficDW;
GO

/* NOTE ON RE-RUNNING: every CREATE TABLE below is guarded so this file can be
   deployed against an existing database without erroring on the second run -
   the procedures already use CREATE OR ALTER. The guard has one consequence to
   be aware of: it also means a CHANGED column definition will NOT be applied to
   an existing table. Schema changes therefore require either an explicit ALTER
   (see the Rationale column in 05_quality_checks.sql) or a fresh deploy, which
   is what scripts/run_end_to_end.ps1 does by dropping both databases first. */

/* ------------------------------------------------ reference data from OLTP */
IF OBJECT_ID('stg.RoadSegment') IS NULL
CREATE TABLE stg.RoadSegment (
    SegmentCode        VARCHAR(30),
    RoadCode           VARCHAR(20),
    RoadName           NVARCHAR(150),
    RoadCategory       VARCHAR(20),
    City               NVARCHAR(100),
    District           NVARCHAR(100),
    Direction          CHAR(2),
    StartIntersection  NVARCHAR(150),
    EndIntersection    NVARCHAR(150),
    Latitude           DECIMAL(9,6),
    Longitude          DECIMAL(9,6),
    LengthM            INT,
    LaneCount          TINYINT,
    SpeedLimitKmh      SMALLINT,
    SourceModifiedAt   DATETIME2(3)
);

IF OBJECT_ID('stg.Sensor') IS NULL
CREATE TABLE stg.Sensor (
    SerialNumber     VARCHAR(30),
    SensorTypeName   NVARCHAR(50),
    Technology       NVARCHAR(50),
    SegmentCode      VARCHAR(30),
    StatusClass      VARCHAR(20),
    InstallDate      DATE,
    FirmwareVersion  VARCHAR(20),
    SourceModifiedAt DATETIME2(3)
);

IF OBJECT_ID('stg.VehicleType') IS NULL
CREATE TABLE stg.VehicleType (
    TypeCode VARCHAR(10), TypeName NVARCHAR(50), Category VARCHAR(20), IsHeavy BIT,
    SourceModifiedAt DATETIME2(3)
);

IF OBJECT_ID('stg.TrafficCamera') IS NULL
CREATE TABLE stg.TrafficCamera (
    CameraCode VARCHAR(20), IntersectionName NVARCHAR(150), Model NVARCHAR(80),
    Resolution VARCHAR(20), FirmwareVersion VARCHAR(20), Status VARCHAR(20),
    SourceModifiedAt DATETIME2(3)
);

IF OBJECT_ID('stg.TrafficLight') IS NULL
CREATE TABLE stg.TrafficLight (
    ControllerCode VARCHAR(20), IntersectionName NVARCHAR(150),
    TimingPlan VARCHAR(30), CycleSeconds SMALLINT, Status VARCHAR(20),
    SourceModifiedAt DATETIME2(3)
);

IF OBJECT_ID('stg.IncidentType') IS NULL
CREATE TABLE stg.IncidentType (
    TypeCode VARCHAR(10), TypeName NVARCHAR(80), Category VARCHAR(20), DefaultSeverity TINYINT,
    SourceModifiedAt DATETIME2(3)
);

IF OBJECT_ID('stg.EmergencyUnit') IS NULL
CREATE TABLE stg.EmergencyUnit (
    UnitCode VARCHAR(20), UnitType VARCHAR(20), HomeStation NVARCHAR(100),
    VehicleMake NVARCHAR(50), VehicleModel NVARCHAR(50),
    SourceModifiedAt DATETIME2(3)
);

/* -------------------------------------- incident lifecycle milestones (OLTP) */
IF OBJECT_ID('stg.IncidentLifecycle') IS NULL
CREATE TABLE stg.IncidentLifecycle (
    IncidentNumber   VARCHAR(20),
    IncidentTypeCode VARCHAR(10),
    SegmentCode      VARCHAR(30),
    UnitCode         VARCHAR(20),
    ConditionCode    VARCHAR(10),          -- weather at detection (joined in extract)
    DetectedAt       DATETIME2(0),
    DispatchedAt     DATETIME2(0),
    ArrivedAt        DATETIME2(0),
    RoadClearedAt    DATETIME2(0),
    ClosedAt         DATETIME2(0),
    CurrentStatus    VARCHAR(20),
    Severity         TINYINT,
    LanesBlocked     TINYINT,
    SourceModifiedAt DATETIME2(3)
);

/* ------------------------------------------------ high-volume feeds (Spark) */
/* DetectorType tells the fact load which asset dimension to resolve:
   'SENSOR' → SensorSerial populated, CameraCode NULL
   'CAMERA' → CameraCode   populated, SensorSerial NULL
   (spark/jobs/02_clean_validate.py conforms both feeds to this shape) */
IF OBJECT_ID('stg.TrafficEvent') IS NULL
CREATE TABLE stg.TrafficEvent (
    EventID          VARCHAR(40),
    EventTimestamp   DATETIME2(3),
    DetectorType     VARCHAR(10),
    SensorSerial     VARCHAR(30),
    CameraCode       VARCHAR(20),
    SegmentCode      VARCHAR(30),
    VehicleTypeCode  VARCHAR(10),
    SpeedKmh         DECIMAL(5,1),
    HeadwaySeconds   DECIMAL(6,2),
    OccupancyPct     DECIMAL(5,2),
    ConditionCode    VARCHAR(10),
    TempC            DECIMAL(4,1),
    PrecipitationMm  DECIMAL(5,1),
    VisibilityM      INT,
    LoadDate         DATE
);

IF OBJECT_ID('stg.HourlyTraffic') IS NULL
CREATE TABLE stg.HourlyTraffic (
    EventDate        DATE,
    HourOfDay        TINYINT,
    SegmentCode      VARCHAR(30),
    VehicleCount     INT,
    HeavyVehicleCount INT,
    AvgSpeedKmh      DECIMAL(5,1),
    P85SpeedKmh      DECIMAL(5,1),
    AvgOccupancyPct  DECIMAL(5,2),
    ConditionCode    VARCHAR(10),
    TempBandSource   DECIMAL(4,1),          -- avg temp (banded during load)
    PrecipitationMm  DECIMAL(5,1),
    VisibilityM      INT
);

/* --------------------------------------------------------- reject / replay */
IF OBJECT_ID('stg.RejectTrafficEvent') IS NULL
CREATE TABLE stg.RejectTrafficEvent (
    RejectID      BIGINT IDENTITY(1,1) PRIMARY KEY,
    ETLBatchID    INT,
    RejectReason  VARCHAR(100),
    EventID       VARCHAR(40),
    EventTimestamp DATETIME2(3),
    SensorSerial  VARCHAR(30),
    SegmentCode   VARCHAR(30),
    RawPayload    NVARCHAR(1000),
    RejectedAt    DATETIME2(3) DEFAULT SYSUTCDATETIME()
);

/* --------------------------------------------- incremental-extract control */
IF OBJECT_ID('etl.WatermarkControl') IS NULL
CREATE TABLE etl.WatermarkControl (
    SourceTable    SYSNAME       NOT NULL PRIMARY KEY,
    LastWatermark  DATETIME2(3)  NOT NULL CONSTRAINT DF_WM_Last DEFAULT '1900-01-01',
    UpdatedAt      DATETIME2(3)  NOT NULL CONSTRAINT DF_WM_Upd  DEFAULT SYSUTCDATETIME()
);

INSERT INTO etl.WatermarkControl (SourceTable)
SELECT t FROM (VALUES ('oltp.RoadSegment'),('oltp.Sensor'),('oltp.VehicleType'),
                      ('oltp.TrafficCamera'),('oltp.TrafficLight'),('oltp.IncidentType'),
                      ('oltp.EmergencyVehicle'),('oltp.Incident')) v(t)
WHERE NOT EXISTS (SELECT 1 FROM etl.WatermarkControl w WHERE w.SourceTable = v.t);
GO

PRINT 'Staging layer created.';
GO

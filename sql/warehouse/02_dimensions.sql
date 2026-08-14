/* ============================================================================
   TrafficDW — dimension tables
   SCD assignment (see docs/05_scd_strategy.md):
     Type 0: DimDate, DimTime (+ Type-0 attributes elsewhere: InstallDate, Original*)
     Type 1: DimVehicleType, DimTrafficCamera, DimIncidentType, DimEmergencyUnit,
             DimWeatherCondition
     Type 2: DimRoadSegment, DimSensor        (full history, hash-diff detection)
     Type 3: DimTrafficLight                  (Current/Previous timing plan)
     Type 6 demo column: DimRoadSegment.CurrentSpeedLimitKmh
   Every dimension has an Unknown member with surrogate key -1 (seeded below).
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

/* --------------------------------------------------------- DimDate (SCD 0) */
IF OBJECT_ID('dim.DimDate') IS NULL
CREATE TABLE dim.DimDate (
    DateKey        INT          NOT NULL CONSTRAINT PK_DimDate PRIMARY KEY,  -- yyyymmdd
    FullDate       DATE         NULL,
    [Year]         SMALLINT     NOT NULL,
    [Quarter]      TINYINT      NOT NULL,
    [MonthNumber]  TINYINT      NOT NULL,
    MonthName      VARCHAR(15)  NOT NULL,
    YearMonth      CHAR(7)      NOT NULL,       -- '2026-06'
    WeekOfYear     TINYINT      NOT NULL,
    DayOfMonth     TINYINT      NOT NULL,
    DayOfWeek      TINYINT      NOT NULL,       -- 1 = Monday (ISO)
    DayName        VARCHAR(15)  NOT NULL,
    IsWeekend      BIT          NOT NULL,
    IsHoliday      BIT          NOT NULL CONSTRAINT DF_DimDate_Holiday DEFAULT 0,
    HolidayName    NVARCHAR(60) NULL,
    Season         VARCHAR(10)  NOT NULL        -- Winter/Spring/Summer/Autumn
);

/* ------------------------------------------------ DimTime (SCD 0, minute grain) */
IF OBJECT_ID('dim.DimTime') IS NULL
CREATE TABLE dim.DimTime (
    TimeKey     SMALLINT    NOT NULL CONSTRAINT PK_DimTime PRIMARY KEY,      -- 0..1439
    TimeBK      CHAR(5)     NOT NULL,        -- 'HH:MM'
    [Hour]      TINYINT     NOT NULL,
    [Minute]    TINYINT     NOT NULL,
    HourBand    CHAR(13)    NOT NULL,        -- '07:00 - 08:00'
    DayPart     VARCHAR(10) NOT NULL,        -- Night/Morning/Midday/Afternoon/Evening
    IsRushHour  BIT         NOT NULL         -- 07:00-09:00, 16:00-18:30
);

/* --------------------------------------------- DimRoadSegment (SCD 2, flattened) */
IF OBJECT_ID('dim.DimRoadSegment') IS NULL
CREATE TABLE dim.DimRoadSegment (
    RoadSegmentKey     INT IDENTITY(1,1) CONSTRAINT PK_DimRoadSegment PRIMARY KEY,
    SegmentCode        VARCHAR(30)   NOT NULL,          -- business key
    RoadCode           VARCHAR(20)   NOT NULL,
    RoadName           NVARCHAR(150) NOT NULL,          -- denormalized (no snowflake)
    RoadCategory       VARCHAR(20)   NOT NULL,
    City               NVARCHAR(100) NOT NULL,
    District           NVARCHAR(100) NOT NULL,
    Direction          CHAR(2)       NOT NULL,
    StartIntersection  NVARCHAR(150) NOT NULL,
    EndIntersection    NVARCHAR(150) NOT NULL,
    Latitude           DECIMAL(9,6)  NULL,
    Longitude          DECIMAL(9,6)  NULL,
    LengthM            INT           NOT NULL,
    LaneCount          TINYINT       NOT NULL,
    SpeedLimitKmh      SMALLINT      NOT NULL,          -- Type 2: frozen per version
    CurrentSpeedLimitKmh SMALLINT    NOT NULL,          -- Type 6 demo: overwritten on ALL versions
    OriginalSpeedLimitKmh SMALLINT   NOT NULL,          -- Type 0: never changes
    /* --- SCD 2 housekeeping --- */
    EffectiveDate      DATE          NOT NULL,
    ExpirationDate     DATE          NOT NULL CONSTRAINT DF_DimSeg_Exp DEFAULT '9999-12-31',
    IsCurrent          BIT           NOT NULL CONSTRAINT DF_DimSeg_Cur DEFAULT 1,
    VersionNumber      INT           NOT NULL CONSTRAINT DF_DimSeg_Ver DEFAULT 1,
    IsInferred         BIT           NOT NULL CONSTRAINT DF_DimSeg_Inf DEFAULT 0,
    HashDiff           BINARY(32)    NOT NULL,
    ETLBatchID         INT           NULL
);
CREATE UNIQUE INDEX UXF_DimSeg_CurrentBK ON dim.DimRoadSegment (SegmentCode) WHERE IsCurrent = 1;
CREATE INDEX IX_DimSeg_BK_Validity ON dim.DimRoadSegment (SegmentCode, EffectiveDate, ExpirationDate);

/* ---------------------------------------------------------- DimSensor (SCD 2) */
IF OBJECT_ID('dim.DimSensor') IS NULL
CREATE TABLE dim.DimSensor (
    SensorKey       INT IDENTITY(1,1) CONSTRAINT PK_DimSensor PRIMARY KEY,
    SerialNumber    VARCHAR(30)   NOT NULL,             -- business key
    SensorTypeName  NVARCHAR(50)  NOT NULL,
    Technology      NVARCHAR(50)  NOT NULL,
    SegmentCode     VARCHAR(30)   NOT NULL,             -- Type 2: relocation creates version
    StatusClass     VARCHAR(20)   NOT NULL,             -- Type 2 (slow status class, not telemetry)
    InstallDate     DATE          NULL,                 -- Type 0: never updated after first load
    FirmwareVersion VARCHAR(20)   NULL,                 -- Type 1-in-place (technical, no history)
    EffectiveDate   DATE          NOT NULL,
    ExpirationDate  DATE          NOT NULL CONSTRAINT DF_DimSensor_Exp DEFAULT '9999-12-31',
    IsCurrent       BIT           NOT NULL CONSTRAINT DF_DimSensor_Cur DEFAULT 1,
    VersionNumber   INT           NOT NULL CONSTRAINT DF_DimSensor_Ver DEFAULT 1,
    IsInferred      BIT           NOT NULL CONSTRAINT DF_DimSensor_Inf DEFAULT 0,
    HashDiff        BINARY(32)    NOT NULL,
    ETLBatchID      INT           NULL
);
CREATE UNIQUE INDEX UXF_DimSensor_CurrentBK ON dim.DimSensor (SerialNumber) WHERE IsCurrent = 1;
CREATE INDEX IX_DimSensor_BK_Validity ON dim.DimSensor (SerialNumber, EffectiveDate, ExpirationDate);

/* ---------------------------------------------------- DimVehicleType (SCD 1) */
IF OBJECT_ID('dim.DimVehicleType') IS NULL
CREATE TABLE dim.DimVehicleType (
    VehicleTypeKey INT IDENTITY(1,1) CONSTRAINT PK_DimVehicleType PRIMARY KEY,
    TypeCode       VARCHAR(10)  NOT NULL,               -- business key
    TypeName       NVARCHAR(50) NOT NULL,
    Category       VARCHAR(20)  NOT NULL,
    IsHeavy        BIT          NOT NULL,
    ETLBatchID     INT          NULL
);
CREATE UNIQUE INDEX UX_DimVehicleType_BK ON dim.DimVehicleType (TypeCode);

/* ----------------------------------------- DimWeatherCondition (banded, SCD 0/1) */
IF OBJECT_ID('dim.DimWeatherCondition') IS NULL
CREATE TABLE dim.DimWeatherCondition (
    WeatherKey     INT IDENTITY(1,1) CONSTRAINT PK_DimWeather PRIMARY KEY,
    ConditionCode  VARCHAR(10) NOT NULL,   -- DRY/RAIN/SNOW/FOG/ICE/STORM
    ConditionName  VARCHAR(30) NOT NULL,
    TempBand       VARCHAR(20) NOT NULL,   -- 'Below -5' / '-5 to 5' / '5 to 15' / '15 to 25' / 'Above 25'
    PrecipBand     VARCHAR(20) NOT NULL,   -- 'None' / 'Light' / 'Moderate' / 'Heavy'
    VisibilityBand VARCHAR(20) NOT NULL,   -- 'Good' / 'Reduced' / 'Poor'
    IsSevere       BIT         NOT NULL
);
CREATE UNIQUE INDEX UX_DimWeather_BK
    ON dim.DimWeatherCondition (ConditionCode, TempBand, PrecipBand, VisibilityBand);

/* --------------------------------------------------- DimTrafficCamera (SCD 1) */
IF OBJECT_ID('dim.DimTrafficCamera') IS NULL
CREATE TABLE dim.DimTrafficCamera (
    CameraKey       INT IDENTITY(1,1) CONSTRAINT PK_DimCamera PRIMARY KEY,
    CameraCode      VARCHAR(20)   NOT NULL,             -- business key
    IntersectionName NVARCHAR(150) NOT NULL,
    Model           NVARCHAR(80)  NULL,
    Resolution      VARCHAR(20)   NULL,                 -- Type 1: overwrite on upgrade
    FirmwareVersion VARCHAR(20)   NULL,                 -- Type 1
    Status          VARCHAR(20)   NOT NULL,
    ETLBatchID      INT           NULL
);
CREATE UNIQUE INDEX UX_DimCamera_BK ON dim.DimTrafficCamera (CameraCode);

/* ---------------------------------------------------- DimTrafficLight (SCD 3) */
IF OBJECT_ID('dim.DimTrafficLight') IS NULL
CREATE TABLE dim.DimTrafficLight (
    TrafficLightKey      INT IDENTITY(1,1) CONSTRAINT PK_DimLight PRIMARY KEY,
    ControllerCode       VARCHAR(20)   NOT NULL,        -- business key
    IntersectionName     NVARCHAR(150) NOT NULL,
    CurrentTimingPlan    VARCHAR(30)   NOT NULL,        -- Type 3 pair
    PreviousTimingPlan   VARCHAR(30)   NULL,            --   "
    TimingPlanChangeDate DATE          NULL,
    CycleSeconds         SMALLINT      NOT NULL,
    Status               VARCHAR(20)   NOT NULL,
    ETLBatchID           INT           NULL
);
CREATE UNIQUE INDEX UX_DimLight_BK ON dim.DimTrafficLight (ControllerCode);

/* ---------------------------------------------------- DimIncidentType (SCD 1) */
IF OBJECT_ID('dim.DimIncidentType') IS NULL
CREATE TABLE dim.DimIncidentType (
    IncidentTypeKey INT IDENTITY(1,1) CONSTRAINT PK_DimIncidentType PRIMARY KEY,
    TypeCode        VARCHAR(10)  NOT NULL,              -- business key
    TypeName        NVARCHAR(80) NOT NULL,
    Category        VARCHAR(20)  NOT NULL,
    DefaultSeverity TINYINT      NOT NULL,
    ETLBatchID      INT          NULL
);
CREATE UNIQUE INDEX UX_DimIncidentType_BK ON dim.DimIncidentType (TypeCode);

/* --------------------------------------------------- DimEmergencyUnit (SCD 1) */
IF OBJECT_ID('dim.DimEmergencyUnit') IS NULL
CREATE TABLE dim.DimEmergencyUnit (
    EmergencyUnitKey INT IDENTITY(1,1) CONSTRAINT PK_DimEmergencyUnit PRIMARY KEY,
    UnitCode        VARCHAR(20)   NOT NULL,             -- business key
    UnitType        VARCHAR(20)   NOT NULL,
    HomeStation     NVARCHAR(100) NOT NULL,
    VehicleMake     NVARCHAR(50)  NULL,
    VehicleModel    NVARCHAR(50)  NULL,
    ETLBatchID      INT           NULL
);
CREATE UNIQUE INDEX UX_DimEmergencyUnit_BK ON dim.DimEmergencyUnit (UnitCode);
GO

/* ============================================================================
   Unknown members (surrogate key -1) — idempotent seeding.
   Facts reference -1 whenever the business key is missing/unresolvable, so
   inner joins never drop rows and reports show an explicit 'Unknown' bucket.
   ========================================================================== */
IF NOT EXISTS (SELECT 1 FROM dim.DimDate WHERE DateKey = -1)
    INSERT INTO dim.DimDate (DateKey, FullDate, [Year], [Quarter], [MonthNumber], MonthName,
                             YearMonth, WeekOfYear, DayOfMonth, DayOfWeek, DayName, IsWeekend, IsHoliday, Season)
    VALUES (-1, NULL, 1900, 1, 1, 'Unknown', '1900-01', 1, 1, 1, 'Unknown', 0, 0, 'Unknown');

IF NOT EXISTS (SELECT 1 FROM dim.DimTime WHERE TimeKey = -1)
    INSERT INTO dim.DimTime (TimeKey, TimeBK, [Hour], [Minute], HourBand, DayPart, IsRushHour)
    VALUES (-1, '??:??', 0, 0, 'Unknown      ', 'Unknown', 0);

IF NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment WHERE RoadSegmentKey = -1)
BEGIN
    SET IDENTITY_INSERT dim.DimRoadSegment ON;
    INSERT INTO dim.DimRoadSegment (RoadSegmentKey, SegmentCode, RoadCode, RoadName, RoadCategory,
        City, District, Direction, StartIntersection, EndIntersection, LengthM, LaneCount,
        SpeedLimitKmh, CurrentSpeedLimitKmh, OriginalSpeedLimitKmh,
        EffectiveDate, ExpirationDate, IsCurrent, VersionNumber, IsInferred, HashDiff)
    VALUES (-1, 'UNKNOWN', 'UNKNOWN', N'Unknown', 'Local', N'Unknown', N'Unknown', 'NB',
        N'Unknown', N'Unknown', 0, 1, 50, 50, 50, '1900-01-01', '9999-12-31', 1, 1, 0, 0x0);
    SET IDENTITY_INSERT dim.DimRoadSegment OFF;
END

IF NOT EXISTS (SELECT 1 FROM dim.DimSensor WHERE SensorKey = -1)
BEGIN
    SET IDENTITY_INSERT dim.DimSensor ON;
    INSERT INTO dim.DimSensor (SensorKey, SerialNumber, SensorTypeName, Technology, SegmentCode,
        StatusClass, InstallDate, FirmwareVersion, EffectiveDate, ExpirationDate, IsCurrent,
        VersionNumber, IsInferred, HashDiff)
    VALUES (-1, 'UNKNOWN', N'Unknown', N'Unknown', 'UNKNOWN', 'Unknown', NULL, NULL,
        '1900-01-01', '9999-12-31', 1, 1, 0, 0x0);
    SET IDENTITY_INSERT dim.DimSensor OFF;
END

IF NOT EXISTS (SELECT 1 FROM dim.DimVehicleType WHERE VehicleTypeKey = -1)
BEGIN
    SET IDENTITY_INSERT dim.DimVehicleType ON;
    INSERT INTO dim.DimVehicleType (VehicleTypeKey, TypeCode, TypeName, Category, IsHeavy)
    VALUES (-1, 'UNK', N'Unknown', 'Passenger', 0);
    SET IDENTITY_INSERT dim.DimVehicleType OFF;
END

IF NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition WHERE WeatherKey = -1)
BEGIN
    SET IDENTITY_INSERT dim.DimWeatherCondition ON;
    INSERT INTO dim.DimWeatherCondition (WeatherKey, ConditionCode, ConditionName, TempBand, PrecipBand, VisibilityBand, IsSevere)
    VALUES (-1, 'UNK', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0);
    SET IDENTITY_INSERT dim.DimWeatherCondition OFF;
END

IF NOT EXISTS (SELECT 1 FROM dim.DimTrafficCamera WHERE CameraKey = -1)
BEGIN
    SET IDENTITY_INSERT dim.DimTrafficCamera ON;
    INSERT INTO dim.DimTrafficCamera (CameraKey, CameraCode, IntersectionName, Model, Resolution, FirmwareVersion, Status)
    VALUES (-1, 'UNKNOWN', N'Unknown', NULL, NULL, NULL, 'Unknown');
    SET IDENTITY_INSERT dim.DimTrafficCamera OFF;
END

IF NOT EXISTS (SELECT 1 FROM dim.DimTrafficLight WHERE TrafficLightKey = -1)
BEGIN
    SET IDENTITY_INSERT dim.DimTrafficLight ON;
    INSERT INTO dim.DimTrafficLight (TrafficLightKey, ControllerCode, IntersectionName,
        CurrentTimingPlan, PreviousTimingPlan, TimingPlanChangeDate, CycleSeconds, Status)
    VALUES (-1, 'UNKNOWN', N'Unknown', 'UNKNOWN', NULL, NULL, 90, 'Unknown');
    SET IDENTITY_INSERT dim.DimTrafficLight OFF;
END

IF NOT EXISTS (SELECT 1 FROM dim.DimIncidentType WHERE IncidentTypeKey = -1)
BEGIN
    SET IDENTITY_INSERT dim.DimIncidentType ON;
    INSERT INTO dim.DimIncidentType (IncidentTypeKey, TypeCode, TypeName, Category, DefaultSeverity)
    VALUES (-1, 'UNK', N'Unknown', 'Hazard', 1);
    SET IDENTITY_INSERT dim.DimIncidentType OFF;
END

IF NOT EXISTS (SELECT 1 FROM dim.DimEmergencyUnit WHERE EmergencyUnitKey = -1)
BEGIN
    SET IDENTITY_INSERT dim.DimEmergencyUnit ON;
    INSERT INTO dim.DimEmergencyUnit (EmergencyUnitKey, UnitCode, UnitType, HomeStation)
    VALUES (-1, 'UNKNOWN', 'Police', N'Unknown');
    SET IDENTITY_INSERT dim.DimEmergencyUnit OFF;
END
GO

PRINT 'TrafficDW dimensions created and unknown members seeded.';
GO

/* ============================================================================
   TrafficDW — fact tables (the three Kimball fact types)
   The monthly partition function/scheme for FactTrafficEvent are created
   here (idempotently), because the table must be created ON the scheme;
   05_indexes_partitioning.sql then adds the partition-aligned indexes.
   ========================================================================== */
USE TrafficDW;
GO

/* ---------------- partitioning objects (monthly, RANGE RIGHT on DateKey) --- */
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pfMonthlyDateKey')
BEGIN
    CREATE PARTITION FUNCTION pfMonthlyDateKey (INT)
    AS RANGE RIGHT FOR VALUES (
        20260101, 20260201, 20260301, 20260401, 20260501, 20260601,
        20260701, 20260801, 20260901, 20261001, 20261101, 20261201,
        20270101);
END
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'psMonthlyDateKey')
BEGIN
    CREATE PARTITION SCHEME psMonthlyDateKey
    AS PARTITION pfMonthlyDateKey ALL TO ([PRIMARY]);
END
GO

/* ============================================================================
   1. TRANSACTION FACT — fact.FactTrafficEvent
   Grain: one row per individual vehicle detection by one sensor.
   Insert-only. Partitioned monthly by DateKey. Clustered columnstore (05).
   ========================================================================== */
CREATE TABLE fact.FactTrafficEvent (
    DateKey           INT          NOT NULL CONSTRAINT FK_FTE_Date        REFERENCES dim.DimDate(DateKey),
    TimeKey           SMALLINT     NOT NULL CONSTRAINT FK_FTE_Time        REFERENCES dim.DimTime(TimeKey),
    RoadSegmentKey    INT          NOT NULL CONSTRAINT FK_FTE_Segment     REFERENCES dim.DimRoadSegment(RoadSegmentKey),
    SensorKey         INT          NOT NULL CONSTRAINT FK_FTE_Sensor      REFERENCES dim.DimSensor(SensorKey),
    VehicleTypeKey    INT          NOT NULL CONSTRAINT FK_FTE_VehicleType REFERENCES dim.DimVehicleType(VehicleTypeKey),
    WeatherKey        INT          NOT NULL CONSTRAINT FK_FTE_Weather     REFERENCES dim.DimWeatherCondition(WeatherKey),
    /* measures */
    SpeedKmh          DECIMAL(5,1) NULL,
    HeadwaySeconds    DECIMAL(6,2) NULL,
    OccupancyPct      DECIMAL(5,2) NULL,
    SpeedOverLimitKmh DECIMAL(5,1) NULL,     -- derived: SpeedKmh - segment version's limit
    /* degenerate dimension + lineage */
    EventID           VARCHAR(40)  NOT NULL,
    ETLBatchID        INT          NOT NULL
) ON psMonthlyDateKey (DateKey);
GO

/* ============================================================================
   2. PERIODIC SNAPSHOT FACT — fact.FactHourlyTraffic
   Grain: one row per road segment per clock hour (rows exist for zero traffic).
   Replaced per load date (delete + insert). Rowstore clustered + NCCI (05).
   ========================================================================== */
CREATE TABLE fact.FactHourlyTraffic (
    DateKey            INT          NOT NULL CONSTRAINT FK_FHT_Date    REFERENCES dim.DimDate(DateKey),
    HourOfDay          TINYINT      NOT NULL,          -- 0..23 (hour spine, not DimTime minute grain)
    RoadSegmentKey     INT          NOT NULL CONSTRAINT FK_FHT_Segment REFERENCES dim.DimRoadSegment(RoadSegmentKey),
    WeatherKey         INT          NOT NULL CONSTRAINT FK_FHT_Weather REFERENCES dim.DimWeatherCondition(WeatherKey),
    /* additive measures */
    VehicleCount       INT          NOT NULL CONSTRAINT DF_FHT_VC  DEFAULT 0,
    HeavyVehicleCount  INT          NOT NULL CONSTRAINT DF_FHT_HVC DEFAULT 0,
    IncidentCount      SMALLINT     NOT NULL CONSTRAINT DF_FHT_IC  DEFAULT 0,
    /* semi-additive / averaged measures (volume-weighted at load) */
    AvgSpeedKmh        DECIMAL(5,1) NULL,
    P85SpeedKmh        DECIMAL(5,1) NULL,
    AvgOccupancyPct    DECIMAL(5,2) NULL,
    CongestionIndex    DECIMAL(4,3) NULL,              -- 1 - AvgSpeed/SpeedLimit, clamped [0,1]
    AvgTempC           DECIMAL(4,1) NULL,
    PrecipitationMm    DECIMAL(5,1) NULL,
    ETLBatchID         INT          NOT NULL,
    CONSTRAINT PK_FactHourlyTraffic PRIMARY KEY CLUSTERED (DateKey, HourOfDay, RoadSegmentKey),
    CONSTRAINT CK_FHT_Hour CHECK (HourOfDay BETWEEN 0 AND 23),
    CONSTRAINT CK_FHT_CI   CHECK (CongestionIndex IS NULL OR CongestionIndex BETWEEN 0 AND 1)
);
GO

/* ============================================================================
   3. ACCUMULATING SNAPSHOT FACT — fact.FactIncidentLifecycle
   Grain: one row per incident; milestone keys/lags UPDATEd as the incident
   progresses (Detected → Dispatched → Arrived → Cleared → Closed).
   Role-playing DimDate/DimTime: five date + five time FKs.
   Unreached milestones point at the Unknown member (-1), NOT NULL —
   this keeps joins simple and makes 'not yet' explicit.
   ========================================================================== */
CREATE TABLE fact.FactIncidentLifecycle (
    IncidentKey          INT IDENTITY(1,1) CONSTRAINT PK_FactIncidentLifecycle PRIMARY KEY CLUSTERED,
    IncidentNumber       VARCHAR(20) NOT NULL,          -- degenerate dimension (source BK)
    IncidentTypeKey      INT         NOT NULL CONSTRAINT FK_FIL_Type    REFERENCES dim.DimIncidentType(IncidentTypeKey),
    RoadSegmentKey       INT         NOT NULL CONSTRAINT FK_FIL_Segment REFERENCES dim.DimRoadSegment(RoadSegmentKey),
    EmergencyUnitKey     INT         NOT NULL CONSTRAINT FK_FIL_Unit    REFERENCES dim.DimEmergencyUnit(EmergencyUnitKey),
    WeatherKey           INT         NOT NULL CONSTRAINT FK_FIL_Weather REFERENCES dim.DimWeatherCondition(WeatherKey),
    /* milestone role-playing keys */
    DetectedDateKey      INT      NOT NULL CONSTRAINT FK_FIL_DetD  REFERENCES dim.DimDate(DateKey),
    DetectedTimeKey      SMALLINT NOT NULL CONSTRAINT FK_FIL_DetT  REFERENCES dim.DimTime(TimeKey),
    DispatchedDateKey    INT      NOT NULL CONSTRAINT FK_FIL_DisD  REFERENCES dim.DimDate(DateKey),
    DispatchedTimeKey    SMALLINT NOT NULL CONSTRAINT FK_FIL_DisT  REFERENCES dim.DimTime(TimeKey),
    ArrivedDateKey       INT      NOT NULL CONSTRAINT FK_FIL_ArrD  REFERENCES dim.DimDate(DateKey),
    ArrivedTimeKey       SMALLINT NOT NULL CONSTRAINT FK_FIL_ArrT  REFERENCES dim.DimTime(TimeKey),
    ClearedDateKey       INT      NOT NULL CONSTRAINT FK_FIL_ClrD  REFERENCES dim.DimDate(DateKey),
    ClearedTimeKey       SMALLINT NOT NULL CONSTRAINT FK_FIL_ClrT  REFERENCES dim.DimTime(TimeKey),
    ClosedDateKey        INT      NOT NULL CONSTRAINT FK_FIL_CloD  REFERENCES dim.DimDate(DateKey),
    ClosedTimeKey        SMALLINT NOT NULL CONSTRAINT FK_FIL_CloT  REFERENCES dim.DimTime(TimeKey),
    /* state + measures */
    CurrentStatus        VARCHAR(20)  NOT NULL,
    Severity             TINYINT      NOT NULL,
    LanesBlocked         TINYINT      NOT NULL CONSTRAINT DF_FIL_Lanes DEFAULT 0,
    MinutesToDispatch    INT NULL,   -- Detected  → Dispatched
    MinutesToArrive      INT NULL,   -- Dispatched → Arrived   (response time)
    MinutesToClear       INT NULL,   -- Arrived   → Cleared    (clearance time)
    TotalDurationMinutes INT NULL,   -- Detected  → Closed
    ETLBatchID           INT NOT NULL,
    CONSTRAINT UQ_FIL_IncidentNumber UNIQUE (IncidentNumber),
    CONSTRAINT CK_FIL_Lags CHECK (
        (MinutesToDispatch    IS NULL OR MinutesToDispatch    >= 0) AND
        (MinutesToArrive      IS NULL OR MinutesToArrive      >= 0) AND
        (MinutesToClear       IS NULL OR MinutesToClear       >= 0) AND
        (TotalDurationMinutes IS NULL OR TotalDurationMinutes >= 0))
);
GO

/* ------------------------- role-playing views over DimDate for BI usability */
CREATE OR ALTER VIEW dim.DimDate_Detected  AS SELECT * FROM dim.DimDate;
GO
CREATE OR ALTER VIEW dim.DimDate_Dispatched AS SELECT * FROM dim.DimDate;
GO
CREATE OR ALTER VIEW dim.DimDate_Arrived   AS SELECT * FROM dim.DimDate;
GO
CREATE OR ALTER VIEW dim.DimDate_Cleared   AS SELECT * FROM dim.DimDate;
GO
CREATE OR ALTER VIEW dim.DimDate_Closed    AS SELECT * FROM dim.DimDate;
GO

PRINT 'TrafficDW fact tables created.';
GO

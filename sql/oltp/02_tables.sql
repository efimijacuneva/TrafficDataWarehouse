/* ============================================================================
   TrafficOLTP — normalized (3NF) schema
   Conventions:
     - Surrogate PK: <Entity>ID INT IDENTITY
     - Business keys: UNIQUE constraints (become DW SCD matching keys)
     - Mutable tables carry ModifiedAt (high-watermark incremental extraction)
   ========================================================================== */
USE TrafficOLTP;
GO

/* ---------------------------------------------------------------- geography */
CREATE TABLE oltp.City (
    CityID        INT IDENTITY(1,1) CONSTRAINT PK_City PRIMARY KEY,
    CityName      NVARCHAR(100) NOT NULL CONSTRAINT UQ_City_Name UNIQUE,
    Country       NVARCHAR(100) NOT NULL,
    Population    INT           NULL,
    ModifiedAt    DATETIME2(3)  NOT NULL CONSTRAINT DF_City_Mod DEFAULT SYSUTCDATETIME()
);

CREATE TABLE oltp.District (
    DistrictID    INT IDENTITY(1,1) CONSTRAINT PK_District PRIMARY KEY,
    CityID        INT           NOT NULL CONSTRAINT FK_District_City REFERENCES oltp.City(CityID),
    DistrictName  NVARCHAR(100) NOT NULL,
    ModifiedAt    DATETIME2(3)  NOT NULL CONSTRAINT DF_District_Mod DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_District UNIQUE (CityID, DistrictName)
);

CREATE TABLE oltp.Location (
    LocationID    INT IDENTITY(1,1) CONSTRAINT PK_Location PRIMARY KEY,
    DistrictID    INT            NOT NULL CONSTRAINT FK_Location_District REFERENCES oltp.District(DistrictID),
    Latitude      DECIMAL(9,6)   NOT NULL,
    Longitude     DECIMAL(9,6)   NOT NULL,
    Address       NVARCHAR(200)  NULL,
    ModifiedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_Location_Mod DEFAULT SYSUTCDATETIME(),
    CONSTRAINT CK_Location_Lat CHECK (Latitude  BETWEEN -90  AND 90),
    CONSTRAINT CK_Location_Lon CHECK (Longitude BETWEEN -180 AND 180)
);

/* ------------------------------------------------------------- road network */
CREATE TABLE oltp.Road (
    RoadID        INT IDENTITY(1,1) CONSTRAINT PK_Road PRIMARY KEY,
    RoadCode      VARCHAR(20)    NOT NULL CONSTRAINT UQ_Road_Code UNIQUE,   -- business key
    RoadName      NVARCHAR(150)  NOT NULL,
    RoadCategory  VARCHAR(20)    NOT NULL
        CONSTRAINT CK_Road_Category CHECK (RoadCategory IN ('Highway','Arterial','Collector','Local')),
    CityID        INT            NOT NULL CONSTRAINT FK_Road_City REFERENCES oltp.City(CityID),
    LengthKm      DECIMAL(8,3)   NULL,
    ModifiedAt    DATETIME2(3)   NOT NULL CONSTRAINT DF_Road_Mod DEFAULT SYSUTCDATETIME()
);

CREATE TABLE oltp.Intersection (
    IntersectionID   INT IDENTITY(1,1) CONSTRAINT PK_Intersection PRIMARY KEY,
    IntersectionCode VARCHAR(20)   NOT NULL CONSTRAINT UQ_Intersection_Code UNIQUE,
    IntersectionName NVARCHAR(150) NOT NULL,
    IntersectionType VARCHAR(20)   NOT NULL
        CONSTRAINT CK_Intersection_Type CHECK (IntersectionType IN ('Signalized','Roundabout','Priority','Interchange')),
    LocationID       INT           NOT NULL CONSTRAINT FK_Intersection_Location REFERENCES oltp.Location(LocationID),
    ModifiedAt       DATETIME2(3)  NOT NULL CONSTRAINT DF_Intersection_Mod DEFAULT SYSUTCDATETIME()
);

CREATE TABLE oltp.RoadSegment (
    RoadSegmentID       INT IDENTITY(1,1) CONSTRAINT PK_RoadSegment PRIMARY KEY,
    SegmentCode         VARCHAR(30)  NOT NULL CONSTRAINT UQ_RoadSegment_Code UNIQUE,  -- business key
    RoadID              INT          NOT NULL CONSTRAINT FK_Segment_Road REFERENCES oltp.Road(RoadID),
    StartIntersectionID INT          NOT NULL CONSTRAINT FK_Segment_StartInt REFERENCES oltp.Intersection(IntersectionID),
    EndIntersectionID   INT          NOT NULL CONSTRAINT FK_Segment_EndInt   REFERENCES oltp.Intersection(IntersectionID),
    Direction           CHAR(2)      NOT NULL
        CONSTRAINT CK_Segment_Dir CHECK (Direction IN ('NB','SB','EB','WB')),
    LengthM             INT          NOT NULL CONSTRAINT CK_Segment_Len CHECK (LengthM > 0),
    LaneCount           TINYINT      NOT NULL CONSTRAINT CK_Segment_Lanes CHECK (LaneCount BETWEEN 1 AND 8),
    SpeedLimitKmh       SMALLINT     NOT NULL CONSTRAINT CK_Segment_Limit CHECK (SpeedLimitKmh BETWEEN 20 AND 140),
    ModifiedAt          DATETIME2(3) NOT NULL CONSTRAINT DF_Segment_Mod DEFAULT SYSUTCDATETIME(),
    CONSTRAINT CK_Segment_Ends CHECK (StartIntersectionID <> EndIntersectionID)
);

/* ------------------------------------------------------------------- assets */
CREATE TABLE oltp.SensorType (
    SensorTypeID  INT IDENTITY(1,1) CONSTRAINT PK_SensorType PRIMARY KEY,
    TypeName      NVARCHAR(50) NOT NULL CONSTRAINT UQ_SensorType UNIQUE,
    Technology    NVARCHAR(50) NOT NULL   -- InductiveLoop / Radar / Lidar / Magnetometer
);

CREATE TABLE oltp.Sensor (
    SensorID       INT IDENTITY(1,1) CONSTRAINT PK_Sensor PRIMARY KEY,
    SerialNumber   VARCHAR(30)  NOT NULL CONSTRAINT UQ_Sensor_Serial UNIQUE,  -- business key
    SensorTypeID   INT          NOT NULL CONSTRAINT FK_Sensor_Type REFERENCES oltp.SensorType(SensorTypeID),
    RoadSegmentID  INT          NOT NULL CONSTRAINT FK_Sensor_Segment REFERENCES oltp.RoadSegment(RoadSegmentID),
    LocationID     INT          NOT NULL CONSTRAINT FK_Sensor_Location REFERENCES oltp.Location(LocationID),
    InstallDate    DATE         NOT NULL,
    Status         VARCHAR(20)  NOT NULL CONSTRAINT DF_Sensor_Status DEFAULT 'Active'
        CONSTRAINT CK_Sensor_Status CHECK (Status IN ('Active','Degraded','Offline','Retired')),
    FirmwareVersion VARCHAR(20) NULL,
    ModifiedAt     DATETIME2(3) NOT NULL CONSTRAINT DF_Sensor_Mod DEFAULT SYSUTCDATETIME()
);

CREATE TABLE oltp.TrafficCamera (
    CameraID       INT IDENTITY(1,1) CONSTRAINT PK_Camera PRIMARY KEY,
    CameraCode     VARCHAR(20)   NOT NULL CONSTRAINT UQ_Camera_Code UNIQUE,   -- business key
    IntersectionID INT           NOT NULL CONSTRAINT FK_Camera_Intersection REFERENCES oltp.Intersection(IntersectionID),
    LocationID     INT           NOT NULL CONSTRAINT FK_Camera_Location REFERENCES oltp.Location(LocationID),
    Model          NVARCHAR(80)  NULL,
    Resolution     VARCHAR(20)   NULL,          -- 1080p / 4K
    FirmwareVersion VARCHAR(20)  NULL,
    InstallDate    DATE          NOT NULL,
    Status         VARCHAR(20)   NOT NULL CONSTRAINT DF_Camera_Status DEFAULT 'Active',
    ModifiedAt     DATETIME2(3)  NOT NULL CONSTRAINT DF_Camera_Mod DEFAULT SYSUTCDATETIME()
);

CREATE TABLE oltp.TrafficLight (
    TrafficLightID INT IDENTITY(1,1) CONSTRAINT PK_TrafficLight PRIMARY KEY,
    ControllerCode VARCHAR(20)  NOT NULL CONSTRAINT UQ_TrafficLight_Code UNIQUE, -- business key
    IntersectionID INT          NOT NULL CONSTRAINT FK_Light_Intersection REFERENCES oltp.Intersection(IntersectionID),
    TimingPlan     VARCHAR(30)  NOT NULL,       -- e.g. 'FIXED-90', 'ADAPTIVE-V2', 'PEAK-AM-120'
    CycleSeconds   SMALLINT     NOT NULL CONSTRAINT CK_Light_Cycle CHECK (CycleSeconds BETWEEN 30 AND 240),
    InstallDate    DATE         NOT NULL,
    Status         VARCHAR(20)  NOT NULL CONSTRAINT DF_Light_Status DEFAULT 'Active',
    ModifiedAt     DATETIME2(3) NOT NULL CONSTRAINT DF_Light_Mod DEFAULT SYSUTCDATETIME()
);

/* ----------------------------------------------------------------- vehicles */
CREATE TABLE oltp.VehicleType (
    VehicleTypeID INT IDENTITY(1,1) CONSTRAINT PK_VehicleType PRIMARY KEY,
    TypeCode      VARCHAR(10)  NOT NULL CONSTRAINT UQ_VehicleType_Code UNIQUE,  -- business key
    TypeName      NVARCHAR(50) NOT NULL,
    Category      VARCHAR(20)  NOT NULL
        CONSTRAINT CK_VehicleType_Cat CHECK (Category IN ('Passenger','Commercial','PublicTransport','TwoWheeler','Emergency')),
    IsHeavy       BIT          NOT NULL CONSTRAINT DF_VehicleType_Heavy DEFAULT 0,
    ModifiedAt    DATETIME2(3) NOT NULL CONSTRAINT DF_VehicleType_Mod DEFAULT SYSUTCDATETIME()
);

/* Registered fleet only (municipal + emergency). Private vehicles are NOT stored:
   roadside detection is anonymous (privacy by design). Plate kept as salted hash. */
CREATE TABLE oltp.Vehicle (
    VehicleID       INT IDENTITY(1,1) CONSTRAINT PK_Vehicle PRIMARY KEY,
    PlateHash       CHAR(64)     NOT NULL CONSTRAINT UQ_Vehicle_Plate UNIQUE,
    VehicleTypeID   INT          NOT NULL CONSTRAINT FK_Vehicle_Type REFERENCES oltp.VehicleType(VehicleTypeID),
    Make            NVARCHAR(50) NULL,
    Model           NVARCHAR(50) NULL,
    RegistrationYear SMALLINT    NULL,
    ModifiedAt      DATETIME2(3) NOT NULL CONSTRAINT DF_Vehicle_Mod DEFAULT SYSUTCDATETIME()
);

CREATE TABLE oltp.EmergencyVehicle (      -- 1:0..1 subtype of Vehicle
    EmergencyVehicleID INT IDENTITY(1,1) CONSTRAINT PK_EmergencyVehicle PRIMARY KEY,
    VehicleID     INT          NOT NULL CONSTRAINT UQ_EmergencyVehicle_Vehicle UNIQUE
                               CONSTRAINT FK_EmVeh_Vehicle REFERENCES oltp.Vehicle(VehicleID),
    UnitCode      VARCHAR(20)  NOT NULL CONSTRAINT UQ_EmergencyVehicle_Unit UNIQUE,  -- business key
    UnitType      VARCHAR(20)  NOT NULL
        CONSTRAINT CK_EmVeh_Type CHECK (UnitType IN ('Police','Fire','Ambulance','Tow')),
    HomeStation   NVARCHAR(100) NOT NULL,
    ModifiedAt    DATETIME2(3) NOT NULL CONSTRAINT DF_EmVeh_Mod DEFAULT SYSUTCDATETIME()
);

/* ------------------------------------------------------------------ weather */
CREATE TABLE oltp.WeatherStation (
    WeatherStationID INT IDENTITY(1,1) CONSTRAINT PK_WeatherStation PRIMARY KEY,
    StationCode   VARCHAR(20)   NOT NULL CONSTRAINT UQ_WeatherStation UNIQUE,
    StationName   NVARCHAR(100) NOT NULL,
    LocationID    INT           NOT NULL CONSTRAINT FK_WStation_Location REFERENCES oltp.Location(LocationID)
);

CREATE TABLE oltp.WeatherObservation (
    WeatherObservationID BIGINT IDENTITY(1,1) CONSTRAINT PK_WeatherObs PRIMARY KEY,
    WeatherStationID INT          NOT NULL CONSTRAINT FK_WObs_Station REFERENCES oltp.WeatherStation(WeatherStationID),
    ObservedAt      DATETIME2(0)  NOT NULL,
    TemperatureC    DECIMAL(4,1)  NULL,
    PrecipitationMm DECIMAL(5,1)  NULL,
    WindSpeedKmh    DECIMAL(5,1)  NULL,
    VisibilityM     INT           NULL,
    ConditionCode   VARCHAR(10)   NOT NULL,   -- DRY / RAIN / SNOW / FOG / ICE / STORM
    CONSTRAINT UQ_WObs UNIQUE (WeatherStationID, ObservedAt)
);

/* --------------------------------------------- measurements (high volume) */
/* NOTE: in production this stream bypasses OLTP and lands as CSV → Spark.
   The table exists to document lineage and hold a small operational window. */
CREATE TABLE oltp.TrafficMeasurement (
    MeasurementID  BIGINT IDENTITY(1,1) CONSTRAINT PK_TrafficMeasurement PRIMARY KEY,
    SensorID       INT          NOT NULL CONSTRAINT FK_Meas_Sensor REFERENCES oltp.Sensor(SensorID),
    MeasuredAt     DATETIME2(3) NOT NULL,
    VehicleTypeID  INT          NULL CONSTRAINT FK_Meas_VehicleType REFERENCES oltp.VehicleType(VehicleTypeID),
    SpeedKmh       DECIMAL(5,1) NULL,
    OccupancyPct   DECIMAL(5,2) NULL,
    HeadwaySeconds DECIMAL(6,2) NULL,
    CONSTRAINT CK_Meas_Speed CHECK (SpeedKmh IS NULL OR SpeedKmh BETWEEN 0 AND 250)
);
CREATE INDEX IX_Meas_Sensor_Time ON oltp.TrafficMeasurement (SensorID, MeasuredAt);

/* ---------------------------------------------------------------- incidents */
CREATE TABLE oltp.IncidentType (
    IncidentTypeID  INT IDENTITY(1,1) CONSTRAINT PK_IncidentType PRIMARY KEY,
    TypeCode        VARCHAR(10)  NOT NULL CONSTRAINT UQ_IncidentType UNIQUE,  -- business key
    TypeName        NVARCHAR(80) NOT NULL,
    Category        VARCHAR(20)  NOT NULL
        CONSTRAINT CK_IncType_Cat CHECK (Category IN ('Accident','Breakdown','Roadworks','Hazard','Event')),
    DefaultSeverity TINYINT      NOT NULL CONSTRAINT CK_IncType_Sev CHECK (DefaultSeverity BETWEEN 1 AND 5),
    ModifiedAt      DATETIME2(3) NOT NULL CONSTRAINT DF_IncType_Mod DEFAULT SYSUTCDATETIME()
);

CREATE TABLE oltp.Incident (
    IncidentID     INT IDENTITY(1,1) CONSTRAINT PK_Incident PRIMARY KEY,
    IncidentNumber VARCHAR(20)  NOT NULL CONSTRAINT UQ_Incident_Number UNIQUE,  -- business key 'INC-2026-000123'
    IncidentTypeID INT          NOT NULL CONSTRAINT FK_Incident_Type REFERENCES oltp.IncidentType(IncidentTypeID),
    RoadSegmentID  INT          NOT NULL CONSTRAINT FK_Incident_Segment REFERENCES oltp.RoadSegment(RoadSegmentID),
    DetectedAt     DATETIME2(0) NOT NULL,
    Severity       TINYINT      NOT NULL CONSTRAINT CK_Incident_Sev CHECK (Severity BETWEEN 1 AND 5),
    LanesBlocked   TINYINT      NOT NULL CONSTRAINT DF_Incident_Lanes DEFAULT 0,
    Description    NVARCHAR(500) NULL,
    Status         VARCHAR(20)  NOT NULL CONSTRAINT DF_Incident_Status DEFAULT 'Detected'
        CONSTRAINT CK_Incident_Status CHECK (Status IN ('Detected','Responded','Cleared','Closed')),
    RoadClearedAt  DATETIME2(0) NULL,
    ClosedAt       DATETIME2(0) NULL,
    ModifiedAt     DATETIME2(3) NOT NULL CONSTRAINT DF_Incident_Mod DEFAULT SYSUTCDATETIME()
);

CREATE TABLE oltp.PoliceResponse (
    PoliceResponseID   INT IDENTITY(1,1) CONSTRAINT PK_PoliceResponse PRIMARY KEY,
    IncidentID         INT          NOT NULL CONSTRAINT FK_Response_Incident REFERENCES oltp.Incident(IncidentID),
    EmergencyVehicleID INT          NOT NULL CONSTRAINT FK_Response_EmVeh REFERENCES oltp.EmergencyVehicle(EmergencyVehicleID),
    DispatchedAt       DATETIME2(0) NOT NULL,
    ArrivedAt          DATETIME2(0) NULL,
    ClearedAt          DATETIME2(0) NULL,
    ModifiedAt         DATETIME2(3) NOT NULL CONSTRAINT DF_Response_Mod DEFAULT SYSUTCDATETIME(),
    CONSTRAINT CK_Response_Order CHECK (ArrivedAt IS NULL OR ArrivedAt >= DispatchedAt)
);

/* -------------------------------------------------------------- maintenance */
CREATE TABLE oltp.Maintenance (
    MaintenanceID   INT IDENTITY(1,1) CONSTRAINT PK_Maintenance PRIMARY KEY,
    MaintenanceType VARCHAR(30)  NOT NULL,   -- Calibration / Repair / Resurfacing / Replacement
    SensorID        INT NULL CONSTRAINT FK_Maint_Sensor  REFERENCES oltp.Sensor(SensorID),
    TrafficLightID  INT NULL CONSTRAINT FK_Maint_Light   REFERENCES oltp.TrafficLight(TrafficLightID),
    CameraID        INT NULL CONSTRAINT FK_Maint_Camera  REFERENCES oltp.TrafficCamera(CameraID),
    RoadSegmentID   INT NULL CONSTRAINT FK_Maint_Segment REFERENCES oltp.RoadSegment(RoadSegmentID),
    StartDate       DATE         NOT NULL,
    EndDate         DATE         NULL,
    CostEur         DECIMAL(10,2) NULL,
    ModifiedAt      DATETIME2(3) NOT NULL CONSTRAINT DF_Maint_Mod DEFAULT SYSUTCDATETIME(),
    /* polymorphic target: exactly one asset FK must be set */
    CONSTRAINT CK_Maint_OneTarget CHECK (
        (CASE WHEN SensorID       IS NULL THEN 0 ELSE 1 END) +
        (CASE WHEN TrafficLightID IS NULL THEN 0 ELSE 1 END) +
        (CASE WHEN CameraID       IS NULL THEN 0 ELSE 1 END) +
        (CASE WHEN RoadSegmentID  IS NULL THEN 0 ELSE 1 END) = 1)
);
GO

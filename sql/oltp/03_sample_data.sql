/* ============================================================================
   TrafficOLTP — realistic sample data (set-based, deterministic-ish)
   Volumes are OLTP-scale demo volumes; the BIG volumes (detections) are
   produced as CSV/JSON by data_generator/generate_data.py and flow via Spark.
   ========================================================================== */
USE TrafficOLTP;
SET NOCOUNT ON;
GO

/* ------------------------------------------------------------------ geography */
INSERT INTO oltp.City (CityName, Country, Population) VALUES
 (N'Skopje',  N'North Macedonia', 550000);

INSERT INTO oltp.District (CityID, DistrictName)
SELECT c.CityID, d.Name
FROM oltp.City c
CROSS APPLY (VALUES (N'Centar'),(N'Karpos'),(N'Aerodrom'),(N'Gazi Baba'),
                    (N'Kisela Voda'),(N'Butel'),(N'Gjorche Petrov'),(N'Chair')) d(Name)
WHERE c.CityName = N'Skopje';

/* 400 geocoded locations spread over the districts */
;WITH n AS (SELECT TOP (400) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
            FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO oltp.Location (DistrictID, Latitude, Longitude, Address)
SELECT 1 + (n.i % 8),
       CAST(41.9600 + (n.i % 97)  * 0.0011 AS DECIMAL(9,6)),
       CAST(21.3900 + (n.i % 113) * 0.0013 AS DECIMAL(9,6)),
       CONCAT(N'Blvd. ', n.i, N', Skopje')
FROM n;

/* --------------------------------------------------------------- road network */
INSERT INTO oltp.Road (RoadCode, RoadName, RoadCategory, CityID, LengthKm) VALUES
 ('RD-001', N'Boulevard Alexander the Great', 'Arterial',  1, 6.800),
 ('RD-002', N'Boulevard Partizanski Odredi',  'Arterial',  1, 7.200),
 ('RD-003', N'E-75 City Bypass',              'Highway',   1, 18.500),
 ('RD-004', N'Boulevard Ilinden',             'Arterial',  1, 5.100),
 ('RD-005', N'Street Makedonija',             'Collector', 1, 2.300),
 ('RD-006', N'Boulevard Jane Sandanski',      'Arterial',  1, 4.600),
 ('RD-007', N'Street Dimitrie Chupovski',     'Local',     1, 1.200),
 ('RD-008', N'Boulevard Kuzman Josifovski',   'Collector', 1, 3.400),
 ('RD-009', N'Boulevard Nikola Karev',        'Arterial',  1, 4.100),
 ('RD-010', N'A2 Airport Corridor',           'Highway',   1, 12.900);

/* 60 intersections */
;WITH n AS (SELECT TOP (60) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
INSERT INTO oltp.Intersection (IntersectionCode, IntersectionName, IntersectionType, LocationID)
SELECT CONCAT('INT-', RIGHT('000' + CAST(n.i AS VARCHAR(3)), 3)),
       CONCAT(N'Intersection ', n.i),
       CASE n.i % 4 WHEN 0 THEN 'Roundabout' WHEN 1 THEN 'Signalized'
                    WHEN 2 THEN 'Signalized' ELSE 'Priority' END,
       n.i          -- locations 1..60
FROM n;

/* 120 directional segments: chains along each road (12 per road, 6 per direction) */
;WITH n AS (SELECT TOP (120) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
INSERT INTO oltp.RoadSegment
      (SegmentCode, RoadID, StartIntersectionID, EndIntersectionID, Direction, LengthM, LaneCount, SpeedLimitKmh)
SELECT CONCAT('SEG-', RIGHT('000' + CAST(n.i AS VARCHAR(3)), 3)),
       1 + ((n.i - 1) / 12),                                  -- road 1..10
       1 + ((n.i - 1) % 59),                                  -- start intersection
       1 + (n.i % 59) + CASE WHEN ((n.i - 1) % 59) = (n.i % 59) THEN 1 ELSE 0 END,
       CASE (n.i - 1) % 4 WHEN 0 THEN 'NB' WHEN 1 THEN 'SB' WHEN 2 THEN 'EB' ELSE 'WB' END,
       400 + (n.i % 14) * 90,
       CASE WHEN ((n.i - 1) / 12) + 1 IN (3, 10) THEN 3 ELSE 1 + (n.i % 2) END,  -- highways 3 lanes
       CASE WHEN ((n.i - 1) / 12) + 1 IN (3, 10) THEN 100                        -- highways
            WHEN ((n.i - 1) / 12) + 1 = 7        THEN 30                         -- local street
            ELSE 50 + 10 * (n.i % 2) END
FROM n;

/* --------------------------------------------------------------------- assets */
INSERT INTO oltp.SensorType (TypeName, Technology) VALUES
 (N'Loop Detector',   N'InductiveLoop'),
 (N'Radar Detector',  N'Radar'),
 (N'Lidar Counter',   N'Lidar'),
 (N'Magnetometer',    N'Magnetometer');

/* 200 sensors: ~1-2 per segment */
;WITH n AS (SELECT TOP (200) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
INSERT INTO oltp.Sensor (SerialNumber, SensorTypeID, RoadSegmentID, LocationID, InstallDate, Status, FirmwareVersion)
SELECT CONCAT('SNS-', RIGHT('0000' + CAST(n.i AS VARCHAR(4)), 4)),
       1 + (n.i % 4),
       1 + ((n.i - 1) % 120),
       61 + ((n.i - 1) % 339),
       DATEADD(DAY, -(n.i * 7 % 1400), '2026-01-01'),
       CASE WHEN n.i % 29 = 0 THEN 'Degraded' WHEN n.i % 53 = 0 THEN 'Offline' ELSE 'Active' END,
       CONCAT('v2.', n.i % 6)
FROM n;

/* 50 cameras at intersections */
;WITH n AS (SELECT TOP (50) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
INSERT INTO oltp.TrafficCamera (CameraCode, IntersectionID, LocationID, Model, Resolution, FirmwareVersion, InstallDate)
SELECT CONCAT('CAM-', RIGHT('000' + CAST(n.i AS VARCHAR(3)), 3)),
       1 + ((n.i - 1) % 60), n.i,
       CASE n.i % 3 WHEN 0 THEN N'Axis Q1798' WHEN 1 THEN N'Bosch MIC 7100' ELSE N'Hikvision DS-2CD' END,
       CASE WHEN n.i % 3 = 0 THEN '4K' ELSE '1080p' END,
       CONCAT('fw-', 3 + n.i % 3, '.', n.i % 10),
       DATEADD(DAY, -(n.i * 11 % 1100), '2026-01-01')
FROM n;

/* 45 traffic lights at the signalized intersections */
;WITH sig AS (SELECT IntersectionID, ROW_NUMBER() OVER (ORDER BY IntersectionID) AS rn
              FROM oltp.Intersection WHERE IntersectionType = 'Signalized')
INSERT INTO oltp.TrafficLight (ControllerCode, IntersectionID, TimingPlan, CycleSeconds, InstallDate)
SELECT CONCAT('TL-', RIGHT('000' + CAST(rn AS VARCHAR(3)), 3)),
       IntersectionID,
       CASE rn % 3 WHEN 0 THEN 'ADAPTIVE-V2' WHEN 1 THEN 'FIXED-90' ELSE 'PEAK-AM-120' END,
       CASE rn % 3 WHEN 0 THEN 110 WHEN 1 THEN 90 ELSE 120 END,
       DATEADD(DAY, -(rn * 13 % 1300), '2026-01-01')
FROM sig;

/* ------------------------------------------------------------------- vehicles */
INSERT INTO oltp.VehicleType (TypeCode, TypeName, Category, IsHeavy) VALUES
 ('CAR',  N'Passenger Car',       'Passenger',       0),
 ('VAN',  N'Light Van',           'Commercial',      0),
 ('TRK',  N'Truck',               'Commercial',      1),
 ('ART',  N'Articulated Truck',   'Commercial',      1),
 ('BUS',  N'City Bus',            'PublicTransport', 1),
 ('MBS',  N'Minibus',             'PublicTransport', 0),
 ('MCY',  N'Motorcycle',          'TwoWheeler',      0),
 ('BIC',  N'Bicycle',             'TwoWheeler',      0),
 ('EMG',  N'Emergency Vehicle',   'Emergency',       0),
 ('OTH',  N'Other / Unclassified','Passenger',       0);

/* 60 fleet vehicles, 40 of them emergency units */
;WITH n AS (SELECT TOP (60) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
INSERT INTO oltp.Vehicle (PlateHash, VehicleTypeID, Make, Model, RegistrationYear)
SELECT CONVERT(CHAR(64), HASHBYTES('SHA2_256', CONCAT('PLATE-', n.i)), 2),
       CASE WHEN n.i <= 40 THEN 9 ELSE 2 END,
       CASE n.i % 3 WHEN 0 THEN N'Volkswagen' WHEN 1 THEN N'Mercedes' ELSE N'Skoda' END,
       CASE n.i % 3 WHEN 0 THEN N'Transporter' WHEN 1 THEN N'Sprinter' ELSE N'Octavia' END,
       2015 + n.i % 11
FROM n;

;WITH ev AS (SELECT VehicleID, ROW_NUMBER() OVER (ORDER BY VehicleID) AS rn
             FROM oltp.Vehicle WHERE VehicleTypeID = 9)
INSERT INTO oltp.EmergencyVehicle (VehicleID, UnitCode, UnitType, HomeStation)
SELECT VehicleID,
       CONCAT('UNIT-', RIGHT('000' + CAST(rn AS VARCHAR(3)), 3)),
       CASE rn % 4 WHEN 0 THEN 'Police' WHEN 1 THEN 'Ambulance' WHEN 2 THEN 'Police' ELSE 'Tow' END,
       CASE rn % 3 WHEN 0 THEN N'Central Station' WHEN 1 THEN N'East Station' ELSE N'West Station' END
FROM ev;

/* -------------------------------------------------------------------- weather */
INSERT INTO oltp.WeatherStation (StationCode, StationName, LocationID) VALUES
 ('WS-01', N'Skopje Center',  5),
 ('WS-02', N'Skopje Airport', 150),
 ('WS-03', N'Skopje North',   300);

/* 30 days × 3 stations × hourly observations */
;WITH h AS (SELECT TOP (720) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects)
INSERT INTO oltp.WeatherObservation
      (WeatherStationID, ObservedAt, TemperatureC, PrecipitationMm, WindSpeedKmh, VisibilityM, ConditionCode)
SELECT s.WeatherStationID,
       DATEADD(HOUR, h.i, '2026-06-01'),
       CAST(14.0 + 9.0 * SIN((h.i % 24) * 3.14159 / 12.0) + (h.i % 7) AS DECIMAL(4,1)),
       CASE WHEN (h.i + s.WeatherStationID) % 19 = 0 THEN CAST((h.i % 6) + 0.5 AS DECIMAL(5,1)) ELSE 0.0 END,
       CAST(5 + (h.i * s.WeatherStationID) % 30 AS DECIMAL(5,1)),
       CASE WHEN (h.i + s.WeatherStationID) % 37 = 0 THEN 400 ELSE 9999 END,
       CASE WHEN (h.i + s.WeatherStationID) % 37 = 0 THEN 'FOG'
            WHEN (h.i + s.WeatherStationID) % 19 = 0 THEN 'RAIN'
            ELSE 'DRY' END
FROM h CROSS JOIN oltp.WeatherStation s;

/* -------------------------------------------- small operational measurement window */
;WITH n AS (SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
            FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO oltp.TrafficMeasurement (SensorID, MeasuredAt, VehicleTypeID, SpeedKmh, OccupancyPct, HeadwaySeconds)
SELECT 1 + (n.i % 200),
       DATEADD(SECOND, n.i * 17 % 86400, '2026-06-30'),
       1 + (n.i % 8),
       CAST(25 + (n.i * 7 % 70) AS DECIMAL(5,1)),
       CAST((n.i % 80) / 2.0 + 5 AS DECIMAL(5,2)),
       CAST(1 + (n.i % 40) / 4.0 AS DECIMAL(6,2))
FROM n;

/* ------------------------------------------------------------------ incidents */
INSERT INTO oltp.IncidentType (TypeCode, TypeName, Category, DefaultSeverity) VALUES
 ('COLL', N'Vehicle Collision',        'Accident',   4),
 ('PEDA', N'Pedestrian Accident',      'Accident',   5),
 ('BRKD', N'Vehicle Breakdown',        'Breakdown',  2),
 ('RWRK', N'Road Works',               'Roadworks',  2),
 ('SPIL', N'Cargo / Oil Spill',        'Hazard',     4),
 ('DEBR', N'Debris On Road',           'Hazard',     3),
 ('FLOD', N'Road Flooding',            'Hazard',     4),
 ('SIGN', N'Signal Malfunction',       'Hazard',     3),
 ('EVNT', N'Public Event Closure',     'Event',      2);

/* 180 incidents over June 2026, in every lifecycle stage */
;WITH n AS (SELECT TOP (180) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
INSERT INTO oltp.Incident
      (IncidentNumber, IncidentTypeID, RoadSegmentID, DetectedAt, Severity, LanesBlocked, Description, Status, RoadClearedAt, ClosedAt)
SELECT CONCAT('INC-2026-', RIGHT('00000' + CAST(n.i AS VARCHAR(5)), 5)),
       1 + (n.i % 9),
       1 + (n.i * 7 % 120),
       DATEADD(MINUTE, (n.i * 227) % 43200, '2026-06-01'),        -- spread over 30 days
       1 + ((n.i * 3) % 5),
       n.i % 3,
       CONCAT(N'Auto-generated incident #', n.i),
       CASE WHEN n.i % 10 = 0 THEN 'Detected'
            WHEN n.i % 10 = 1 THEN 'Responded'
            WHEN n.i % 10 = 2 THEN 'Cleared'
            ELSE 'Closed' END,
       CASE WHEN n.i % 10 IN (0,1) THEN NULL
            ELSE DATEADD(MINUTE, (n.i * 227) % 43200 + 25 + n.i % 90, '2026-06-01') END,
       CASE WHEN n.i % 10 IN (0,1,2) THEN NULL
            ELSE DATEADD(MINUTE, (n.i * 227) % 43200 + 45 + n.i % 120, '2026-06-01') END
FROM n;

/* police responses for all non-'Detected' incidents */
INSERT INTO oltp.PoliceResponse (IncidentID, EmergencyVehicleID, DispatchedAt, ArrivedAt, ClearedAt)
SELECT i.IncidentID,
       1 + (i.IncidentID % 40),
       DATEADD(MINUTE, 2 + i.IncidentID % 6,  i.DetectedAt),
       CASE WHEN i.Status IN ('Responded','Cleared','Closed')
            THEN DATEADD(MINUTE, 6 + i.IncidentID % 14, i.DetectedAt) END,
       CASE WHEN i.Status IN ('Cleared','Closed') THEN i.RoadClearedAt END
FROM oltp.Incident i
WHERE i.Status <> 'Detected';

/* ---------------------------------------------------------------- maintenance */
;WITH n AS (SELECT TOP (40) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
INSERT INTO oltp.Maintenance (MaintenanceType, SensorID, TrafficLightID, CameraID, RoadSegmentID, StartDate, EndDate, CostEur)
SELECT CASE n.i % 4 WHEN 0 THEN 'Calibration' WHEN 1 THEN 'Repair' WHEN 2 THEN 'Resurfacing' ELSE 'Replacement' END,
       CASE WHEN n.i % 4 = 0 THEN 1 + n.i % 200 END,
       CASE WHEN n.i % 4 = 1 THEN 1 + n.i % 30  END,
       CASE WHEN n.i % 4 = 3 THEN 1 + n.i % 50  END,
       CASE WHEN n.i % 4 = 2 THEN 1 + n.i % 120 END,
       DATEADD(DAY, -(n.i * 5 % 300), '2026-06-30'),
       DATEADD(DAY, -(n.i * 5 % 300) + 1 + n.i % 5, '2026-06-30'),
       CAST(250 + (n.i * 137) % 9000 AS DECIMAL(10,2))
FROM n;

PRINT 'TrafficOLTP sample data loaded.';
GO

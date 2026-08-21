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

/* one traffic light per signalized intersection = 30 lights.
   (Of the 60 intersections above, i % 4 IN (1,2) makes exactly 30 'Signalized';
   roundabouts and priority junctions have no controller.) */
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

/* 180 incidents over June 2026, in every lifecycle stage.

   MILESTONE MONOTONICITY IS A HARD INVARIANT of the accumulating-snapshot fact:
       Detected <= Dispatched <= Arrived <= RoadCleared <= Closed
   Every milestone below is therefore DERIVED FROM ITS PREDECESSOR (offset added
   to the previous milestone) instead of being computed independently from
   DetectedAt. Independent offsets were the earlier defect: RoadClearedAt used
   (25 + i%90) minutes and ClosedAt used (45 + i%120), so for e.g. incident 120
   the road cleared at +55 min but the incident "closed" at +45 min — before it
   was cleared. That produced permanent failures of the MILESTONE_ORDER_FIL
   quality check (sql/etl/05_quality_checks.sql).
   Deliberate DATA-QUALITY defects belong in the file feeds, not in the OLTP
   system of record — see data_generator/generate_data.py.

   SEVERITY AND DURATIONS ARE MODELLED, NOT COUNTED OFF. The earlier version
   used plain arithmetic on the row number: Severity = 1 + (i*3 % 5) cycled
   4,2,5,3,1 independently of the incident TYPE, so every category averaged
   severity 3.000000 and oltp.IncidentType.DefaultSeverity — which exists
   precisely to say a pedestrian accident is worse than road works — was never
   reflected anywhere. The response lags marched 5,6,7,8,9,... in IncidentID
   order, which is plainly visible in the first screen of the incident tab.

   Instead each incident draws from SHA2_256(i || purpose), which is
   deterministic (same database on every rebuild, so tests stay reproducible)
   but carries no ordering: consecutive incidents get unrelated values. The
   draws then feed a small behavioural model —
       severity   = the TYPE's default, jittered +/-1, clamped to 1..5
       lanes      = more lanes blocked the more severe the incident
       clearance  = longer for severe incidents
   — so the numbers answer "why" and not merely "what".

   CHECKSUM(...) % 100000 is taken BEFORE ABS: ABS(CHECKSUM(...)) alone can
   overflow on the single value -2147483648, which has no positive counterpart
   in INT.                                                                    */
;WITH n AS (SELECT TOP (180) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects),
 h AS (   /* deterministic, order-free pseudo-random draws, one per purpose */
    SELECT n.i,
           1 + (n.i % 9) AS IncidentTypeID,
           ABS(CHECKSUM(HASHBYTES('SHA2_256', CONCAT(CAST(n.i AS VARCHAR(10)), '|sev'  ))) % 100000) AS hSev,
           ABS(CHECKSUM(HASHBYTES('SHA2_256', CONCAT(CAST(n.i AS VARCHAR(10)), '|lanes'))) % 100000) AS hLanes,
           ABS(CHECKSUM(HASHBYTES('SHA2_256', CONCAT(CAST(n.i AS VARCHAR(10)), '|clear'))) % 100000) AS hClear,
           ABS(CHECKSUM(HASHBYTES('SHA2_256', CONCAT(CAST(n.i AS VARCHAR(10)), '|close'))) % 100000) AS hClose
    FROM n
 ),
 s AS (   /* severity: the TYPE's own default, jittered, clamped to CK_Incident_Sev */
    SELECT h.*,
           CASE WHEN it.DefaultSeverity + j.d < 1 THEN 1
                WHEN it.DefaultSeverity + j.d > 5 THEN 5
                ELSE it.DefaultSeverity + j.d END AS Severity
    FROM h
    JOIN oltp.IncidentType it ON it.IncidentTypeID = h.IncidentTypeID
    CROSS APPLY (SELECT CASE WHEN h.hSev % 10 < 2 THEN -1     -- 20% one milder
                             WHEN h.hSev % 10 > 7 THEN  1     -- 20% one worse
                             ELSE 0 END AS d) j               -- 60% at the type default
 ),
 m AS (
    SELECT s.*,
           DATEADD(MINUTE, (s.i * 227) % 43200, '2026-06-01')  AS DetectedAt,
           /* clearance 27..90 min after detection, longer the worse it is.
              The MINIMUM here (20 + 0 + 1*7 = 27) must stay above the MAXIMUM
              dispatch+arrival below (6 + 15 = 21) or Arrived <= RoadCleared
              breaks and MILESTONE_ORDER_FIL fails. */
           20 + (s.hClear % 45) + s.Severity * 7               AS ClearOffsetMin,
           /* administrative closure a further 19..80 min after clearance */
           15 + (s.hClose % 50) + s.Severity * 4               AS CloseOffsetMin
    FROM s
 )
INSERT INTO oltp.Incident
      (IncidentNumber, IncidentTypeID, RoadSegmentID, DetectedAt, Severity, LanesBlocked, Description, Status, RoadClearedAt, ClosedAt)
SELECT CONCAT('INC-2026-', RIGHT('00000' + CAST(m.i AS VARCHAR(5)), 5)),
       m.IncidentTypeID,
       1 + (m.i * 7 % 120),
       m.DetectedAt,                                                -- spread over 30 days
       m.Severity,
       /* a severe incident closes more of the carriageway than a signal fault */
       CASE WHEN m.Severity >= 5 THEN 2 + (m.hLanes % 2)             -- 2-3 lanes
            WHEN m.Severity  = 4 THEN 1 + (m.hLanes % 2)             -- 1-2 lanes
            WHEN m.Severity  = 3 THEN 1
            ELSE m.hLanes % 2 END,                                   -- 0-1 lanes
       CONCAT(N'Auto-generated incident #', m.i),
       CASE WHEN m.i % 10 = 0 THEN 'Detected'
            WHEN m.i % 10 = 1 THEN 'Responded'
            WHEN m.i % 10 = 2 THEN 'Cleared'
            ELSE 'Closed' END,
       CASE WHEN m.i % 10 IN (0,1) THEN NULL
            ELSE DATEADD(MINUTE, m.ClearOffsetMin, m.DetectedAt) END,
       CASE WHEN m.i % 10 IN (0,1,2) THEN NULL
            ELSE DATEADD(MINUTE, m.ClearOffsetMin + m.CloseOffsetMin, m.DetectedAt) END
FROM m;

/* Police responses for all non-'Detected' incidents.

   Same monotonicity rule: ArrivedAt is derived FROM DispatchedAt, so
   CK_Response_Order (ArrivedAt >= DispatchedAt) holds by construction, and the
   maximum dispatch+arrival (6 + 15 = 21 min) stays below the minimum clearance
   offset (27 min) so Arrived <= RoadCleared always holds too.

   The lags were previously 2 + IncidentID % 6 and 4 + IncidentID % 13, which
   made MinutesToArrive run 5,6,7,8,9,10,... straight down the incident tab.
   They now come from the same SHA2_256 draw as the incident itself, and a
   severe incident is prioritised: control rooms dispatch and reach a serious
   collision faster than a signal fault, which is what makes the response-time
   analysis in the mart worth running at all.                                 */
INSERT INTO oltp.PoliceResponse (IncidentID, EmergencyVehicleID, DispatchedAt, ArrivedAt, ClearedAt)
SELECT i.IncidentID,
       1 + (i.IncidentID % 40),
       d.DispatchedAt,
       CASE WHEN i.Status IN ('Responded','Cleared','Closed')
            THEN DATEADD(MINUTE, a.ArriveMin, d.DispatchedAt) END,
       CASE WHEN i.Status IN ('Cleared','Closed') THEN i.RoadClearedAt END
FROM oltp.Incident i
CROSS APPLY (SELECT ABS(CHECKSUM(HASHBYTES('SHA2_256', CONCAT(CAST(i.IncidentID AS VARCHAR(10)), '|disp'))) % 100000) AS hD,
                    ABS(CHECKSUM(HASHBYTES('SHA2_256', CONCAT(CAST(i.IncidentID AS VARCHAR(10)), '|arr' ))) % 100000) AS hA) hh
/* severity >= 4 is prioritised: dispatched in 1-4 min instead of 3-6,
   on scene in 3-10 min instead of 8-15 */
CROSS APPLY (SELECT 1 + (hh.hD % 4) + CASE WHEN i.Severity >= 4 THEN 0 ELSE 2 END AS DispMin) dm
CROSS APPLY (SELECT DATEADD(MINUTE, dm.DispMin, i.DetectedAt) AS DispatchedAt) d
CROSS APPLY (SELECT 3 + (hh.hA % 8) + CASE WHEN i.Severity >= 4 THEN 0 ELSE 5 END AS ArriveMin) a
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

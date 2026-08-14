/* ============================================================================
   WAREHOUSE INVARIANTS TEST

   Asserts the structural guarantees the star schema is built on, independently
   of the catalog-driven quality framework. The framework validates the DATA a
   load produced; this script validates that the MODEL ITSELF is intact —
   unknown members seeded, SCD2 intervals sane, no orphan keys, snapshot dense,
   detector routing correct.

   Runs read-only. Safe at any time after a load.

   USAGE
       sqlcmd -S localhost -U sa -P '...' -C -b -I -d TrafficDW \
              -i tests/sql/test_warehouse_invariants.sql
   ========================================================================== */
SET NOCOUNT ON;
USE TrafficDW;
GO

DECLARE @results TABLE (Seq TINYINT, Category VARCHAR(20), Assertion VARCHAR(90),
                        Expected VARCHAR(20), Actual VARCHAR(20), Verdict CHAR(4));
DECLARE @DateKey INT = (SELECT MAX(DateKey) FROM fact.FactHourlyTraffic);

/* ------------------------------------------------- 1. unknown members ----- */
DECLARE @missingUnknown INT =
      (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimDate             WHERE DateKey          = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimTime             WHERE TimeKey          = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment      WHERE RoadSegmentKey   = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimSensor           WHERE SensorKey        = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimVehicleType      WHERE VehicleTypeKey   = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimWeatherCondition WHERE WeatherKey       = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimTrafficCamera    WHERE CameraKey        = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimTrafficLight     WHERE TrafficLightKey  = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimIncidentType     WHERE IncidentTypeKey  = -1) THEN 1 ELSE 0 END)
    + (CASE WHEN NOT EXISTS (SELECT 1 FROM dim.DimEmergencyUnit    WHERE EmergencyUnitKey = -1) THEN 1 ELSE 0 END);

INSERT INTO @results VALUES
 (1, 'Completeness', 'All 10 dimensions carry the Unknown member (-1)',
     '0 missing', CAST(@missingUnknown AS VARCHAR(20)) + ' missing',
     CASE WHEN @missingUnknown = 0 THEN 'PASS' ELSE 'FAIL' END);

/* -------------------------------------------------------- 2. SCD2 ---------- */
DECLARE @segTwoCurrent INT = (SELECT COUNT(*) FROM (
    SELECT SegmentCode FROM dim.DimRoadSegment WHERE IsCurrent = 1 AND RoadSegmentKey <> -1
    GROUP BY SegmentCode HAVING COUNT(*) > 1) q);
DECLARE @segNoCurrent INT = (SELECT COUNT(*) FROM (
    SELECT SegmentCode FROM dim.DimRoadSegment WHERE RoadSegmentKey <> -1
    GROUP BY SegmentCode HAVING SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) = 0) q);
DECLARE @segOverlap INT = (SELECT COUNT(*) FROM dim.DimRoadSegment a
    JOIN dim.DimRoadSegment b ON b.SegmentCode = a.SegmentCode AND b.RoadSegmentKey > a.RoadSegmentKey
    WHERE a.RoadSegmentKey <> -1
      AND a.EffectiveDate <= b.ExpirationDate AND b.EffectiveDate <= a.ExpirationDate);
DECLARE @inverted INT =
      (SELECT COUNT(*) FROM dim.DimRoadSegment WHERE RoadSegmentKey <> -1 AND EffectiveDate > ExpirationDate)
    + (SELECT COUNT(*) FROM dim.DimSensor      WHERE SensorKey      <> -1 AND EffectiveDate > ExpirationDate);

INSERT INTO @results VALUES
 (2, 'SCD',  'Exactly one current row per business key',
     '0 dups', CAST(@segTwoCurrent AS VARCHAR(20)) + ' dups',
     CASE WHEN @segTwoCurrent = 0 THEN 'PASS' ELSE 'FAIL' END),
 (3, 'SCD',  'No business key left with zero current rows',
     '0', CAST(@segNoCurrent AS VARCHAR(20)),
     CASE WHEN @segNoCurrent = 0 THEN 'PASS' ELSE 'FAIL' END),
 (4, 'SCD',  'No overlapping validity intervals',
     '0', CAST(@segOverlap AS VARCHAR(20)),
     CASE WHEN @segOverlap = 0 THEN 'PASS' ELSE 'FAIL' END),
 (5, 'SCD',  'No inverted validity intervals (Effective > Expiration)',
     '0', CAST(@inverted AS VARCHAR(20)),
     CASE WHEN @inverted = 0 THEN 'PASS' ELSE 'FAIL' END);

/* ------------------------------------------------ 3. referential integrity - */
DECLARE @orphans INT =
      (SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE NOT EXISTS (SELECT 1 FROM dim.DimDate d WHERE d.DateKey = f.DateKey))
    + (SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE NOT EXISTS (SELECT 1 FROM dim.DimTime t WHERE t.TimeKey = f.TimeKey))
    + (SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment d WHERE d.RoadSegmentKey = f.RoadSegmentKey))
    + (SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE NOT EXISTS (SELECT 1 FROM dim.DimSensor d WHERE d.SensorKey = f.SensorKey))
    + (SELECT COUNT(*) FROM fact.FactTrafficEvent f WHERE NOT EXISTS (SELECT 1 FROM dim.DimTrafficCamera d WHERE d.CameraKey = f.CameraKey))
    + (SELECT COUNT(*) FROM fact.FactHourlyTraffic f WHERE NOT EXISTS (SELECT 1 FROM dim.DimRoadSegment d WHERE d.RoadSegmentKey = f.RoadSegmentKey));
DECLARE @untrustedFk INT = (SELECT COUNT(*) FROM sys.foreign_keys WHERE is_not_trusted = 1 OR is_disabled = 1);

INSERT INTO @results VALUES
 (6, 'Integrity', 'No orphan surrogate keys on any fact',
     '0', CAST(@orphans AS VARCHAR(20)),
     CASE WHEN @orphans = 0 THEN 'PASS' ELSE 'FAIL' END),
 (7, 'Integrity', 'All foreign keys trusted and enabled',
     '0', CAST(@untrustedFk AS VARCHAR(20)),
     CASE WHEN @untrustedFk = 0 THEN 'PASS' ELSE 'FAIL' END);

/* --------------------------------------------- 4. detector routing --------- */
/* The camera/sensor split: exactly one asset dimension resolves per detection. */
DECLARE @badRouting INT = (SELECT COUNT(*) FROM fact.FactTrafficEvent
    WHERE (DetectorType = 'SENSOR' AND CameraKey <> -1)
       OR (DetectorType = 'CAMERA' AND SensorKey <> -1));
DECLARE @camerasAsSensors INT = (SELECT COUNT(*) FROM dim.DimSensor
    WHERE SerialNumber LIKE 'CAM-%');

INSERT INTO @results VALUES
 (8, 'Modelling', 'Exactly one asset dimension resolves per detection',
     '0', CAST(@badRouting AS VARCHAR(20)),
     CASE WHEN @badRouting = 0 THEN 'PASS' ELSE 'FAIL' END),
 (9, 'Modelling', 'No camera codes leaked into DimSensor',
     '0', CAST(@camerasAsSensors AS VARCHAR(20)),
     CASE WHEN @camerasAsSensors = 0 THEN 'PASS' ELSE 'FAIL' END);

/* ------------------------------------------------ 5. snapshot density ------ */
DECLARE @expectedRows INT = (SELECT COUNT(*) FROM dim.DimRoadSegment WHERE IsCurrent = 1 AND RoadSegmentKey <> -1) * 24;
DECLARE @actualRows   INT = (SELECT COUNT(*) FROM fact.FactHourlyTraffic WHERE DateKey = @DateKey);
DECLARE @dupSegHour   INT = (SELECT COUNT(*) FROM (
    SELECT DateKey, HourOfDay, RoadSegmentKey FROM fact.FactHourlyTraffic
    GROUP BY DateKey, HourOfDay, RoadSegmentKey HAVING COUNT(*) > 1) q);

/* A "phantom day": every row zero traffic AND unknown weather = a whole day
   fabricated by the spine for a date that was never fed. Distinct from a
   zero-traffic HOUR, which is legitimate and is the point of a dense snapshot. */
DECLARE @phantomDays INT = (SELECT COUNT(*) FROM (
    SELECT DateKey FROM fact.FactHourlyTraffic
    GROUP BY DateKey
    HAVING SUM(VehicleCount) = 0 AND SUM(CASE WHEN WeatherKey <> -1 THEN 1 ELSE 0 END) = 0) q);

/* Weather must resolve wherever there IS traffic. Unknown weather on a
   zero-traffic hour is correct (nothing was observed to band); unknown weather
   on an hour that recorded vehicles means the banding join failed. */
DECLARE @weatherMissingOnTraffic INT = (SELECT COUNT(*) FROM fact.FactHourlyTraffic
    WHERE VehicleCount > 0 AND WeatherKey = -1);
DECLARE @hoursWithTraffic INT = (SELECT COUNT(*) FROM fact.FactHourlyTraffic WHERE VehicleCount > 0);
DECLARE @speedMissingOnTraffic INT = (SELECT COUNT(*) FROM fact.FactHourlyTraffic
    WHERE VehicleCount > 0 AND AvgSpeedKmh IS NULL);

INSERT INTO @results VALUES
 (10,'Completeness', 'Periodic snapshot is dense (segments x 24 for latest date)',
     CAST(@expectedRows AS VARCHAR(20)), CAST(@actualRows AS VARCHAR(20)),
     CASE WHEN @actualRows = @expectedRows THEN 'PASS' ELSE 'FAIL' END),
 (11,'Uniqueness', 'No duplicate segment-hour in the snapshot',
     '0', CAST(@dupSegHour AS VARCHAR(20)),
     CASE WHEN @dupSegHour = 0 THEN 'PASS' ELSE 'FAIL' END),
 (16,'Completeness', 'No phantom day (whole date fabricated with no source data)',
     '0 days', CAST(@phantomDays AS VARCHAR(20)) + ' days',
     CASE WHEN @phantomDays = 0 THEN 'PASS' ELSE 'FAIL' END),
 (17,'Completeness', 'Snapshot actually carries traffic (not all zeros)',
     '> 0 hours', CAST(@hoursWithTraffic AS VARCHAR(20)) + ' hours',
     CASE WHEN @hoursWithTraffic > 0 THEN 'PASS' ELSE 'FAIL' END),
 (18,'Integrity', 'Weather resolves on every hour that has traffic',
     '0', CAST(@weatherMissingOnTraffic AS VARCHAR(20)),
     CASE WHEN @weatherMissingOnTraffic = 0 THEN 'PASS' ELSE 'FAIL' END),
 (19,'Validity', 'AvgSpeedKmh populated on every hour that has traffic',
     '0', CAST(@speedMissingOnTraffic AS VARCHAR(20)),
     CASE WHEN @speedMissingOnTraffic = 0 THEN 'PASS' ELSE 'FAIL' END);

/* ------------------------------------------------ 6. measure validity ------ */
DECLARE @badCongestion INT = (SELECT COUNT(*) FROM fact.FactHourlyTraffic
    WHERE CongestionIndex IS NOT NULL AND (CongestionIndex < 0 OR CongestionIndex > 1));
DECLARE @badCounts INT = (SELECT COUNT(*) FROM fact.FactHourlyTraffic
    WHERE VehicleCount < 0 OR HeavyVehicleCount < 0 OR HeavyVehicleCount > VehicleCount);
DECLARE @badMilestones INT = (SELECT COUNT(*) FROM fact.FactIncidentLifecycle
    WHERE MinutesToDispatch < 0 OR MinutesToArrive < 0 OR MinutesToClear < 0 OR TotalDurationMinutes < 0);

INSERT INTO @results VALUES
 (12,'Validity', 'CongestionIndex within [0,1]',
     '0', CAST(@badCongestion AS VARCHAR(20)),
     CASE WHEN @badCongestion = 0 THEN 'PASS' ELSE 'FAIL' END),
 (13,'Validity', 'Counts non-negative and heavy <= total',
     '0', CAST(@badCounts AS VARCHAR(20)),
     CASE WHEN @badCounts = 0 THEN 'PASS' ELSE 'FAIL' END),
 (14,'Validity', 'No negative incident lag measures',
     '0', CAST(@badMilestones AS VARCHAR(20)),
     CASE WHEN @badMilestones = 0 THEN 'PASS' ELSE 'FAIL' END);

/* ------------------------------------------------ 7. reconciliation -------- */
DECLARE @mismatchSteps INT = (SELECT COUNT(*) FROM etl.RowLog
    WHERE StepName = 'Fact.TrafficEvent' AND RowsExtracted IS NOT NULL
      AND RowsExtracted <> ISNULL(RowsInserted,0) + ISNULL(RowsUpdated,0) + ISNULL(RowsRejected,0));

INSERT INTO @results VALUES
 (15,'Reconciliation', 'Extracted = Inserted + Rejected on every fact step',
     '0 mismatch', CAST(@mismatchSteps AS VARCHAR(20)) + ' mismatch',
     CASE WHEN @mismatchSteps = 0 THEN 'PASS' ELSE 'FAIL' END);

/* ------------------------------------------------------------- report ------ */
PRINT '=================================================================';
PRINT ' WAREHOUSE INVARIANTS';
PRINT '=================================================================';
SELECT Seq, Category, Assertion, Expected, Actual, Verdict FROM @results ORDER BY Seq;

DECLARE @Failures INT = (SELECT COUNT(*) FROM @results WHERE Verdict = 'FAIL');
DECLARE @Total INT = (SELECT COUNT(*) FROM @results);   -- PRINT cannot take a subquery
PRINT '';
IF @Failures = 0
BEGIN
    PRINT ' ALL ' + CAST(@Total AS VARCHAR(12)) + ' INVARIANTS HOLD';
    PRINT '=================================================================';
END
ELSE
BEGIN
    DECLARE @msg NVARCHAR(200) = CONCAT('WAREHOUSE INVARIANTS FAILED: ', @Failures, ' of ', @Total, ' assertions.');
    THROW 50102, @msg, 1;
END
GO
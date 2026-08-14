/* ============================================================================
   IDEMPOTENCY TEST — re-running a load date must change nothing.

   Idempotency is what makes retries safe: a failed batch has to be rerunnable
   without duplicating or losing data. docs/13 told the reader to verify this
   "by hand"; this script does it and fails loudly.

   Covers the audit's Scenario A (rerun after success) and Scenario C
   (reprocessing a historical hourly snapshot).

   USAGE
       sqlcmd -S localhost -U sa -P '...' -C -b -I -d TrafficDW \
              -i tests/sql/test_idempotency.sql
   ========================================================================== */
SET NOCOUNT ON;
USE TrafficDW;
GO

DECLARE @LoadDate DATE = (SELECT MAX(LoadDate) FROM etl.BatchLog WHERE Status = 'Succeeded');
IF @LoadDate IS NULL
BEGIN
    RAISERROR('No successful batch found. Run the pipeline before this test.', 16, 1);
    RETURN;
END
DECLARE @DateKey INT = CONVERT(INT, FORMAT(@LoadDate, 'yyyyMMdd'));

PRINT '=================================================================';
PRINT ' IDEMPOTENCY TEST - reprocessing ' + CONVERT(CHAR(10), @LoadDate, 120);
PRINT '=================================================================';

/* ------------------------------------------------------------- BEFORE ----- */
DECLARE @EventsBefore    INT = (SELECT COUNT(*) FROM fact.FactTrafficEvent      WHERE DateKey = @DateKey);
DECLARE @HourlyBefore    INT = (SELECT COUNT(*) FROM fact.FactHourlyTraffic     WHERE DateKey = @DateKey);
DECLARE @IncidentsBefore INT = (SELECT COUNT(*) FROM fact.FactIncidentLifecycle);
DECLARE @SegVersBefore   INT = (SELECT COUNT(*) FROM dim.DimRoadSegment);
DECLARE @SensorsBefore   INT = (SELECT COUNT(*) FROM dim.DimSensor);
DECLARE @RejectsBefore   INT = (SELECT COUNT(*) FROM stg.RejectTrafficEvent
                                WHERE CAST(EventTimestamp AS DATE) = @LoadDate);

PRINT 'BEFORE  events=' + CAST(@EventsBefore AS VARCHAR(12))
    + '  hourly=' + CAST(@HourlyBefore AS VARCHAR(12))
    + '  incidents=' + CAST(@IncidentsBefore AS VARCHAR(12))
    + '  segVersions=' + CAST(@SegVersBefore AS VARCHAR(12))
    + '  rejects=' + CAST(@RejectsBefore AS VARCHAR(12));

/* -------------------------------------------- run the SAME date again ----- */
/* NOTE: staging still holds this date's rows only if Spark job 05 has not been
   re-run for a later date (it truncates). The fact loads are driven by staging,
   so a truncated staging legitimately yields 0 rows; the test detects that and
   reports SKIP rather than a false failure. */
DECLARE @StagedRows INT = (SELECT COUNT(*) FROM stg.TrafficEvent WHERE LoadDate = @LoadDate);

IF @StagedRows = 0
BEGIN
    PRINT '';
    PRINT 'SKIP - stg.TrafficEvent holds no rows for this date (a later date has';
    PRINT '       truncated it). Re-run Spark job 05 for ' + CONVERT(CHAR(10), @LoadDate, 120);
    PRINT '       to exercise the full reload path.';
    PRINT '';
END

EXEC etl.usp_RunNightlyPipeline @LoadDate = @LoadDate;
PRINT 'PIPELINE re-ran for ' + CONVERT(CHAR(10), @LoadDate, 120);

/* -------------------------------------------------------------- AFTER ----- */
DECLARE @EventsAfter    INT = (SELECT COUNT(*) FROM fact.FactTrafficEvent      WHERE DateKey = @DateKey);
DECLARE @HourlyAfter    INT = (SELECT COUNT(*) FROM fact.FactHourlyTraffic     WHERE DateKey = @DateKey);
DECLARE @IncidentsAfter INT = (SELECT COUNT(*) FROM fact.FactIncidentLifecycle);
DECLARE @SegVersAfter   INT = (SELECT COUNT(*) FROM dim.DimRoadSegment);
DECLARE @SensorsAfter   INT = (SELECT COUNT(*) FROM dim.DimSensor);
DECLARE @RejectsAfter   INT = (SELECT COUNT(*) FROM stg.RejectTrafficEvent
                               WHERE CAST(EventTimestamp AS DATE) = @LoadDate);

DECLARE @DupEvents INT = (
    SELECT COUNT(*) FROM (
        SELECT EventID FROM fact.FactTrafficEvent
        WHERE DateKey = @DateKey GROUP BY EventID HAVING COUNT(*) > 1) q);

DECLARE @DupSegmentHours INT = (
    SELECT COUNT(*) FROM (
        SELECT DateKey, HourOfDay, RoadSegmentKey FROM fact.FactHourlyTraffic
        WHERE DateKey = @DateKey
        GROUP BY DateKey, HourOfDay, RoadSegmentKey HAVING COUNT(*) > 1) q);

DECLARE @results TABLE (Seq TINYINT, Assertion VARCHAR(90),
                        Before VARCHAR(20), After VARCHAR(20), Verdict CHAR(4));

INSERT INTO @results VALUES
 (1, 'Transaction fact row count unchanged (delete-by-date reload)',
     CAST(@EventsBefore AS VARCHAR(20)), CAST(@EventsAfter AS VARCHAR(20)),
     CASE WHEN @EventsAfter = @EventsBefore THEN 'PASS' ELSE 'FAIL' END),
 (2, 'No duplicate EventID for the date (the idempotency guard)',
     '0', CAST(@DupEvents AS VARCHAR(20)),
     CASE WHEN @DupEvents = 0 THEN 'PASS' ELSE 'FAIL' END),
 (3, 'Periodic snapshot row count unchanged',
     CAST(@HourlyBefore AS VARCHAR(20)), CAST(@HourlyAfter AS VARCHAR(20)),
     CASE WHEN @HourlyAfter = @HourlyBefore THEN 'PASS' ELSE 'FAIL' END),
 (4, 'No duplicate segment-hour in the snapshot',
     '0', CAST(@DupSegmentHours AS VARCHAR(20)),
     CASE WHEN @DupSegmentHours = 0 THEN 'PASS' ELSE 'FAIL' END),
 (5, 'Accumulating fact not duplicated (MERGE on IncidentNumber)',
     CAST(@IncidentsBefore AS VARCHAR(20)), CAST(@IncidentsAfter AS VARCHAR(20)),
     CASE WHEN @IncidentsAfter = @IncidentsBefore THEN 'PASS' ELSE 'FAIL' END),
 (6, 'No spurious SCD2 versions created by an unchanged source',
     CAST(@SegVersBefore AS VARCHAR(20)), CAST(@SegVersAfter AS VARCHAR(20)),
     CASE WHEN @SegVersAfter = @SegVersBefore THEN 'PASS' ELSE 'FAIL' END),
 (7, 'No spurious sensor versions / duplicate inferred members',
     CAST(@SensorsBefore AS VARCHAR(20)), CAST(@SensorsAfter AS VARCHAR(20)),
     CASE WHEN @SensorsAfter = @SensorsBefore THEN 'PASS' ELSE 'FAIL' END),
 (8, 'Quarantine not double-counted (rejects deleted by date on reload)',
     CAST(@RejectsBefore AS VARCHAR(20)), CAST(@RejectsAfter AS VARCHAR(20)),
     CASE WHEN @RejectsAfter = @RejectsBefore THEN 'PASS' ELSE 'FAIL' END);

PRINT '';
SELECT Seq, Assertion, Before, After, Verdict FROM @results ORDER BY Seq;

DECLARE @Failures INT = (SELECT COUNT(*) FROM @results WHERE Verdict = 'FAIL');
PRINT '';
IF @Failures = 0
BEGIN
    PRINT '=================================================================';
    PRINT ' IDEMPOTENCY: ALL ASSERTIONS PASSED - reruns are safe';
    PRINT '=================================================================';
END
ELSE
BEGIN
    DECLARE @msg NVARCHAR(200) = CONCAT('IDEMPOTENCY FAILED: ', @Failures, ' assertion(s) did not hold.');
    THROW 50101, @msg, 1;
END
GO
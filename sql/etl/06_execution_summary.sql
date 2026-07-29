/* ============================================================================
   TrafficDW — EXECUTION SUMMARY (docs/13_execution_runbook.md)

   etl.usp_ExecutionSummary renders a production-style run report over a load
   window, sourced entirely from the framework's own audit trail
   (etl.BatchLog / etl.RowLog / etl.QualityCheckLog) plus warehouse counts —
   the same numbers an on-call engineer would pull after a nightly run.

   Result sets:
     1. run-report metrics (Metric | Value), ordered like a pipeline printout
     2. per-batch detail (status, duration, rows in/out/rejected)
     3. rejected rows by reason (from the quarantine table)

   Usage:
     EXEC etl.usp_ExecutionSummary;                                -- whole history
     EXEC etl.usp_ExecutionSummary '2026-06-01', '2026-06-07';     -- a run window
   ========================================================================== */
USE TrafficDW;
GO

CREATE OR ALTER PROCEDURE etl.usp_ExecutionSummary
    @LoadDateFrom DATE = NULL,     -- default: earliest batch
    @LoadDateTo   DATE = NULL      -- default: latest batch
AS
BEGIN
    SET NOCOUNT ON;

    IF @LoadDateFrom IS NULL SET @LoadDateFrom = ISNULL((SELECT MIN(LoadDate) FROM etl.BatchLog), '1900-01-01');
    IF @LoadDateTo   IS NULL SET @LoadDateTo   = ISNULL((SELECT MAX(LoadDate) FROM etl.BatchLog), '9999-12-30');

    DECLARE @DateKeyFrom INT = CONVERT(INT, FORMAT(@LoadDateFrom, 'yyyyMMdd'));
    DECLARE @DateKeyTo   INT = CONVERT(INT, FORMAT(@LoadDateTo,   'yyyyMMdd'));

    DECLARE @report TABLE (Seq SMALLINT, Metric VARCHAR(60), Value VARCHAR(60));

    /* ------------------------------------------------------------ batches --- */
    INSERT INTO @report
    SELECT 10, 'Load window',
           CONCAT(CONVERT(CHAR(10), @LoadDateFrom, 120), ' .. ', CONVERT(CHAR(10), @LoadDateTo, 120));

    INSERT INTO @report
    SELECT 11, 'Batches executed',            FORMAT(COUNT(*), 'N0')
    FROM etl.BatchLog WHERE LoadDate BETWEEN @LoadDateFrom AND @LoadDateTo;

    INSERT INTO @report
    SELECT 12, 'Batches succeeded / failed',
           CONCAT(FORMAT(SUM(CASE WHEN Status = 'Succeeded' THEN 1 ELSE 0 END), 'N0'), ' / ',
                  FORMAT(SUM(CASE WHEN Status = 'Failed'    THEN 1 ELSE 0 END), 'N0'))
    FROM etl.BatchLog WHERE LoadDate BETWEEN @LoadDateFrom AND @LoadDateTo;

    INSERT INTO @report
    SELECT 13, 'Total execution time',
           CONCAT(FORMAT(SUM(DATEDIFF(SECOND, StartedAt, ISNULL(FinishedAt, StartedAt))) / 60, 'N0'), ' min ',
                  FORMAT(SUM(DATEDIFF(SECOND, StartedAt, ISNULL(FinishedAt, StartedAt))) % 60, 'N0'), ' s')
    FROM etl.BatchLog WHERE LoadDate BETWEEN @LoadDateFrom AND @LoadDateTo;

    /* ------------------------------------------------- warehouse row counts -- */
    INSERT INTO @report
    SELECT 20, 'Traffic events loaded (transaction fact)', FORMAT(COUNT_BIG(*), 'N0')
    FROM fact.FactTrafficEvent WHERE DateKey BETWEEN @DateKeyFrom AND @DateKeyTo;

    INSERT INTO @report
    SELECT 21, 'Hourly snapshot rows (periodic fact)', FORMAT(COUNT_BIG(*), 'N0')
    FROM fact.FactHourlyTraffic WHERE DateKey BETWEEN @DateKeyFrom AND @DateKeyTo;

    INSERT INTO @report
    SELECT 22, 'Incidents tracked (accumulating fact)', FORMAT(COUNT(*), 'N0')
    FROM fact.FactIncidentLifecycle WHERE DetectedDateKey BETWEEN @DateKeyFrom AND @DateKeyTo;

    INSERT INTO @report
    SELECT 23, 'Active sensors (current dimension rows)', FORMAT(COUNT(*), 'N0')
    FROM dim.DimSensor WHERE IsCurrent = 1 AND SensorKey <> -1;

    INSERT INTO @report
    SELECT 24, 'Active road segments (current dimension rows)', FORMAT(COUNT(*), 'N0')
    FROM dim.DimRoadSegment WHERE IsCurrent = 1 AND RoadSegmentKey <> -1;

    /* ----------------------------------------------------- ETL side effects -- */
    INSERT INTO @report
    SELECT 30, 'Rows rejected (quarantined, replayable)', FORMAT(ISNULL(SUM(r.RowsRejected), 0), 'N0')
    FROM etl.RowLog r
    JOIN etl.BatchLog b ON b.ETLBatchID = r.ETLBatchID
    WHERE b.LoadDate BETWEEN @LoadDateFrom AND @LoadDateTo;

    INSERT INTO @report
    SELECT 31, 'Inferred members created (late-arriving dims)', FORMAT(ISNULL(SUM(r.RowsInserted), 0), 'N0')
    FROM etl.RowLog r
    JOIN etl.BatchLog b ON b.ETLBatchID = r.ETLBatchID
    WHERE b.LoadDate BETWEEN @LoadDateFrom AND @LoadDateTo
      AND r.StepName LIKE 'InferredMembers%';

    INSERT INTO @report
    SELECT 32, 'SCD2 versions created (RoadSegment + Sensor)',
           FORMAT(
             (SELECT COUNT(*) FROM dim.DimRoadSegment
              WHERE VersionNumber > 1 AND EffectiveDate BETWEEN @LoadDateFrom AND @LoadDateTo)
           + (SELECT COUNT(*) FROM dim.DimSensor
              WHERE VersionNumber > 1 AND EffectiveDate BETWEEN @LoadDateFrom AND @LoadDateTo), 'N0');

    /* -------------------------------------------------------- quality gate -- */
    INSERT INTO @report
    SELECT 40, 'Quality checks run (passed / failed)',
           CONCAT(FORMAT(COUNT(*), 'N0'), ' (',
                  FORMAT(SUM(CASE WHEN q.Status = 'Pass' THEN 1 ELSE 0 END), 'N0'), ' / ',
                  FORMAT(SUM(CASE WHEN q.Status = 'Fail' THEN 1 ELSE 0 END), 'N0'), ')')
    FROM etl.QualityCheckLog q
    JOIN etl.BatchLog b ON b.ETLBatchID = q.ETLBatchID
    WHERE b.LoadDate BETWEEN @LoadDateFrom AND @LoadDateTo;

    SELECT Metric, Value FROM @report ORDER BY Seq;

    /* ------------------------------------------------ 2. per-batch detail --- */
    SELECT b.ETLBatchID,
           b.PipelineName,
           b.LoadDate,
           b.Status,
           DATEDIFF(SECOND, b.StartedAt, ISNULL(b.FinishedAt, b.StartedAt)) AS DurationSec,
           ISNULL(SUM(r.RowsExtracted), 0) AS RowsExtracted,
           ISNULL(SUM(r.RowsInserted), 0)  AS RowsInserted,
           ISNULL(SUM(r.RowsUpdated), 0)   AS RowsUpdated,
           ISNULL(SUM(r.RowsRejected), 0)  AS RowsRejected
    FROM etl.BatchLog b
    LEFT JOIN etl.RowLog r ON r.ETLBatchID = b.ETLBatchID
    WHERE b.LoadDate BETWEEN @LoadDateFrom AND @LoadDateTo
    GROUP BY b.ETLBatchID, b.PipelineName, b.LoadDate, b.Status, b.StartedAt, b.FinishedAt
    ORDER BY b.ETLBatchID;

    /* --------------------------------------------- 3. rejects by reason ----- */
    SELECT r.RejectReason,
           COUNT_BIG(*) AS RejectedRows,
           MIN(CAST(r.EventTimestamp AS DATE)) AS FirstEventDate,
           MAX(CAST(r.EventTimestamp AS DATE)) AS LastEventDate
    FROM stg.RejectTrafficEvent r
    JOIN etl.BatchLog b ON b.ETLBatchID = r.ETLBatchID
    WHERE b.LoadDate BETWEEN @LoadDateFrom AND @LoadDateTo
    GROUP BY r.RejectReason
    ORDER BY RejectedRows DESC;
END
GO

PRINT 'Execution summary procedure created (etl.usp_ExecutionSummary).';
GO

/* ============================================================================
   TrafficDW — analytical data warehouse (Kimball star schema)
   Schemas:
     dim  — conformed dimensions          fact — fact tables
     stg  — staging (truncate-and-load)   etl  — ETL framework (logs, watermarks)
     mart — presentation views
   ========================================================================== */
IF DB_ID(N'TrafficDW') IS NULL
BEGIN
    CREATE DATABASE TrafficDW;
END
GO

ALTER DATABASE TrafficDW SET RECOVERY SIMPLE;
GO

USE TrafficDW;
GO

DECLARE @s NVARCHAR(20);
DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT s FROM (VALUES (N'dim'),(N'fact'),(N'stg'),(N'etl'),(N'mart')) v(s)
    WHERE SCHEMA_ID(s) IS NULL;
OPEN c;
FETCH NEXT FROM c INTO @s;
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC (N'CREATE SCHEMA ' + @s + N' AUTHORIZATION dbo;');
    FETCH NEXT FROM c INTO @s;
END
CLOSE c; DEALLOCATE c;
GO

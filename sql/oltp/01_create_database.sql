/* ============================================================================
   TrafficOLTP — source (operational) database
   Part 2: Source System
   ========================================================================== */
IF DB_ID(N'TrafficOLTP') IS NULL
BEGIN
    CREATE DATABASE TrafficOLTP;
END
GO

ALTER DATABASE TrafficOLTP SET RECOVERY SIMPLE;  -- demo environment
GO

USE TrafficOLTP;
GO

IF SCHEMA_ID(N'oltp') IS NULL
    EXEC (N'CREATE SCHEMA oltp AUTHORIZATION dbo;');
GO

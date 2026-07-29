/* ============================================================================
   TrafficDW — security: the SQL login Spark job 05 uses for its JDBC
   staging loads (spark/config/spark_config.py).

   SQL authentication is used (not integrated auth) because Spark connects
   from any platform — the docker containers and Linux hosts have no Windows
   identity to present.

   Least privilege: etl_spark can read/write/truncate ONLY the stg schema.
   dim / fact / mart and the etl framework are reachable exclusively through
   the T-SQL procedures, which run under the operator's own connection
   (sqlcmd / SQL Agent / Airflow task) — Spark never touches them (docs/07).

   The demo password matches spark_config.py; in a real deployment override
   BOTH from a secret store (the config file says the same).

   Run AFTER sql/warehouse/01_create_warehouse.sql (needs TrafficDW + stg).
   Idempotent: safe to re-run.
   ========================================================================== */
USE master;
GO
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'etl_spark' AND type = 'S')
BEGIN
    CREATE LOGIN etl_spark
    WITH PASSWORD = N'ChangeMe_Demo1!',      -- demo only — rotate via secret store
         CHECK_EXPIRATION = OFF;
END
GO

USE TrafficDW;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'etl_spark' AND type = 'S')
BEGIN
    CREATE USER etl_spark FOR LOGIN etl_spark;
END
GO

/* SELECT — the JDBC writer probes table schemas (SELECT * WHERE 1=0);
   INSERT — batched staging writes;
   ALTER  — required by the JDBC truncate=true option (TRUNCATE TABLE needs
            ALTER permission on the table; schema-scoped keeps it to stg). */
GRANT SELECT, INSERT, ALTER ON SCHEMA::stg TO etl_spark;
GO

PRINT 'etl_spark login/user created and granted stg-schema access.';
GO

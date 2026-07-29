"""Airflow DAG — nightly Smart City Traffic warehouse refresh.

Maps 1:1 to the documented ETL flow (docs/06): generator-fed raw files are
already landed; the DAG runs Spark bronze→silver→gold, hands over to SQL
Server, then verifies quality. Backfills work out of the box because every
task receives the data interval's date ({{ ds }}) and every step is
idempotent per date.

    ingest → clean_validate → transform_aggregate → generate_kpis
          → load_staging → run_warehouse_pipeline → quality_checks
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

MSSQL_JDBC = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
SPARK = f"spark-submit --packages {MSSQL_JDBC}"
SQLCMD = "sqlcmd -S mssql -d TrafficDW -b -Q"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),
}

with DAG(
    dag_id="traffic_dw_nightly",
    description="Bronze→Silver→Gold→SQL Server star schema, per load date",
    schedule="30 2 * * *",              # 02:30, after all daily files have landed
    start_date=datetime(2026, 6, 1),
    catchup=True,                       # enables historical backfills
    max_active_runs=1,
    default_args=default_args,
    tags=["traffic", "warehouse"],
) as dag:

    ingest = BashOperator(
        task_id="spark_ingest_bronze",
        bash_command=f"{SPARK} /opt/project/spark/jobs/01_ingest_raw.py --date {{{{ ds }}}}",
    )

    clean = BashOperator(
        task_id="spark_clean_silver",
        bash_command=f"{SPARK} /opt/project/spark/jobs/02_clean_validate.py --date {{{{ ds }}}}",
    )

    transform = BashOperator(
        task_id="spark_transform_gold",
        bash_command=f"{SPARK} /opt/project/spark/jobs/03_transform_aggregate.py --date {{{{ ds }}}}",
    )

    kpis = BashOperator(
        task_id="spark_generate_kpis",
        bash_command=f"{SPARK} /opt/project/spark/jobs/04_generate_kpis.py",
    )

    load_staging = BashOperator(
        task_id="spark_load_sql_staging",
        bash_command=f"{SPARK} /opt/project/spark/jobs/05_load_warehouse.py --date {{{{ ds }}}}",
    )

    warehouse = BashOperator(
        task_id="sql_warehouse_pipeline",
        bash_command=(
            f'{SQLCMD} "EXEC etl.usp_RunNightlyPipeline @LoadDate = \'{{{{ ds }}}}\';"'
        ),
    )

    quality = BashOperator(
        task_id="sql_quality_checks",
        # the pipeline proc already RAN the checks (log-only); this gate task
        # asserts on the results — usp_AssertQuality THROWs on any failed
        # Error-severity check, and -b turns that into a non-zero exit that
        # fails the task (sql/etl/05_quality_checks.sql, docs/14)
        bash_command=(
            f'{SQLCMD} "DECLARE @b INT = (SELECT MAX(ETLBatchID) FROM etl.BatchLog); '
            f'EXEC etl.usp_AssertQuality @b;"'
        ),
    )

    ingest >> clean >> transform >> kpis >> load_staging >> warehouse >> quality

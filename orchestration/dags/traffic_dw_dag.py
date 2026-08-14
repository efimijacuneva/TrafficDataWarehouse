"""Airflow DAG — nightly Smart City Traffic warehouse refresh (optional, docs/11).

Maps 1:1 to the documented ETL flow (docs/06): generator-fed raw files are
already landed; the DAG runs Spark bronze->silver->gold, hands over to SQL
Server, then verifies quality.

    ingest -> clean_validate -> transform_aggregate -> load_staging
           -> run_warehouse_pipeline -> quality_gate -> generate_kpis

EXECUTION MODEL
---------------
Every task `docker exec`s into the SAME containers that
scripts/run_end_to_end.ps1 drives, so there is exactly ONE way this pipeline
executes and the DAG cannot drift from the primary path.

The previous version invoked bare `spark-submit` and `sqlcmd`, neither of which
exists in the apache/airflow image — the tasks could never have run. The
compose file now gives this container the Docker CLI and the host socket
(orchestration/docker-compose.yml), which is far lighter than installing Spark
and mssql-tools into the image.

Backfills work out of the box: every task receives the data interval's date via
{{ ds }} and every step is idempotent per date.

    docker compose --profile airflow up -d      # then http://localhost:8081
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# --- how a task reaches each engine -----------------------------------------
SPARK_EXEC = (
    "docker exec -w /opt/project trafficdw-spark "
    "/opt/spark/bin/spark-submit"
)
# job 05 needs the JDBC driver that scripts/run_end_to_end.ps1 downloads into
# orchestration/jars (mounted at /opt/jars in the Spark container)
SPARK_EXEC_JDBC = f"{SPARK_EXEC} --jars /opt/jars/mssql-jdbc-12.6.1.jre11.jar"

SQLCMD = (
    "docker exec trafficdw-mssql /opt/mssql-tools18/bin/sqlcmd "
    "-S localhost -U sa -P \"$SQL_PASSWORD\" -C -b -I -d TrafficDW -Q"
)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),
}

with DAG(
    dag_id="traffic_dw_nightly",
    description="Bronze->Silver->Gold->SQL Server star schema, per load date",
    schedule="30 2 * * *",              # 02:30, after all daily files have landed
    start_date=datetime(2026, 6, 1),
    catchup=True,                       # enables historical backfills
    max_active_runs=1,                  # the warehouse load is not concurrent-safe
    default_args=default_args,
    tags=["traffic", "warehouse"],
) as dag:

    ingest = BashOperator(
        task_id="spark_ingest_bronze",
        bash_command=f"{SPARK_EXEC} /opt/project/spark/jobs/01_ingest_raw.py --date {{{{ ds }}}}",
    )

    clean = BashOperator(
        task_id="spark_clean_silver",
        bash_command=f"{SPARK_EXEC} /opt/project/spark/jobs/02_clean_validate.py --date {{{{ ds }}}}",
    )

    transform = BashOperator(
        task_id="spark_transform_gold",
        bash_command=f"{SPARK_EXEC} /opt/project/spark/jobs/03_transform_aggregate.py --date {{{{ ds }}}}",
    )

    load_staging = BashOperator(
        task_id="spark_load_sql_staging",
        bash_command=f"{SPARK_EXEC_JDBC} /opt/project/spark/jobs/05_load_warehouse.py --date {{{{ ds }}}}",
    )

    warehouse = BashOperator(
        task_id="sql_warehouse_pipeline",
        bash_command=f"{SQLCMD} \"EXEC etl.usp_RunNightlyPipeline @LoadDate = '{{{{ ds }}}}';\"",
    )

    quality = BashOperator(
        task_id="sql_quality_gate",
        # usp_RunNightlyPipeline already RAN the checks log-only, so BatchLog.Status
        # keeps meaning "the LOAD succeeded". This task asserts on the results:
        # usp_AssertQuality THROWs on any failed Error-severity check and sqlcmd -b
        # turns that into a non-zero exit, failing the task (docs/14).
        bash_command=(
            f"{SQLCMD} \"DECLARE @b INT = (SELECT MAX(ETLBatchID) FROM etl.BatchLog); "
            f"EXEC etl.usp_AssertQuality @b;\""
        ),
    )

    # KPI datasets are computed over the WHOLE gold layer, so they run last —
    # after this date's gold partition exists and the gate has approved it.
    kpis = BashOperator(
        task_id="spark_generate_kpis",
        bash_command=f"{SPARK_EXEC} /opt/project/spark/jobs/04_generate_kpis.py",
    )

    ingest >> clean >> transform >> load_staging >> warehouse >> quality >> kpis
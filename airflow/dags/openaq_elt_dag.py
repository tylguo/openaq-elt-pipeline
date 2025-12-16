from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/project"
INGEST_DIR = f"{PROJECT_ROOT}/ingest"
DBT_DIR = f"{PROJECT_ROOT}/openaq_dbt"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}


with DAG(
    dag_id="openaq_elt_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger for now
    catchup=False,
    tags=["openaq", "elt", "portfolio"],
) as dag:

    ingest_locations = BashOperator(
        task_id="ingest_locations",
        bash_command=f"""
        cd {INGEST_DIR} &&
        python ingest_locations.py
        """,
    )

    ingest_measurements = BashOperator(
        task_id="ingest_measurements",
        bash_command=f"""
        cd {INGEST_DIR} &&
        python ingest_measurements.py
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_DIR} &&
        dbt run
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_DIR} &&
        dbt test
        """,
    )

    ingest_locations >> ingest_measurements >> dbt_run >> dbt_test

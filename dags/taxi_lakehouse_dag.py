"""Airflow DAG: monthly bronze -> silver -> gold run.

Place this file in your Airflow `dags/` folder and make this repo importable
(e.g. mount it into the Airflow container and `pip install -e .` or set PYTHONPATH).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

LAKE = "/opt/airflow/data/lake"
LANDING = "/opt/airflow/data/landing"
# {{ ds[:7] }} is Airflow's run month, e.g. "2024-01".
CMD = (f"python -m lakehouse.run_pipeline --landing {LANDING} --lake {LAKE} --format delta "
       "--month {{{{ ds[:7] }}}} --stage {stage}")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,          # set `email` + SMTP in Airflow config
    "email": ["you@example.com"],
}

with DAG(
    dag_id="nyc_taxi_lakehouse",
    description="NYC taxi medallion pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",               # TLC publishes monthly files
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "pyspark"],
) as dag:
    bronze = BashOperator(task_id="bronze_ingest", bash_command=CMD.format(stage="bronze"))
    silver = BashOperator(task_id="silver_clean", bash_command=CMD.format(stage="silver"))
    gold = BashOperator(task_id="gold_aggregate", bash_command=CMD.format(stage="gold"))

    bronze >> silver >> gold

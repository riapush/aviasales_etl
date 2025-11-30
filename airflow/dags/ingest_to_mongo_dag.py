from __future__ import annotations
import os
import requests
import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

FASTAPI_HOST = os.environ.get("FASTAPI_HOST", "app")
FASTAPI_PORT = int(os.environ.get("FASTAPI_PORT", 8081))
FASTAPI_BASE = f"http://{FASTAPI_HOST}:{FASTAPI_PORT}"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def call_generate(n=50, timeout=30, **context):
    url = f"{FASTAPI_BASE}/generate"
    params = {"n": n}
    try:
        r = requests.post(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        log.info("FastAPI generate response: %s", data)
        return data
    except Exception as e:
        log.exception("Failed to call FastAPI /generate: %s", e)
        raise


with DAG(
    dag_id="ingest_to_mongo",
    start_date=days_ago(1),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingest", "mongo", "mock"],
) as dag:

    t_call_generate = PythonOperator(
        task_id="call_fastapi_generate",
        python_callable=call_generate,
        op_kwargs={"n": 60},
    )

    t_trigger_elt = TriggerDagRunOperator(
        task_id="trigger_mongo_to_postgres_elt",
        trigger_dag_id="mongo_to_postgres_elt",
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=30,
    )

    t_call_generate >> t_trigger_elt

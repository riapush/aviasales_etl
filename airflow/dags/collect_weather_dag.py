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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def call_collect_all(timeout=30, **context):
    """Вызов API для сбора текущей погоды и прогноза"""
    url = f"{FASTAPI_BASE}/collect/all"
    try:
        r = requests.post(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        log.info("Weather collection response: %s", data)
        return data
    except Exception as e:
        log.exception("Failed to call FastAPI /collect/all: %s", e)
        raise


with DAG(
    dag_id="collect_weather_data",
    start_date=days_ago(1),
    schedule_interval="0 * * * *",  # Каждый час
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["weather", "collect", "openweather"],
) as dag:

    t_collect_weather = PythonOperator(
        task_id="collect_current_and_forecast",
        python_callable=call_collect_all,
        op_kwargs={"timeout": 60},
    )

    t_trigger_elt = TriggerDagRunOperator(
        task_id="trigger_weather_elt",
        trigger_dag_id="weather_elt",
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=30,
    )

    t_collect_weather >> t_trigger_elt

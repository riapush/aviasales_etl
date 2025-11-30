from __future__ import annotations
import os
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pymongo import MongoClient
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_USER = os.environ.get("MONGO_INITDB_ROOT_USERNAME") or os.environ.get(
    "MONGO_USER"
)
MONGO_PASSWORD = os.environ.get("MONGO_INITDB_ROOT_PASSWORD") or os.environ.get(
    "MONGO_PASSWORD"
)

PG_HOST = os.environ.get("POSTGRES_ANALYTICS_HOST", "postgres_analytics")
PG_DB = os.environ.get("POSTGRES_ANALYTICS_DB", "analytics")
PG_USER = os.environ.get("POSTGRES_ANALYTICS_USER", "pguser")
PG_PASSWORD = os.environ.get("POSTGRES_ANALYTICS_PASSWORD", "pgpass")
PG_PORT = int(os.environ.get("POSTGRES_ANALYTICS_PORT", 5432))


def _make_mongo_client():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


def _make_pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
    )
    conn.autocommit = True
    return conn


def extract_recent_from_mongo(window_hours: int = 3, **context):
    client = _make_mongo_client()
    col = client["aviation"]["prices"]
    cutoff = datetime.utcnow() - timedelta(hours=window_hours)
    cursor = col.find({"collected_ts": {"$gte": cutoff}})
    rows = []
    for d in cursor:
        rows.append(
            {
                "id": d["_id"],
                "route": d.get("route"),
                "carrier": d.get("carrier"),
                "departure": d.get("departure"),
                "collected_ts": d.get("collected_ts"),
                "days_before": d.get("days_before"),
                "price": d.get("price"),
            }
        )
    log.info("Extracted %s rows from Mongo (since %s)", len(rows), cutoff.isoformat())
    return rows


def load_into_postgres(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="extract_recent")
    if not rows:
        log.info("No rows to load")
        return {"inserted": 0}

    conn = _make_pg_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE IF NOT EXISTS analytics.tickets (
            id TEXT PRIMARY KEY,
            route TEXT,
            carrier TEXT,
            departure TIMESTAMPTZ,
            collected_ts TIMESTAMPTZ,
            days_before INTEGER,
            price NUMERIC
        );
    """
    )

    vals = [
        (
            r["id"],
            r["route"],
            r["carrier"],
            r["departure"],
            r["collected_ts"],
            r["days_before"],
            r["price"],
        )
        for r in rows
    ]

    insert_sql = """
        INSERT INTO analytics.tickets (id, route, carrier, departure, collected_ts, days_before, price)
        VALUES %s
        ON CONFLICT (id) DO NOTHING;
    """
    try:
        psycopg2.extras.execute_values(
            cur, insert_sql, vals, template=None, page_size=100
        )
        log.info("Loaded %s rows into Postgres (attempted)", len(vals))
    finally:
        cur.close()
        conn.close()
    return {"inserted": len(vals)}


def verify(**context):
    conn = _make_pg_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM analytics.tickets WHERE collected_ts >= (now() - INTERVAL '24 hours');"
    )
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    log.info("Verify: rows in analytics.tickets (last 24h): %s", count)
    return {"rows_last_24h": count}


with DAG(
    dag_id="mongo_to_postgres_elt",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["etl", "mongo", "postgres", "mock"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_recent",
        python_callable=extract_recent_from_mongo,
        op_kwargs={"window_hours": 3},
    )

    t_load = PythonOperator(
        task_id="load_into_postgres",
        python_callable=load_into_postgres,
    )

    t_verify = PythonOperator(
        task_id="verify_load",
        python_callable=verify,
    )

    t_extract >> t_load >> t_verify

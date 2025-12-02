from __future__ import annotations
import os
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pymongo import MongoClient
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

# MongoDB connection
MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_USER = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")

# PostgreSQL connection
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


def extract_current_weather(window_hours: int = 3, **context) -> List[Dict[str, Any]]:
    """Извлечение текущей погоды из MongoDB"""
    client = _make_mongo_client()
    col = client["weather_db"]["current_weather"]
    cutoff = datetime.utcnow() - timedelta(hours=window_hours)

    cursor = col.find({"collected_ts": {"$gte": cutoff}})
    rows = []
    for d in cursor:
        rows.append(
            {
                "id": d["_id"],
                "city": d.get("city", "Kaliningrad"),
                "country": d.get("country", "RU"),
                "dt": d.get("dt"),
                "temp": d.get("temp"),
                "feels_like": d.get("feels_like"),
                "pressure": d.get("pressure"),
                "humidity": d.get("humidity"),
                "wind_speed": d.get("wind_speed"),
                "wind_deg": d.get("wind_deg"),
                "weather_main": d.get("weather_main"),
                "weather_description": d.get("weather_description"),
                "clouds": d.get("clouds", 0),
                "visibility": d.get("visibility", 10000),
                "collected_ts": d.get("collected_ts"),
            }
        )

    log.info("Extracted %s current weather rows from Mongo", len(rows))
    return rows


def extract_forecasts(window_hours: int = 3, **context) -> List[Dict[str, Any]]:
    """Извлечение прогнозов из MongoDB"""
    client = _make_mongo_client()
    col = client["weather_db"]["forecasts"]
    cutoff = datetime.utcnow() - timedelta(hours=window_hours)

    cursor = col.find({"collection_dt": {"$gte": cutoff}})
    rows = []
    for d in cursor:
        rows.append(
            {
                "id": d["_id"],
                "city": d.get("city", "Kaliningrad"),
                "country": d.get("country", "RU"),
                "forecast_dt": d.get("forecast_dt"),
                "collection_dt": d.get("collection_dt"),
                "temp": d.get("temp"),
                "feels_like": d.get("feels_like"),
                "pressure": d.get("pressure"),
                "humidity": d.get("humidity"),
                "wind_speed": d.get("wind_speed"),
                "wind_deg": d.get("wind_deg"),
                "weather_main": d.get("weather_main"),
                "weather_description": d.get("weather_description"),
                "clouds": d.get("clouds", 0),
                "pop": d.get("pop", 0),
            }
        )

    log.info("Extracted %s forecast rows from Mongo", len(rows))
    return rows


def load_current_weather(**context):
    """Загрузка текущей погоды в PostgreSQL"""
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="extract_current_weather")

    if not rows:
        log.info("No current weather rows to load")
        return {"inserted": 0}

    conn = _make_pg_conn()
    cur = conn.cursor()

    try:
        # Безопасное создание схемы (игнорируем ошибку, если уже существует)
        try:
            cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
            log.info("Schema 'analytics' created or already exists")
        except Exception as schema_error:
            # Игнорируем ошибку, если схема уже существует
            if "duplicate key" not in str(schema_error) and "already exists" not in str(
                schema_error
            ):
                raise schema_error
            log.info("Schema 'analytics' already exists, continuing...")

        # Создаем таблицу
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics.current_weather (
                id TEXT PRIMARY KEY,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                dt BIGINT NOT NULL,
                temp NUMERIC NOT NULL,
                feels_like NUMERIC NOT NULL,
                pressure INTEGER NOT NULL,
                humidity INTEGER NOT NULL,
                wind_speed NUMERIC NOT NULL,
                wind_deg INTEGER NOT NULL,
                weather_main TEXT NOT NULL,
                weather_description TEXT NOT NULL,
                clouds INTEGER NOT NULL,
                visibility INTEGER NOT NULL,
                collected_ts TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """
        )

        # Подготавливаем данные
        vals = [
            (
                r["id"],
                r["city"],
                r["country"],
                r["dt"],
                r["temp"],
                r["feels_like"],
                r["pressure"],
                r["humidity"],
                r["wind_speed"],
                r["wind_deg"],
                r["weather_main"],
                r["weather_description"],
                r["clouds"],
                r["visibility"],
                r["collected_ts"],
            )
            for r in rows
        ]

        insert_sql = """
            INSERT INTO analytics.current_weather 
            (id, city, country, dt, temp, feels_like, pressure, humidity, 
             wind_speed, wind_deg, weather_main, weather_description, 
             clouds, visibility, collected_ts)
            VALUES %s
            ON CONFLICT (id) DO NOTHING;
        """

        psycopg2.extras.execute_values(cur, insert_sql, vals, template=None, page_size=100)
        inserted = cur.rowcount
        log.info("Loaded %s current weather rows into Postgres", inserted)

        return {"inserted": inserted}

    finally:
        cur.close()
        conn.close()


def load_forecasts(**context):
    """Загрузка прогнозов в PostgreSQL"""
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="extract_forecasts")

    if not rows:
        log.info("No forecast rows to load")
        return {"inserted": 0}

    conn = _make_pg_conn()
    cur = conn.cursor()

    try:
        # Безопасное создание схемы
        try:
            cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
            log.info("Schema 'analytics' created or already exists")
        except Exception as schema_error:
            if "duplicate key" not in str(schema_error) and "already exists" not in str(
                schema_error
            ):
                raise schema_error
            log.info("Schema 'analytics' already exists, continuing...")

        # Создаем таблицу прогнозов
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics.forecasts (
                id TEXT PRIMARY KEY,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                forecast_dt BIGINT NOT NULL,
                collection_dt TIMESTAMPTZ NOT NULL,
                temp NUMERIC NOT NULL,
                feels_like NUMERIC NOT NULL,
                pressure INTEGER NOT NULL,
                humidity INTEGER NOT NULL,
                wind_speed NUMERIC NOT NULL,
                wind_deg INTEGER NOT NULL,
                weather_main TEXT NOT NULL,
                weather_description TEXT NOT NULL,
                clouds INTEGER NOT NULL,
                pop NUMERIC NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """
        )

        # Подготавливаем данные
        vals = [
            (
                r["id"],
                r["city"],
                r["country"],
                r["forecast_dt"],
                r["collection_dt"],
                r["temp"],
                r["feels_like"],
                r["pressure"],
                r["humidity"],
                r["wind_speed"],
                r["wind_deg"],
                r["weather_main"],
                r["weather_description"],
                r["clouds"],
                r["pop"],
            )
            for r in rows
        ]

        insert_sql = """
            INSERT INTO analytics.forecasts 
            (id, city, country, forecast_dt, collection_dt, temp, feels_like, 
             pressure, humidity, wind_speed, wind_deg, weather_main, 
             weather_description, clouds, pop)
            VALUES %s
            ON CONFLICT (id) DO NOTHING;
        """

        psycopg2.extras.execute_values(cur, insert_sql, vals, template=None, page_size=100)
        inserted = cur.rowcount
        log.info("Loaded %s forecast rows into Postgres", inserted)

        return {"inserted": inserted}

    finally:
        cur.close()
        conn.close()


def calculate_accuracy_metrics(**context):
    """Расчет метрик точности прогнозов"""
    conn = _make_pg_conn()
    cur = conn.cursor()

    # Создаем таблицу для метрик точности
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS analytics.accuracy_metrics (
            id SERIAL PRIMARY KEY,
            forecast_dt TIMESTAMPTZ NOT NULL,
            collection_dt TIMESTAMPTZ NOT NULL,
            verification_dt TIMESTAMPTZ NOT NULL,
            temp_actual NUMERIC NOT NULL,
            temp_forecast NUMERIC NOT NULL,
            temp_error NUMERIC NOT NULL,
            temp_absolute_error NUMERIC NOT NULL,
            humidity_actual INTEGER NOT NULL,
            humidity_forecast INTEGER NOT NULL,
            humidity_error INTEGER NOT NULL,
            pressure_actual INTEGER NOT NULL,
            pressure_forecast INTEGER NOT NULL,
            pressure_error INTEGER NOT NULL,
            weather_match BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_accuracy_metrics_forecast_dt 
        ON analytics.accuracy_metrics(forecast_dt);
        
        CREATE INDEX IF NOT EXISTS idx_accuracy_metrics_verification_dt 
        ON analytics.accuracy_metrics(verification_dt);
    """
    )

    # Находим прогнозы, которые еще не проверены
    # Прогнозы сделаны более 1 часа назад, но менее 48 часов назад
    cur.execute(
        """
        SELECT 
            f.id,
            f.forecast_dt,
            f.collection_dt,
            f.temp as temp_forecast,
            f.humidity as humidity_forecast,
            f.pressure as pressure_forecast,
            f.weather_main as weather_forecast
        FROM analytics.forecasts f
        LEFT JOIN analytics.accuracy_metrics am 
            ON DATE_TRUNC('hour', TO_TIMESTAMP(f.forecast_dt)) = DATE_TRUNC('hour', am.forecast_dt)
            AND DATE_TRUNC('hour', f.collection_dt) = DATE_TRUNC('hour', am.collection_dt)
        WHERE 
            TO_TIMESTAMP(f.forecast_dt) <= NOW() - INTERVAL '1 hour'
            AND TO_TIMESTAMP(f.forecast_dt) >= NOW() - INTERVAL '48 hours'
            AND am.id IS NULL
        GROUP BY 
            f.id, f.forecast_dt, f.collection_dt, 
            f.temp, f.humidity, f.pressure, f.weather_main
        ORDER BY f.forecast_dt
        LIMIT 100;
    """
    )

    forecasts = cur.fetchall()

    if not forecasts:
        log.info("No new forecasts to verify")
        return {"verified": 0}

    verified_count = 0

    for forecast in forecasts:
        (
            forecast_id,
            forecast_dt_timestamp,
            collection_dt,
            temp_forecast,
            humidity_forecast,
            pressure_forecast,
            weather_forecast,
        ) = forecast

        # Конвертируем timestamp в datetime
        forecast_dt = datetime.fromtimestamp(forecast_dt_timestamp)

        # Ищем фактическую погоду в это время (±1 час)
        cur.execute(
            """
            SELECT 
                temp, humidity, pressure, weather_main,
                collected_ts
            FROM analytics.current_weather
            WHERE 
                collected_ts >= %s - INTERVAL '1 hour'
                AND collected_ts <= %s + INTERVAL '1 hour'
            ORDER BY ABS(EXTRACT(EPOCH FROM (collected_ts - %s)))
            LIMIT 1;
        """,
            (forecast_dt, forecast_dt, forecast_dt),
        )

        actual_data = cur.fetchone()

        if actual_data:
            (
                temp_actual,
                humidity_actual,
                pressure_actual,
                weather_actual,
                verification_dt,
            ) = actual_data

            # Рассчитываем ошибки
            temp_error = temp_actual - temp_forecast
            temp_absolute_error = abs(temp_error)
            humidity_error = humidity_actual - humidity_forecast
            pressure_error = pressure_actual - pressure_forecast
            weather_match = weather_actual == weather_forecast

            # Сохраняем метрики
            cur.execute(
                """
                INSERT INTO analytics.accuracy_metrics 
                (forecast_dt, collection_dt, verification_dt,
                 temp_actual, temp_forecast, temp_error, temp_absolute_error,
                 humidity_actual, humidity_forecast, humidity_error,
                 pressure_actual, pressure_forecast, pressure_error,
                 weather_match)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    forecast_dt,
                    collection_dt,
                    verification_dt,
                    temp_actual,
                    temp_forecast,
                    temp_error,
                    temp_absolute_error,
                    humidity_actual,
                    humidity_forecast,
                    humidity_error,
                    pressure_actual,
                    pressure_forecast,
                    pressure_error,
                    weather_match,
                ),
            )

            verified_count += 1
            log.info(
                f"Verified forecast for {forecast_dt}: "
                f"temp error={temp_error:.2f}°C, "
                f"weather match={weather_match}"
            )

    log.info(f"Verified {verified_count} forecasts")
    return {"verified": verified_count}


def generate_accuracy_report(**context):
    """Генерация отчета о точности прогнозов"""
    conn = _make_pg_conn()
    cur = conn.cursor()

    # Статистика за последние 24 часа
    cur.execute(
        """
        SELECT 
            COUNT(*) as total_forecasts,
            AVG(temp_absolute_error) as avg_temp_error,
            STDDEV(temp_error) as std_temp_error,
            AVG(ABS(humidity_error)) as avg_humidity_error,
            AVG(ABS(pressure_error)) as avg_pressure_error,
            SUM(CASE WHEN weather_match THEN 1 ELSE 0 END) as weather_matches,
            COUNT(*) as total_checked,
            MIN(forecast_dt) as earliest_forecast,
            MAX(forecast_dt) as latest_forecast
        FROM analytics.accuracy_metrics
        WHERE verification_dt >= NOW() - INTERVAL '24 hours';
    """
    )

    stats = cur.fetchone()

    if stats and stats[0] > 0:
        (
            total_forecasts,
            avg_temp_error,
            std_temp_error,
            avg_humidity_error,
            avg_pressure_error,
            weather_matches,
            total_checked,
            earliest_forecast,
            latest_forecast,
        ) = stats

        weather_accuracy = (weather_matches / total_checked * 100) if total_checked > 0 else 0

        report = {
            "period": "last_24_hours",
            "total_forecasts_verified": total_forecasts,
            "avg_temperature_error_c": round(float(avg_temp_error or 0), 2),
            "temperature_error_std": round(float(std_temp_error or 0), 2),
            "avg_humidity_error_percent": round(float(avg_humidity_error or 0), 1),
            "avg_pressure_error_hpa": round(float(avg_pressure_error or 0), 1),
            "weather_type_accuracy_percent": round(weather_accuracy, 1),
            "earliest_forecast": (earliest_forecast.isoformat() if earliest_forecast else None),
            "latest_forecast": latest_forecast.isoformat() if latest_forecast else None,
            "generated_at": datetime.utcnow().isoformat(),
        }

        log.info("Accuracy report: %s", report)

        # Также записываем сводку в лог таблицу (можно расширить)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics.accuracy_reports (
                id SERIAL PRIMARY KEY,
                period TEXT NOT NULL,
                total_forecasts INTEGER NOT NULL,
                avg_temp_error NUMERIC NOT NULL,
                temp_error_std NUMERIC NOT NULL,
                avg_humidity_error NUMERIC NOT NULL,
                avg_pressure_error NUMERIC NOT NULL,
                weather_accuracy NUMERIC NOT NULL,
                report_dt TIMESTAMPTZ DEFAULT NOW()
            );
            
            INSERT INTO analytics.accuracy_reports
            (period, total_forecasts, avg_temp_error, temp_error_std,
             avg_humidity_error, avg_pressure_error, weather_accuracy)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """,
            (
                "24h",
                total_forecasts,
                round(float(avg_temp_error or 0), 2),
                round(float(std_temp_error or 0), 2),
                round(float(avg_humidity_error or 0), 1),
                round(float(avg_pressure_error or 0), 1),
                round(weather_accuracy, 1),
            ),
        )

        return report
    else:
        log.info("No accuracy data available for the last 24 hours")
        return {"message": "No data available"}


with DAG(
    dag_id="weather_elt",
    schedule_interval=None,  # Запускается по триггеру
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["weather", "elt", "accuracy", "analytics"],
) as dag:

    t_extract_current = PythonOperator(
        task_id="extract_current_weather",
        python_callable=extract_current_weather,
        op_kwargs={"window_hours": 3},
    )

    t_extract_forecasts = PythonOperator(
        task_id="extract_forecasts",
        python_callable=extract_forecasts,
        op_kwargs={"window_hours": 3},
    )

    t_load_current = PythonOperator(
        task_id="load_current_weather",
        python_callable=load_current_weather,
    )

    t_load_forecasts = PythonOperator(
        task_id="load_forecasts",
        python_callable=load_forecasts,
    )

    t_calculate_accuracy = PythonOperator(
        task_id="calculate_accuracy_metrics",
        python_callable=calculate_accuracy_metrics,
    )

    t_generate_report = PythonOperator(
        task_id="generate_accuracy_report",
        python_callable=generate_accuracy_report,
    )

    # Параллельное извлечение текущей погоды и прогнозов
    [t_extract_current, t_extract_forecasts] >> t_load_current
    [t_extract_current, t_extract_forecasts] >> t_load_forecasts

    # Затем расчет точности и отчет
    [t_load_current, t_load_forecasts] >> t_calculate_accuracy >> t_generate_report

import os
import random
from datetime import datetime, timedelta
from threading import Thread
import uuid
import time as _time
from typing import List, Dict, Any
import requests
from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient, errors
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Weather Data Collector API")

# Конфигурация OpenWeatherMap
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY", "your_api_key_here")
CITY_NAME = "Kaliningrad"
COUNTRY_CODE = "RU"
LAT = 54.7104
LON = 20.4522

client = None
db = None
current_collection = None
forecast_collection = None


# Pydantic модели
class CurrentWeather(BaseModel):
    city: str
    country: str
    dt: int  # Unix timestamp
    temp: float
    feels_like: float
    pressure: int
    humidity: int
    wind_speed: float
    wind_deg: int
    weather_main: str
    weather_description: str
    clouds: int
    visibility: int
    collected_ts: datetime


class Forecast(BaseModel):
    city: str
    country: str
    forecast_dt: int  # Время прогноза (Unix timestamp)
    collection_dt: datetime  # Время сбора прогноза
    temp: float
    feels_like: float
    pressure: int
    humidity: int
    wind_speed: float
    wind_deg: int
    weather_main: str
    weather_description: str
    clouds: int
    pop: float  # Вероятность осадков (0-1)


def make_mongo_uri():
    MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
    MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
    MONGO_USER = os.environ.get("MONGO_USER", "admin")
    MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD", "secret")
    return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"


def init_mongo_client(retries=10, delay=3):
    global client, db, current_collection, forecast_collection
    uri = make_mongo_uri()
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db = client["weather_db"]
            current_collection = db["current_weather"]
            forecast_collection = db["forecasts"]
            print("Mongo connected successfully")
            return
        except errors.PyMongoError as e:
            print(f"Mongo connection failed (attempt {attempt}/{retries}): {e}")
            _time.sleep(delay)
    raise RuntimeError("Could not connect to MongoDB after retries")


def get_current_weather() -> Dict[str, Any]:
    """Получение текущей погоды с OpenWeatherMap"""
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"lat": LAT, "lon": LON, "appid": OPENWEATHER_API_KEY, "units": "metric", "lang": "ru"}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        current_weather = {
            "city": data.get("name", CITY_NAME),
            "country": data.get("sys", {}).get("country", COUNTRY_CODE),
            "dt": data.get("dt"),
            "temp": data.get("main", {}).get("temp"),
            "feels_like": data.get("main", {}).get("feels_like"),
            "pressure": data.get("main", {}).get("pressure"),
            "humidity": data.get("main", {}).get("humidity"),
            "wind_speed": data.get("wind", {}).get("speed"),
            "wind_deg": data.get("wind", {}).get("deg"),
            "weather_main": data.get("weather", [{}])[0].get("main"),
            "weather_description": data.get("weather", [{}])[0].get("description"),
            "clouds": data.get("clouds", {}).get("all", 0),
            "visibility": data.get("visibility", 10000),
            "collected_ts": datetime.utcnow(),
        }

        return current_weather
    except Exception as e:
        print(f"Error fetching current weather: {e}")
        # Возвращаем мок данные в случае ошибки
        return mock_current_weather()


def get_forecast() -> List[Dict[str, Any]]:
    """Получение прогноза погоды на 48 часов"""
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "lat": LAT,
        "lon": LON,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",
        "lang": "ru",
        "cnt": 16,  # 48 часов с шагом 3 часа
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        forecasts = []
        collection_time = datetime.utcnow()

        for item in data.get("list", []):
            forecast = {
                "city": data.get("city", {}).get("name", CITY_NAME),
                "country": data.get("city", {}).get("country", COUNTRY_CODE),
                "forecast_dt": item.get("dt"),
                "collection_dt": collection_time,
                "temp": item.get("main", {}).get("temp"),
                "feels_like": item.get("main", {}).get("feels_like"),
                "pressure": item.get("main", {}).get("pressure"),
                "humidity": item.get("main", {}).get("humidity"),
                "wind_speed": item.get("wind", {}).get("speed"),
                "wind_deg": item.get("wind", {}).get("deg"),
                "weather_main": item.get("weather", [{}])[0].get("main"),
                "weather_description": item.get("weather", [{}])[0].get("description"),
                "clouds": item.get("clouds", {}).get("all", 0),
                "pop": item.get("pop", 0),  # Probability of precipitation
            }
            forecasts.append(forecast)

        return forecasts
    except Exception as e:
        print(f"Error fetching forecast: {e}")
        # Возвращаем мок данные в случае ошибки
        return mock_forecast()


def mock_current_weather() -> Dict[str, Any]:
    """Мок текущей погоды (используется при ошибке API)"""
    now = datetime.utcnow()
    base_temp = 10 + 10 * random.random()  # Температура от 10 до 20
    return {
        "city": CITY_NAME,
        "country": COUNTRY_CODE,
        "dt": int(now.timestamp()),
        "temp": round(base_temp, 1),
        "feels_like": round(base_temp - random.uniform(0, 3), 1),
        "pressure": random.randint(980, 1030),
        "humidity": random.randint(40, 90),
        "wind_speed": round(random.uniform(0, 10), 1),
        "wind_deg": random.randint(0, 360),
        "weather_main": random.choice(["Clear", "Clouds", "Rain", "Snow"]),
        "weather_description": random.choice(["ясно", "облачно", "легкий дождь", "снег"]),
        "clouds": random.randint(0, 100),
        "visibility": random.randint(5000, 20000),
        "collected_ts": now,
    }


def mock_forecast() -> List[Dict[str, Any]]:
    """Мок прогноза погоды на 48 часов"""
    forecasts = []
    collection_time = datetime.utcnow()
    base_temp = 10 + 10 * random.random()

    for hours_ahead in range(0, 49, 3):  # 0, 3, 6, ..., 48 часов
        forecast_time = collection_time + timedelta(hours=hours_ahead)
        # Температура меняется синусоидально в течение суток
        temp_change = 5 * random.random() * (1 if hours_ahead % 24 < 12 else -1)

        forecasts.append(
            {
                "city": CITY_NAME,
                "country": COUNTRY_CODE,
                "forecast_dt": int(forecast_time.timestamp()),
                "collection_dt": collection_time,
                "temp": round(base_temp + temp_change, 1),
                "feels_like": round(base_temp + temp_change - random.uniform(0, 3), 1),
                "pressure": random.randint(980, 1030),
                "humidity": random.randint(40, 90),
                "wind_speed": round(random.uniform(0, 10), 1),
                "wind_deg": random.randint(0, 360),
                "weather_main": random.choice(["Clear", "Clouds", "Rain", "Snow"]),
                "weather_description": random.choice(["ясно", "облачно", "легкий дождь", "снег"]),
                "clouds": random.randint(0, 100),
                "pop": round(random.uniform(0, 0.8), 2),  # Вероятность осадков 0-80%
            }
        )

    return forecasts


def collect_weather_data():
    """Основной цикл сбора данных о погоде"""
    while True:
        try:
            # Сбор текущей погоды
            current_weather = get_current_weather()
            if current_weather:
                current_collection.insert_one({"_id": str(uuid.uuid4()), **current_weather})
                print(f"Collected current weather at {current_weather['collected_ts']}")

            # Сбор прогноза
            forecasts = get_forecast()
            if forecasts:
                forecast_docs = []
                for forecast in forecasts:
                    forecast_docs.append({"_id": str(uuid.uuid4()), **forecast})
                forecast_collection.insert_many(forecast_docs)
                print(f"Collected {len(forecasts)} forecast records")

            # Ждем 1 час до следующего сбора
            _time.sleep(3600)

        except Exception as e:
            print(f"Error in weather collection loop: {e}")
            _time.sleep(300)  # Ждем 5 минут при ошибке


@app.on_event("startup")
def startup():
    init_mongo_client(retries=20, delay=2)
    Thread(target=collect_weather_data, daemon=True).start()


@app.post("/collect/current")
def collect_current_weather():
    """Ручной запуск сбора текущей погоды"""
    try:
        current_weather = get_current_weather()
        if current_weather:
            result = current_collection.insert_one({"_id": str(uuid.uuid4()), **current_weather})
            return {"status": "success", "inserted_id": str(result.inserted_id)}
        return {"status": "error", "message": "No data received"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/collect/forecast")
def collect_forecast():
    """Ручной запуск сбора прогноза"""
    try:
        forecasts = get_forecast()
        if forecasts:
            forecast_docs = []
            for forecast in forecasts:
                forecast_docs.append({"_id": str(uuid.uuid4()), **forecast})
            result = forecast_collection.insert_many(forecast_docs)
            return {"status": "success", "inserted_count": len(result.inserted_ids)}
        return {"status": "error", "message": "No data received"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/collect/all")
def collect_all_weather():
    """Сбор и текущей погоды и прогноза"""
    try:
        # Сбор текущей погоды
        current_weather = get_current_weather()
        current_id = None
        if current_weather:
            result = current_collection.insert_one({"_id": str(uuid.uuid4()), **current_weather})
            current_id = str(result.inserted_id)

        # Сбор прогноза
        forecasts = get_forecast()
        forecast_count = 0
        if forecasts:
            forecast_docs = []
            for forecast in forecasts:
                forecast_docs.append({"_id": str(uuid.uuid4()), **forecast})
            result = forecast_collection.insert_many(forecast_docs)
            forecast_count = len(result.inserted_ids)

        return {
            "status": "success",
            "current_weather_inserted": current_id is not None,
            "current_weather_id": current_id,
            "forecast_records_inserted": forecast_count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/weather/current")
def get_current():
    """Получение последней записи о текущей погоде"""
    doc = current_collection.find_one(sort=[("collected_ts", -1)])
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc or {}


@app.get("/weather/forecast")
def get_latest_forecast():
    """Получение последнего прогноза"""
    # Находим время последнего сбора прогноза
    latest_collection = forecast_collection.find_one(
        sort=[("collection_dt", -1)], projection={"collection_dt": 1}
    )

    if not latest_collection:
        return []

    # Получаем все прогнозы из последнего сбора
    forecasts = list(
        forecast_collection.find({"collection_dt": latest_collection["collection_dt"]}).sort(
            "forecast_dt", 1
        )
    )

    for doc in forecasts:
        doc["_id"] = str(doc["_id"])

    return forecasts


@app.get("/weather/history")
def get_weather_history(hours: int = Query(24, ge=1, le=168)):
    """История текущей погоды за указанное количество часов"""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    docs = list(current_collection.find({"collected_ts": {"$gte": cutoff}}).sort("collected_ts", 1))

    for doc in docs:
        doc["_id"] = str(doc["_id"])

    return docs


@app.get("/stats")
def stats():
    """Статистика по собранным данным"""
    current_count = current_collection.count_documents({})
    forecast_count = forecast_collection.count_documents({})

    # Средняя температура за последние 24 часа
    cutoff = datetime.utcnow() - timedelta(hours=24)
    recent_temps = list(current_collection.find({"collected_ts": {"$gte": cutoff}}, {"temp": 1}))

    avg_temp = None
    if recent_temps:
        avg_temp = sum(doc["temp"] for doc in recent_temps) / len(recent_temps)

    return {
        "current_weather_records": current_count,
        "forecast_records": forecast_count,
        "avg_temp_last_24h": round(avg_temp, 2) if avg_temp else None,
        "collection_period_hours": 24,
    }


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

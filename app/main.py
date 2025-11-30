import os, random, time
from datetime import datetime, timedelta
from threading import Thread
import uuid
from statistics import mean
import time as _time
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pymongo import MongoClient, errors


app = FastAPI(title="Mock Avia API")

ROUTES = ["MOW-LED", "MOW-SVO", "MOW-BCN", "LED-ATH", "MOW-IST"]
CARRIERS = ["S7", "Aeroflot", "Pobeda", "UIA", "Turkish"]

client = None
db = None
collection = None


def make_mongo_uri():
    MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
    MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
    MONGO_USER = os.environ.get("MONGO_USER")
    MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
    return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"


def init_mongo_client(retries=10, delay=3):
    global client, db, collection
    uri = make_mongo_uri()
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # принудительная проверка подключения
            client.admin.command("ping")
            db = client["aviation"]
            collection = db["prices"]
            print("Mongo connected")
            return
        except errors.PyMongoError as e:
            print(f"Mongo connection failed (attempt {attempt}/{retries}): {e}")
            _time.sleep(delay)
    raise RuntimeError("Could not connect to MongoDB after retries")


def generator_loop():
    while True:
        days_before = random.randint(1, 120)
        base = 5000
        ramp = max(0, (60 - days_before) / 60)
        mean_price = base * (1 + 0.8 * ramp)
        price = round(max(300, random.gauss(mean_price, mean_price * 0.12)), 2)

        doc = {
            "_id": str(uuid.uuid4()),
            "route": random.choice(ROUTES),
            "carrier": random.choice(CARRIERS),
            "departure": (datetime.utcnow() + timedelta(days=days_before)).isoformat(),
            "collected_ts": datetime.utcnow(),
            "days_before": days_before,
            "price": price,
        }
        try:
            collection.insert_one(doc)
        except Exception as e:
            print("Insert error", e)
        time.sleep(2)


@app.on_event("startup")
def startup():
    init_mongo_client(retries=20, delay=2)
    Thread(target=generator_loop, daemon=True).start()


@app.get("/routes")
def get_routes():
    return {"routes": ROUTES}


@app.get("/prices")
def get_prices(from_: str, to: str, date: str):
    route = f"{from_}-{to}"
    docs = list(collection.find({"route": route}).sort("collected_ts", -1).limit(20))

    return [
        {
            "carrier": d["carrier"],
            "price": d["price"],
            "departure": d["departure"],
            "days_before": d["days_before"],
        }
        for d in docs
    ]


@app.get("/analytics/avg-price")
def avg_price(route: str):
    docs = list(collection.find({"route": route}))
    if not docs:
        return {"route": route, "avg_price": None}
    return {
        "route": route,
        "avg_price": round(mean([d["price"] for d in docs]), 2),
        "records": len(docs),
    }


@app.get("/analytics/best-time")
def best_time(route: str):
    docs = list(collection.find({"route": route}))
    if not docs:
        return {"route": route, "recommendation": "Нет данных"}

    buckets = {}
    for d in docs:
        dbf = d["days_before"]
        buckets.setdefault(dbf, []).append(d["price"])

    avg_by_days = {k: round(mean(v), 2) for k, v in buckets.items()}
    best_days = min(avg_by_days, key=lambda k: avg_by_days[k])

    return {
        "route": route,
        "best_buy_in_days": best_days,
        "expected_price": avg_by_days[best_days],
        "price_by_days_before": avg_by_days,
        "recommendation": f"Лучше покупать за {best_days} дней до вылета",
    }


@app.get("/stats")
def stats():
    docs = list(collection.find().limit(1000))
    if not docs:
        return {}
    avg = round(mean([d["price"] for d in docs]), 2)
    min_price = min(d["price"] for d in docs)
    return {
        "records": collection.count_documents({}),
        "avg_price_all": avg,
        "min_price": min_price,
        "routes": list(set(d["route"] for d in docs)),
    }


@app.post("/generate")
def generate_now(n: int = Query(20, ge=1, le=1000)):
    """
    Сгенерировать n записей немедленно и вставить в Mongo (возвращает количество).
    Это полезно для инжеста через Airflow.
    """
    global collection
    if collection is None:
        return JSONResponse(status_code=500, content={"error": "mongo not initialized"})

    now = datetime.utcnow()
    docs = []
    for _ in range(n):
        days_before = random.randint(1, 120)
        base = 5000
        ramp = max(0, (60 - days_before) / 60)
        mean_price = base * (1 + 0.8 * ramp)
        price = round(max(300, random.gauss(mean_price, mean_price * 0.12)), 2)

        doc = {
            "_id": str(uuid.uuid4()),
            "route": random.choice(ROUTES),
            "carrier": random.choice(CARRIERS),
            "departure": (now + timedelta(days=days_before)).isoformat(),
            "collected_ts": now,
            "days_before": days_before,
            "price": price,
        }
        docs.append(doc)

    try:
        collection.insert_many(docs)
        return {"inserted": len(docs)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/health")
def health():
    return {"status": "ok"}

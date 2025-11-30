# aviasales_api.py
import os
from datetime import datetime
import requests
from dotenv import load_dotenv
from fastapi import HTTPException
from loguru import logger
from pydantic import BaseModel
from pymongo import MongoClient

load_dotenv()

class FlightPriceRequest(BaseModel):
    origin: str  # IATA code like "LON"
    destination: str  # IATA code like "NYC"
    depart_date: str  # "2024-12-01"
    return_date: str = None  # Optional for one-way
    trip_class: int = 0  # 0-economy, 1-business

class FlightPriceData(BaseModel):
    origin: str
    destination: str
    depart_date: str
    price: float
    airline: str
    flight_number: str
    found_at: datetime
    trip_class: int
    number_of_changes: int  # stops
    distance: int
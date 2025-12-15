import requests
import json
from datetime import datetime
import os

BASE_DIR = "/opt/airflow"
RAW_DIR = f"{BASE_DIR}/data/raw"

# Multi-city configuration
CITIES = [
    {"city": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"city": "Delhi", "lat": 28.6139, "lon": 77.2090},
    {"city": "Bangalore", "lat": 12.9716, "lon": 77.5946},
    {"city": "Chennai", "lat": 13.0827, "lon": 80.2707},
    {"city": "Kolkata", "lat": 22.5726, "lon": 88.3639},
]

BASE_URL = "https://api.open-meteo.com/v1/forecast"

def extract_weather_data():
    os.makedirs(RAW_DIR, exist_ok=True)

    extracted_files = []

    for city_cfg in CITIES:
        params = {
            "latitude": city_cfg["lat"],
            "longitude": city_cfg["lon"],
            "current_weather": True
        }

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()
        weather = data["current_weather"]

        weather_record = {
            "city": city_cfg["city"],
            "latitude": city_cfg["lat"],
            "longitude": city_cfg["lon"],
            "temperature_celsius": weather["temperature"],
            "windspeed": weather["windspeed"],
            "winddirection": weather["winddirection"],
            "weathercode": weather["weathercode"],
            "timestamp_utc": weather["time"],
            "ingested_at_utc": datetime.utcnow().isoformat()
        }

        file_path = (
            f"{RAW_DIR}/weather_"
            f"{city_cfg['city'].lower()}_"
            f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        )

        with open(file_path, "w") as f:
            json.dump(weather_record, f, indent=4)

        extracted_files.append(file_path)
        print(f"Extracted weather data for {city_cfg['city']}")

    return extracted_files



if __name__ == "__main__":
    extract_weather_data()

import requests
import json
from datetime import datetime
import os

# CONFIG
CITY = "Mumbai"
LATITUDE = 19.0760
LONGITUDE = 72.8777
BASE_URL = "https://api.open-meteo.com/v1/forecast"


def extract_weather_data():

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "current_weather": True
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    weather = data["current_weather"]

    weather_record = {
        "city": CITY,
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "temperature_celsius": weather["temperature"],
        "windspeed": weather["windspeed"],
        "winddirection": weather["winddirection"],
        "weathercode": weather["weathercode"],
        "timestamp_utc": weather["time"],
        "ingested_at_utc": datetime.utcnow().isoformat()
    }

    os.makedirs("data/raw", exist_ok=True)

    file_path = f"data/raw/weather_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    with open(file_path, "w") as f:
        json.dump(weather_record, f, indent=4)

    print(f"Weather data extracted successfully: {file_path}")
    return file_path


if __name__ == "__main__":
    extract_weather_data()

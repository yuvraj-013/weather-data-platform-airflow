import json
import glob
import psycopg2
from datetime import datetime

RAW_DATA_PATH = "/opt/airflow/data/raw/*.json"

DB_CONFIG = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def load_weather_to_postgres():
    files = glob.glob(RAW_DATA_PATH)

    if not files:
        raise Exception("No raw weather files found")

    latest_file = sorted(files)[-1]

    with open(latest_file) as f:
        data = json.load(f)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO weather_fact (
            city, latitude, longitude, temperature_celsius,
            windspeed, winddirection, weathercode,
            timestamp_utc, ingested_at_utc
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor.execute(
        insert_sql,
        (
            data["city"],
            data["latitude"],
            data["longitude"],
            data["temperature_celsius"],
            data["windspeed"],
            data["winddirection"],
            data["weathercode"],
            datetime.fromisoformat(data["timestamp_utc"]),
            datetime.fromisoformat(data["ingested_at_utc"])
        )
    )

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Data loaded into Postgres successfully")

if __name__ == "__main__":
    load_weather_to_postgres()

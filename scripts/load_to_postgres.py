import os
import json
import glob
import psycopg2
from datetime import datetime

RAW_DATA_PATH = "/opt/airflow/data/raw/*.json"
PROCESSED_DIR = "/opt/airflow/data/processed"


DB_CONFIG = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def load_weather_to_postgres():
    files = glob.glob(RAW_DATA_PATH)

    if not files:
        print("No new raw files to process")
        return

    os.makedirs(PROCESSED_DIR, exist_ok=True)

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

    for file_path in files:
        with open(file_path) as f:
            data = json.load(f)

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

        # Move file to processed folder
        file_name = os.path.basename(file_path)
        processed_path = os.path.join(PROCESSED_DIR, file_name)
        os.rename(file_path, processed_path)

        print(f"Processed and archived file: {file_name}")

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ All new files loaded and archived")

if __name__ == "__main__":
    load_weather_to_postgres()

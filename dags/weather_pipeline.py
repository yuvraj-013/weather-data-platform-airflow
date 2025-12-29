from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="End-to-end Weather ETL with Data Quality",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id="extract_weather",
        bash_command="python /opt/airflow/scripts/extract_weather.py",
    )

    validate = BashOperator(
        task_id="validate_weather",
        bash_command="python /opt/airflow/scripts/validate_weather.py",
    )

    load = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /opt/airflow/scripts/load_to_postgres.py",
    )

    extract >> validate >> load

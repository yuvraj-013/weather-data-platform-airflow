import json
import glob
import pandas as pd
import great_expectations as ge

RAW_DATA_PATH = "/opt/airflow/data/raw/*.json"

def validate_weather_data():
    files = glob.glob(RAW_DATA_PATH)

    if not files:
        raise Exception("No raw weather files found")

    latest_file = sorted(files)[-1]

    with open(latest_file) as f:
        data = json.load(f)

    # Create pandas DataFrame
    df = pd.DataFrame([data])

    # Convert to Great Expectations dataframe
    ge_df = ge.from_pandas(df)

    # EXPECTATIONS
    ge_df.expect_column_values_to_not_be_null("city")
    ge_df.expect_column_values_to_be_between(
        "temperature_celsius", -50, 60
    )
    ge_df.expect_column_values_to_be_between(
        "windspeed", 0, 150
    )
    ge_df.expect_column_values_to_not_be_null("weathercode")
    ge_df.expect_column_values_to_not_be_null("timestamp_utc")

    result = ge_df.validate()

    if not result["success"]:
        raise Exception("❌ Data quality validation FAILED")

    print("✅ Data quality validation PASSED")

if __name__ == "__main__":
    validate_weather_data()

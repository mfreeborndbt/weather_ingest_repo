import pandas as pd
import requests
import boto3
import io
from datetime import datetime

# Reference points (8 locations across NYC boroughs)
weather_points = [
    {"location_id": "MHT_1", "borough": "Manhattan", "lat": 40.7831, "lon": -73.9712},
    {"location_id": "BK_1",  "borough": "Brooklyn",  "lat": 40.6782, "lon": -73.9442},
    {"location_id": "QN_1",  "borough": "Queens",    "lat": 40.7421, "lon": -73.7694},
    {"location_id": "BX_1",  "borough": "Bronx",     "lat": 40.8448, "lon": -73.8648},
    {"location_id": "MHT_2", "borough": "Manhattan", "lat": 40.73061, "lon": -73.935242},
    {"location_id": "BK_2",  "borough": "Brooklyn",  "lat": 40.6400, "lon": -73.9496},
    {"location_id": "QN_2",  "borough": "Queens",    "lat": 40.7048, "lon": -73.7950},
    {"location_id": "BX_2",  "borough": "Bronx",     "lat": 40.8265, "lon": -73.9165}
]

# Get current weather data for each point
all_data = []

for point in weather_points:
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={point['lat']}&longitude={point['lon']}&current_weather=true"
    )
    response = requests.get(url)
    response.raise_for_status()  # Raise exception if request failed
    data = response.json()["current_weather"]

    row = {
        "timestamp_utc": data["time"],
        "temperature": data["temperature"],
        "windspeed": data["windspeed"],
        "winddirection": data["winddirection"],
        "weather_code": data["weathercode"],
        "is_day": data["is_day"],
        "location_id": point["location_id"],
        "borough": point["borough"],
        "latitude": point["lat"],
        "longitude": point["lon"]
    }
    all_data.append(row)

# Convert to DataFrame
df_all = pd.DataFrame(all_data)

# Define S3 location
s3 = boto3.client("s3")
bucket_name = "sales-sandbox-databricks-user-mfreeborn"
object_key = "nyc_weather.csv"

# Try to get existing file, otherwise create new
try:
    obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    existing_df = pd.read_csv(obj["Body"])
    combined_df = pd.concat([existing_df, df_all], ignore_index=True)
except s3.exceptions.NoSuchKey:
    combined_df = df_all  # First run – create new file

# Write updated DataFrame back to S3
csv_buffer = io.StringIO()
combined_df.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=bucket_name,
    Key=object_key,
    Body=csv_buffer.getvalue()
)

print(f"✅ Weather data appended to s3://{bucket_name}/{object_key}")

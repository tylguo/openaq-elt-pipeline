import os
import json
import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from time import sleep

load_dotenv()
api_key = os.getenv("OPENAQ_API_KEY")
gcp_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key_path

client = bigquery.Client()
dataset_id = "openaq_raw"
table_id = "locations"

table_ref = f"{client.project}.{dataset_id}.{table_id}"

# Flatten location JSON
def flatten_location(location):
    sensors = [s["parameter"]["name"] for s in location.get("sensors", [])]
    instruments = [i["name"] for i in location.get("instruments", [])]
    return {
        "id": location.get("id"),
        "name": location.get("name"),
        "country": location.get("country", {}).get("code"),
        "latitude": location.get("coordinates", {}).get("latitude"),
        "longitude": location.get("coordinates", {}).get("longitude"),
        "sensors": ", ".join(sensors),
        "instruments": ", ".join(instruments),
        "timezone": location.get("timezone"),
        "is_mobile": location.get("isMobile"),
        "is_monitor": location.get("isMonitor"),
    }

# Fetch locations from OpenAQ
url = "https://api.openaq.org/v3/locations"
limit = 100
page = 1
all_locations = []

while True:
    params = {"limit": limit, "page": page}
    headers = {"X-API-Key": api_key}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break
    
    data = response.json()
    results = data.get("results", [])
    if not results:
        break
    
    for loc in results:
        all_locations.append(flatten_location(loc))

    print(f"Fetched page {page}, {len(results)} locations")
    page += 1
    sleep(1)

# Insert data in BigQuery
if all_locations:
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_json(all_locations, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(all_locations)} locations into {table_ref}")
else:
    print("No locations fetched.")
import os
from datetime import datetime, timedelta, timezone
from time import sleep

import requests
from dotenv import load_dotenv
from google.cloud import bigquery


# ------------------------------
# Config
# ------------------------------
DAYS_BACK = 7              # how many days of history (you can increase later)
PAGE_SIZE_LOCATIONS = 100  # OpenAQ default; can go up to 1000
PAGE_SIZE_HOURS = 100      # results per page for /hours
MAX_SENSORS = 500          # safety cap for first version so it doesn't explode
API_BASE_URL = "https://api.openaq.org/v3"


# ------------------------------
# Load environment variables
# ------------------------------
load_dotenv()

api_key = os.getenv("OPENAQ_API_KEY")
gcp_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not api_key:
    raise ValueError("OPENAQ_API_KEY not set in .env")
if not gcp_key_path:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not set in .env")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key_path


# ------------------------------
# BigQuery setup
# ------------------------------
client = bigquery.Client()
dataset_id = "openaq_raw"
table_id = "measurements_hours_p"
table_ref = f"{client.project}.{dataset_id}.{table_id}"


# ------------------------------
# Helpers
# ------------------------------
def isoformat_utc(dt: datetime) -> str:
    """Return ISO-8601 UTC string compatible with OpenAQ."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_locations_with_sensors():
    """
    Fetch locations from /v3/locations and extract sensor context.
    Returns a list of sensor dicts with location metadata attached.
    """
    sensors = []
    page = 1

    while True:
        params = {"limit": PAGE_SIZE_LOCATIONS, "page": page}
        headers = {"X-API-Key": api_key}

        resp = requests.get(f"{API_BASE_URL}/locations", headers=headers, params=params)
        if resp.status_code != 200:
            print(f"Error fetching locations page {page}: {resp.status_code} - {resp.text}")
            break

        data = resp.json()
        results = data.get("results", [])
        if not results:
            print("No more locations returned, stopping.")
            break

        for loc in results:
            loc_id = loc.get("id")
            country = (loc.get("country") or {}).get("code")
            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            for sensor in loc.get("sensors", []):
                sensor_id = sensor.get("id")
                param = sensor.get("parameter") or {}
                sensors.append(
                    {
                        "sensor_id": sensor_id,
                        "location_id": loc_id,
                        "country": country,
                        "latitude": lat,
                        "longitude": lon,
                        "parameter_id": param.get("id"),
                        "parameter_name": param.get("name"),
                        "units": param.get("units"),
                    }
                )

        print(f"Fetched locations page {page} with {len(results)} locations.")
        page += 1
        sleep(1)  # be polite

        if len(sensors) >= MAX_SENSORS:
            print(f"Reached MAX_SENSORS={MAX_SENSORS}, stopping sensor discovery.")
            break

    print(f"Total sensors discovered (capped): {len(sensors)}")
    return sensors


def flatten_hour_record(record: dict, sensor_ctx: dict) -> dict:
    """
    Flatten one hourly record from /v3/sensors/{id}/hours.
    """
    period = record.get("period") or {}
    dt_to = (period.get("datetimeTo") or {}).get("utc")

    coords = record.get("coordinates") or {}
    param = record.get("parameter") or {}

    return {
        "sensor_id": sensor_ctx["sensor_id"],
        "location_id": sensor_ctx["location_id"],
        "country": sensor_ctx["country"],
        "latitude": coords.get("latitude", sensor_ctx["latitude"]),
        "longitude": coords.get("longitude", sensor_ctx["longitude"]),
        "parameter_id": param.get("id", sensor_ctx["parameter_id"]),
        "parameter_name": param.get("name", sensor_ctx["parameter_name"]),
        "units": param.get("units", sensor_ctx["units"]),
        "datetime_utc": dt_to,
        "value": record.get("value"),
    }


def fetch_hours_for_sensor(sensor_ctx: dict, dt_from: str, dt_to: str):
    """
    Fetch hourly measurements for a single sensor over a time range.
    Returns a list of flattened rows.
    """
    sensor_id = sensor_ctx["sensor_id"]
    if sensor_id is None:
        return []

    all_rows = []
    page = 1

    while True:
        params = {
            "datetime_from": dt_from,
            "datetime_to": dt_to,
            "limit": PAGE_SIZE_HOURS,
            "page": page,
        }
        headers = {"X-API-Key": api_key}

        url = f"{API_BASE_URL}/sensors/{sensor_id}/hours"
        resp = requests.get(url, headers=headers, params=params)
        # if resp.status_code != 200:
        #     print(f"Error fetching hours for sensor {sensor_id} page {page}: {resp.status_code} - {resp.text}")
        #     break

        # Backoff + retry for error 429
        for attempt in range(3):
            resp = requests.get(url, headers=headers, params=params)
            if resp.status_code == 200:
                break
            elif resp.status_code == 429:
                wait_secs = 5 * (attempt + 1)
                print(f"Rate limited for sensor {sensor_id}, attempt {attempt+1}/3. Sleeping {wait_secs}s...")
                sleep(wait_secs)
            else:
                print(f"Error fetching hours for sensor {sensor_id} page {page}: {resp.status_code} - {resp.text}")
                return None  # give up on this sensor

        if resp.status_code != 200:
            # after 3 attempts still not 200
            return None

        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        for rec in results:
            all_rows.append(flatten_hour_record(rec, sensor_ctx))

        print(f"  Sensor {sensor_id}: fetched page {page} with {len(results)} hourly records.")
        page += 1
        sleep(1)  # a bit gentler for per-sensor calls

    return all_rows


def load_measurements_to_bigquery(rows: list, completed: bool):
    if not rows:
        print("No measurement rows collected; skipping BigQuery load.")
        return

    if not completed:
        print(
            f"Measurement ingestion did not complete (collected {len(rows)} rows); "
            "continuing with APPEND load (safe) to keep incremental progress."
        )
        return

    schema = [
        bigquery.SchemaField("sensor_id", "INT64"),
        bigquery.SchemaField("location_id", "INT64"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("parameter_id", "INT64"),
        bigquery.SchemaField("parameter_name", "STRING"),
        bigquery.SchemaField("units", "STRING"),
        bigquery.SchemaField("datetime_utc", "TIMESTAMP"),
        bigquery.SchemaField("value", "FLOAT64"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"Loading {len(rows)} rows into {table_ref} ...")
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    print("BigQuery load job completed.")

    table = client.get_table(table_ref)
    print(f"Table {table_ref} now has {table.num_rows} rows.")


# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    # Time window: last DAYS_BACK days
    dt_to = datetime.now(timezone.utc)
    dt_from = dt_to - timedelta(days=DAYS_BACK)

    dt_to_str = isoformat_utc(dt_to)
    dt_from_str = isoformat_utc(dt_from)

    print(f"Fetching hourly measurements from {dt_from_str} to {dt_to_str} ...")

    sensors = fetch_locations_with_sensors()
    if not sensors:
        print("No sensors discovered, exiting.")
        exit(0)

    all_measurements = []
    completed = True

    for idx, sensor_ctx in enumerate(sensors, start=1):
        print(f"Processing sensor {idx}/{len(sensors)} (sensor_id={sensor_ctx['sensor_id']})")
        sensor_rows = fetch_hours_for_sensor(sensor_ctx, dt_from_str, dt_to_str)

        if sensor_rows is None:
            completed = False
            print(f"Failed to fetch data for sensor {sensor_ctx['sensor_id']}; marking run incomplete.")
            break

        all_measurements.extend(sensor_rows)

    print(f"Total hourly measurement rows collected: {len(all_measurements)}")
    load_measurements_to_bigquery(all_measurements, completed)

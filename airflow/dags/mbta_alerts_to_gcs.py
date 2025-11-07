# # airflow/dags/mbta_alerts_to_gcs.py
# from __future__ import annotations

# import json
# from datetime import timedelta

# import pendulum
# import requests
# from airflow.decorators import dag, task
# from airflow.models import Variable
# from airflow.providers.google.cloud.hooks.gcs import GCSHook
# from airflow.operators.python import get_current_context

# # ──────────────────────────────────────────────────────────────────────────────
# # CONFIG — edit these to your environment
# # ──────────────────────────────────────────────────────────────────────────────
# API_BASE = "https://api-v3.mbta.com"           # MBTA V3 base URL
# BUCKET_NAME = "ba882-team10-bucket"            # <-- change if needed
# GCP_CONN_ID = "google_cloud_default"           # Airflow connection with Storage perms
# PREFIX = "mbta-dataset/alerts"                          # GCS folder/prefix
# TIMEZONE = "America/New_York"                   # schedule timezone
# SCHEDULE = "0 1 * * 6"                          # 1:00 AM every Saturday (cron)
# # Optional: store an MBTA API key as Airflow Variable "MBTA_API_KEY"
# #           or hardcode below (set to None if not using)
# MBTA_API_KEY = "4e3c51157a42404394aed06ee9a548bb"

# # ──────────────────────────────────────────────────────────────────────────────

# @dag(
#     dag_id="mbta_alerts_to_gcs",
#     schedule=SCHEDULE,
#     start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
#     catchup=False,
#     max_active_runs=1,
#     default_args={
#         "owner": "data-eng",
#         "retries": 2,
#         "retry_delay": timedelta(minutes=5),
#     },
#     tags=["mbta", "alerts", "gcs"],
# )
# def mbta_alerts_to_gcs():
#     """
#     Weekly job (Sat 1:00AM) that fetches MBTA /alerts and saves JSON to GCS.

#     Output path example:
#       gs://<bucket>/mbta/alerts/alerts_20251108.json
#     """

#     @task(retries=3, retry_delay=timedelta(minutes=2))
#     def fetch_alerts(api_base: str = API_BASE, api_key: str | None = MBTA_API_KEY) -> dict:
#         url = f"{api_base.rstrip('/')}/alerts"
#         params = {}
#         if api_key:
#             # MBTA accepts api_key as a query param
#             params["api_key"] = api_key

#         resp = requests.get(url, params=params, timeout=60)
#         resp.raise_for_status()
#         return resp.json()  # a Python dict that we’ll upload as JSON

#     @task()
#     def build_object_path(prefix: str = PREFIX) -> str:
#         ctx = get_current_context()
#         ds_nodash = ctx["ds_nodash"]  # e.g., 20251108
#         return f"{prefix}/alerts_{ds_nodash}.json"

#     @task()
#     def upload_to_gcs(data: dict, object_name: str, bucket: str = BUCKET_NAME, gcp_conn_id: str = GCP_CONN_ID) -> str:
#         hook = GCSHook(gcp_conn_id=gcp_conn_id)
#         hook.upload(
#             bucket_name=bucket,
#             object_name=object_name,
#             data=json.dumps(data),
#             mime_type="application/json",
#         )
#         return f"gs://{bucket}/{object_name}"

#     alerts = fetch_alerts()
#     object_name = build_object_path()
#     upload_to_gcs(alerts, object_name)

# mbta_alerts_to_gcs()

# airflow/dags/mbta_all_to_gcs.py
from __future__ import annotations
import json, requests
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import get_current_context

API_BASE   = "https://api-v3.mbta.com"
BUCKET     = "ba882-team10-bucket"
GCP_CONN   = "google_cloud_default"
PREFIX     = "mbta-dataset"                # we'll create subfolders per endpoint
TIMEZONE   = "America/New_York"
SCHEDULE   = "0 1 * * 6"                   # Sat 1:00am
API_KEY    = "4e3c51157a42404394aed06ee9a548bb"

ENDPOINTS = [
    "alerts","facilities","lines","live_facilities","predictions","routes",
    "route_patterns","schedules","services","shapes","stops","trips","vehicles"
]

@dag(
    dag_id="mbta_all_to_gcs",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    timezone=TIMEZONE,
    catchup=False,
    max_active_runs=1,
    default_args={"owner":"data-eng","retries":2,"retry_delay":timedelta(minutes=5)},
    tags=["mbta","gcs","snapshots"],
)
def mbta_all_to_gcs():

    @task
    def build_run_info():
        ctx = get_current_context()
        return {"ds_nodash": ctx["ds_nodash"]}

    @task.map
    def fetch_and_upload(endpoint: str, run_info: dict):
        # Fetch
        url = f"{API_BASE.rstrip('/')}/{endpoint}"
        params = {}
        if API_KEY:
            params["api_key"] = API_KEY
        r = requests.get(url, params=params, timeout=90)
        r.raise_for_status()
        payload = r.json()

        # Path: gs://bucket/mbta-dataset/<endpoint>/<endpoint>_YYYYMMDD.json
        object_name = f"{PREFIX}/{endpoint}/{endpoint}_{run_info['ds_nodash']}.json"

        # Upload (no large XComs)
        hook = GCSHook(gcp_conn_id=GCP_CONN)
        hook.upload(
            bucket_name=BUCKET,
            object_name=object_name,
            data=json.dumps(payload),
            mime_type="application/json",
            timeout=90,
        )
        return f"gs://{BUCKET}/{object_name}"

    info = build_run_info()
    fetch_and_upload.expand(endpoint=ENDPOINTS, run_info=[info]*len(ENDPOINTS))

mbta_all_to_gcs()

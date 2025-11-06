# airflow/dags/mbta_alerts_to_gcs.py
from __future__ import annotations

import json
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import get_current_context

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG — edit these to your environment
# ──────────────────────────────────────────────────────────────────────────────
API_BASE = "https://api-v3.mbta.com"           # MBTA V3 base URL
BUCKET_NAME = "ba882-team10-bucket"            # <-- change if needed
GCP_CONN_ID = "google_cloud_default"           # Airflow connection with Storage perms
PREFIX = "mbta-dataset/alerts"                          # GCS folder/prefix
TIMEZONE = "America/New_York"                   # schedule timezone
SCHEDULE = "0 1 * * 6"                          # 1:00 AM every Saturday (cron)
# Optional: store an MBTA API key as Airflow Variable "MBTA_API_KEY"
#           or hardcode below (set to None if not using)
MBTA_API_KEY = "4e3c51157a42404394aed06ee9a548bb"

# ──────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="mbta_alerts_to_gcs",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mbta", "alerts", "gcs"],
)
def mbta_alerts_to_gcs():
    """
    Weekly job (Sat 1:00AM) that fetches MBTA /alerts and saves JSON to GCS.

    Output path example:
      gs://<bucket>/mbta/alerts/alerts_20251108.json
    """

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def fetch_alerts(api_base: str = API_BASE, api_key: str | None = MBTA_API_KEY) -> dict:
        url = f"{api_base.rstrip('/')}/alerts"
        params = {}
        if api_key:
            # MBTA accepts api_key as a query param
            params["api_key"] = api_key

        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()  # a Python dict that we’ll upload as JSON

    @task()
    def build_object_path(prefix: str = PREFIX) -> str:
        ctx = get_current_context()
        ds_nodash = ctx["ds_nodash"]  # e.g., 20251108
        return f"{prefix}/alerts_{ds_nodash}.json"

    @task()
    def upload_to_gcs(data: dict, object_name: str, bucket: str = BUCKET_NAME, gcp_conn_id: str = GCP_CONN_ID) -> str:
        hook = GCSHook(gcp_conn_id=gcp_conn_id)
        hook.upload(
            bucket_name=bucket,
            object_name=object_name,
            data=json.dumps(data),
            mime_type="application/json",
        )
        return f"gs://{bucket}/{object_name}"

    alerts = fetch_alerts()
    object_name = build_object_path()
    upload_to_gcs(alerts, object_name)

mbta_alerts_to_gcs()

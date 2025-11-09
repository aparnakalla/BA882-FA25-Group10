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
PREFIX     = "mbta-dataset"
TIMEZONE   = "America/New_York"
SCHEDULE   = "0 6 * * 6"   # 06:00 UTC ≈ 1 AM ET (std time)
API_KEY    = "4e3c51157a42404394aed06ee9a548bb"

ENDPOINTS = [
    "alerts","facilities","lines","live_facilities","predictions","routes",
    "route_patterns","schedules","services","shapes","stops","trips","vehicles"
]

@dag(
    dag_id="mbta_all_to_gcs",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["mbta", "gcs", "snapshots"],
)
def mbta_all_to_gcs():

    @task
    def build_run_info():
        ctx = get_current_context()
        return {"ds_nodash": ctx["ds_nodash"]}

    def make_fetch_task(endpoint: str):
        @task(task_id=f"fetch_and_upload_{endpoint}")
        def _inner(run_info: dict, endpoint: str = endpoint):
            url = f"{API_BASE.rstrip('/')}/{endpoint}"
            params = {}
            if API_KEY:
                params["api_key"] = API_KEY
            r = requests.get(url, params=params, timeout=90)
            r.raise_for_status()
            payload = r.json()

            object_name = f"{PREFIX}/{endpoint}/{endpoint}_{run_info['ds_nodash']}.json"

            hook = GCSHook(gcp_conn_id=GCP_CONN)
            hook.upload(
                bucket_name=BUCKET,
                object_name=object_name,
                data=json.dumps(payload),
                mime_type="application/json",
                timeout=90,
            )
            return f"gs://{BUCKET}/{object_name}"

        return _inner

    info = build_run_info()

    # create one task per endpoint with a clear name
    for ep in ENDPOINTS:
        t = make_fetch_task(ep)
        t(info)

mbta_all_to_gcs()

# airflow/dags/mbta_all_to_gcs.py
from __future__ import annotations

import json
import requests
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import get_current_context

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────
API_BASE   = "https://api-v3.mbta.com"
BUCKET     = "ba882-team10-bucket"
GCP_CONN   = "google_cloud_default"
PREFIX     = "mbta-dataset"
TIMEZONE   = "America/New_York"
# Airflow cron is interpreted in UTC on your runtime:
# 0 6 * * 6  => Saturday 06:00 UTC (≈ 1:00 AM ET in standard time)
SCHEDULE   = "0 6 * * 6"
API_KEY    = "4e3c51157a42404394aed06ee9a548bb"  # or None if you don't want to send it

# Routes we care about for filter-required endpoints (subway + key bus + CR + ferry + SL)
ROUTES_OF_INTEREST = [
    # Subway (heavy + light rail)
    "Red", "Orange", "Blue",
    "Green-B", "Green-C", "Green-D", "Green-E",

    # Key bus routes
    "1", "15", "22", "23", "28", "32", "39", "57",
    "66", "71", "73", "77", "111", "116", "117",

    # Commuter rail
    "CR-Fairmount", "CR-Fitchburg", "CR-Franklin", "CR-Greenbush",
    "CR-Haverhill", "CR-Kingston", "CR-Lowell", "CR-Middleborough",
    "CR-Needham", "CR-Newburyport", "CR-Providence", "CR-Worcester",

    # Ferries
    "Boat-F1", "Boat-F4",

    # Silver Line (BRT)
    "741", "742", "743", "746", "749", "751",
]

# All endpoints we want to snapshot
ENDPOINTS = [
    "alerts",
    "facilities",
    "lines",
    "predictions",       # needs filters
    "routes",
    "route_patterns",
    "schedules",         # needs filters
    "shapes",            # needs filters
    "stops",
    "trips",             # needs filters
    "vehicles",
]


@dag(
    dag_id="mbta_all_to_gcs",
    schedule=SCHEDULE,  # interpreted as UTC cron
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mbta", "gcs", "snapshots"],
)
def mbta_all_to_gcs():
    """
    Weekly snapshot of multiple MBTA endpoints into GCS.

    For endpoints that require filters (predictions, schedules, shapes, trips),
    we iterate over ROUTES_OF_INTEREST and store one JSON file per route.
    """

    @task
    def build_run_info() -> dict:
        """Capture the logical execution date for filenames."""
        ctx = get_current_context()
        return {"ds_nodash": ctx["ds_nodash"]}  # e.g., "20251109"

    def make_fetch_task(endpoint: str):
        @task(task_id=f"fetch_and_upload_{endpoint}")
        def _inner(run_info: dict, endpoint: str = endpoint):
            ds_nodash = run_info["ds_nodash"]
            base_url = f"{API_BASE.rstrip('/')}/{endpoint}"

            # Endpoints that must be filtered: we snapshot per route
            filter_required = {"predictions", "schedules", "shapes", "trips"}

            if endpoint in filter_required:
                results = []
                for route_id in ROUTES_OF_INTEREST:
                    params = {"filter[route]": route_id}
                    if API_KEY:
                        params["api_key"] = API_KEY

                    resp = requests.get(base_url, params=params, timeout=90)
                    resp.raise_for_status()
                    payload = resp.json()

                    object_name = (
                        f"{PREFIX}/{endpoint}/route={route_id}/"
                        f"{endpoint}_{ds_nodash}.json"
                    )

                    hook = GCSHook(gcp_conn_id=GCP_CONN)
                    hook.upload(
                        bucket_name=BUCKET,
                        object_name=object_name,
                        data=json.dumps(payload),
                        mime_type="application/json",
                        timeout=90,
                    )

                    results.append(f"gs://{BUCKET}/{object_name}")

                return results  # list of URIs for this endpoint

            # Normal endpoints: one call, no required filters
            params = {}
            if API_KEY:
                params["api_key"] = API_KEY

            resp = requests.get(base_url, params=params, timeout=90)
            resp.raise_for_status()
            payload = resp.json()

            object_name = f"{PREFIX}/{endpoint}/{endpoint}_{ds_nodash}.json"

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

    run_info = build_run_info()

    # One clear task per endpoint
    for ep in ENDPOINTS:
        t = make_fetch_task(ep)
        t(run_info)


mbta_all_to_gcs()


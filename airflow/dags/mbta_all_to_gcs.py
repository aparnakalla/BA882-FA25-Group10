from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Dict, Any, List

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL CONFIG
# ─────────────────────────────────────────────────────────────────────────────

API_BASE = "https://api-v3.mbta.com"

ROUTES_OF_INTEREST = [
    # Subway
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
    # Silver Line
    "741", "742", "743", "746", "749", "751",
]

ENDPOINTS = [
    "alerts",
    "facilities",
    "lines",
    "predictions",       # filter[route]
    "routes",
    "route_patterns",
    "schedules",         # filter[route]
    "shapes",            # filter[route]
    "stops",
    "trips",             # filter[route]
    "vehicles",
]

FILTERED_ENDPOINTS = {"predictions", "schedules", "shapes", "trips"}

PROJECT_ID = os.environ.get("PROJECT_ID", "christina-ba882-fall25")
BQ_DATASET = os.environ.get("BQ_DATASET", "data_from_gcs_to_bq")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "ba882-team10-bucket")
PREFIX = "mbta-dataset"
GCP_CONN = "google_cloud_default"
TIMEZONE = "America/New_York"
API_KEY = os.environ.get("MBTA_API_KEY") or "4e3c51157a42404394aed06ee9a548bb"

# Cloud Function HTTP backfill endpoint
HTTP_CONN_ID = "mbta_cf_http"
CF_BACKFILL_PATH = "/load_all_existing_from_gcs"


# ─────────────────────────────────────────────────────────────────────────────
# DAG 1: SNAPSHOT MBTA ENDPOINTS → GCS (RAW JSON)
# ─────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="mbta_all_to_gcs",
    schedule="0 6 * * 6",  # weekly, Saturday 06:00 UTC
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
    iterate over ROUTES_OF_INTEREST and write one JSON file per route.
    """

    @task
    def build_run_info() -> dict:
        ctx = get_current_context()
        return {"ds_nodash": ctx["ds_nodash"]}

    def make_fetch_task(endpoint: str):
        @task(task_id=f"fetch_and_upload_{endpoint}")
        def _inner(run_info: dict, endpoint: str = endpoint):
            ds_nodash = run_info["ds_nodash"]
            base_url = f"{API_BASE.rstrip('/')}/{endpoint}"

            hook = GCSHook(gcp_conn_id=GCP_CONN)

            # endpoints that must be called per route
            if endpoint in FILTERED_ENDPOINTS:
                uris: List[str] = []
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

                    hook.upload(
                        bucket_name=BUCKET_NAME,
                        object_name=object_name,
                        data=json.dumps(payload),
                        mime_type="application/json",
                        timeout=90,
                    )

                    uris.append(f"gs://{BUCKET_NAME}/{object_name}")

                return uris

            # non-filtered endpoints
            params = {}
            if API_KEY:
                params["api_key"] = API_KEY

            resp = requests.get(base_url, params=params, timeout=90)
            resp.raise_for_status()
            payload = resp.json()

            object_name = f"{PREFIX}/{endpoint}/{endpoint}_{ds_nodash}.json"

            hook.upload(
                bucket_name=BUCKET_NAME,
                object_name=object_name,
                data=json.dumps(payload),
                mime_type="application/json",
                timeout=90,
            )

            return f"gs://{BUCKET_NAME}/{object_name}"

        return _inner

    run_info = build_run_info()

    for ep in ENDPOINTS:
        t = make_fetch_task(ep)
        t(run_info)


mbta_all_to_gcs_dag = mbta_all_to_gcs()


# ─────────────────────────────────────────────────────────────────────────────
# DAG 2: CREATE/REFRESH BIGQUERY EXTERNAL TABLES ON RAW GCS SNAPSHOTS
# ─────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="mbta_bq_external_tables",
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    schedule=None,  # run manually when schemas/paths change
    catchup=False,
    max_active_runs=1,
    tags=["mbta", "bigquery", "external"],
)
def mbta_bq_external_tables():
    """
    One external table per MBTA endpoint:

      data_from_gcs_to_bq.<endpoint>_ext

    Sources:
      gs://BUCKET_NAME/mbta-dataset/<endpoint>/*.json
      gs://BUCKET_NAME/mbta-dataset/<endpoint>/route=*/*.json
    """

    for ep in ENDPOINTS:
        if ep in FILTERED_ENDPOINTS:
            source_uri = f"gs://{BUCKET_NAME}/{PREFIX}/{ep}/route=*/*.json"
        else:
            source_uri = f"gs://{BUCKET_NAME}/{PREFIX}/{ep}/*.json"

        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{ep}_ext"

        BigQueryInsertJobOperator(
            task_id=f"create_ext_{ep}",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE EXTERNAL TABLE `{table_id}`
                        OPTIONS (
                          format = 'NEWLINE_DELIMITED_JSON',
                          uris = ['{source_uri}'],
                          autodetect = TRUE
                        )
                    """,
                    "useLegacySql": False,
                }
            },
        )


mbta_bq_external_tables_dag = mbta_bq_external_tables()


# ─────────────────────────────────────────────────────────────────────────────
# DAG 3: TRIGGER CLOUD FUNCTION BACKFILL → NATIVE BIGQUERY TABLES
# ─────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="mbta_cf_backfill_native",
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    schedule=None,  # on-demand backfill / rebuild
    catchup=False,
    max_active_runs=1,
    tags=["mbta", "cloud-function", "bigquery", "native"],
)
def mbta_cf_backfill_native():
    """
    Calls the HTTP-triggered Cloud Function that runs load_all_existing_from_gcs()
    to scan:
      gs://ba882-team10-bucket/mbta-dataset/*
    and upsert/insert into native flattened tables in BigQuery.
    """

    SimpleHttpOperator(
        task_id="trigger_load_all_existing_from_gcs",
        http_conn_id=HTTP_CONN_ID,
        endpoint=CF_BACKFILL_PATH.lstrip("/"),
        method="GET",
        log_response=True,
    )


mbta_cf_backfill_native_dag = mbta_cf_backfill_native()

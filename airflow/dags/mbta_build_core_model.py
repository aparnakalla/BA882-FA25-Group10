from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "christina-ba882-fall25")

# Native/raw tables (where your Cloud Function is writing now)
RAW_DATASET = os.environ.get("BQ_DATASET", "data_from_gcs_to_bq")

# Core/star-schema dataset for dims + facts
CORE_DATASET = os.environ.get("CORE_DATASET", "mbta_core")

TIMEZONE = "America/New_York"

# Use the new connection you created in the Airflow UI
GCP_BQ_CONN_ID = "google_cloud_default"


@dag(
    dag_id="mbta_build_core_model",
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    schedule=None,  # triggered from mbta_cf_backfill_native
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mbta", "core-model", "bigquery"],
)
def mbta_build_core_model():
    """
    Build core MBTA data model (dimensions + facts) in BigQuery.

    - Reads from native/raw tables in {PROJECT_ID}.{RAW_DATASET}
    - Deduplicates on (id, snapshot_date) using ROW_NUMBER()
    - Writes cleaned dims/facts into {PROJECT_ID}.{CORE_DATASET}
    """

    # ───────────── DIMENSIONS ─────────────

    dim_line = BigQueryInsertJobOperator(
        task_id="build_dim_line",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_line` AS
                    SELECT DISTINCT
                      id          AS line_id,
                      type        AS line_type,
                      long_name   AS line_long_name,
                      short_name  AS line_short_name,
                      color,
                      sort_order,
                      text_color,
                      snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{RAW_DATASET}.lines`;
                """,
                "useLegacySql": False,
            }
        },
    )

    dim_route = BigQueryInsertJobOperator(
        task_id="build_dim_route",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_route` AS
                    SELECT DISTINCT
                      id           AS route_id,
                      type         AS route_type,
                      color        AS route_color,
                      text_color   AS route_text_color,
                      long_name    AS route_long_name,
                      short_name   AS route_short_name,
                      description  AS route_description,
                      line_id      AS line_id,
                      fare_class,
                      listed_route,
                      sort_order,
                      snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{RAW_DATASET}.routes`;
                """,
                "useLegacySql": False,
            }
        },
    )

    dim_stop = BigQueryInsertJobOperator(
        task_id="build_dim_stop",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_stop` AS
                    SELECT DISTINCT
                      id            AS stop_id,
                      name          AS stop_name,
                      municipality,
                      latitude,
                      longitude,
                      zone_id,
                      parent_station_id,
                      platform_name,
                      vehicle_type,
                      snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{RAW_DATASET}.stops`;
                """,
                "useLegacySql": False,
            }
        },
    )

    dim_trip = BigQueryInsertJobOperator(
        task_id="build_dim_trip",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_trip` AS
                    SELECT DISTINCT
                      id             AS trip_id,
                      route_id,
                      direction_id,
                      headsign,
                      service_id,
                      route_pattern_id,
                      shape_id,
                      bikes_allowed,
                      wheelchair_accessible,
                      snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{RAW_DATASET}.trips`;
                """,
                "useLegacySql": False,
            }
        },
    )

    dim_route_pattern = BigQueryInsertJobOperator(
        task_id="build_dim_route_pattern",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_route_pattern` AS
                    SELECT DISTINCT
                      id                  AS route_pattern_id,
                      route_id,
                      direction_id,
                      name,
                      canonical,
                      typicality,
                      time_desc,
                      representative_trip_id,
                      snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{RAW_DATASET}.route_patterns`;
                """,
                "useLegacySql": False,
            }
        },
    )

    dim_shape = BigQueryInsertJobOperator(
        task_id="build_dim_shape",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_shape` AS
                    SELECT DISTINCT
                      id        AS shape_id,
                      polyline,
                      route_hint,
                      snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{RAW_DATASET}.shapes`;
                """,
                "useLegacySql": False,
            }
        },
    )

    dim_facility = BigQueryInsertJobOperator(
        task_id="build_dim_facility",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_facility` AS
                    SELECT DISTINCT
                      id         AS facility_id,
                      type       AS facility_type,
                      stop_id,
                      short_name,
                      long_name,
                      latitude,
                      longitude,
                      properties,
                      snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{RAW_DATASET}.facilities`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # ───────────── FACTS (WITH DEDUP) ─────────────

    fact_schedule = BigQueryInsertJobOperator(
        task_id="build_fact_schedule_stop",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_schedule_stop` AS
                    WITH ranked AS (
                      SELECT
                        *,
                        ROW_NUMBER() OVER (
                          PARTITION BY id, snapshot_date
                          ORDER BY ingest_ts DESC
                        ) AS rn
                      FROM `{PROJECT_ID}.{RAW_DATASET}.schedules`
                    )
                    SELECT
                      id              AS schedule_id,
                      trip_id,
                      route_id,
                      stop_id,
                      direction_id,
                      stop_sequence,
                      arrival_time    AS arrival_time_scheduled,
                      departure_time  AS departure_time_scheduled,
                      pickup_type,
                      drop_off_type,
                      stop_headsign,
                      timepoint,
                      route_hint,
                      snapshot_date,
                      ingest_ts
                    FROM ranked
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    fact_prediction = BigQueryInsertJobOperator(
        task_id="build_fact_prediction_stop",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_prediction_stop` AS
                    WITH ranked AS (
                      SELECT
                        *,
                        ROW_NUMBER() OVER (
                          PARTITION BY id, snapshot_date
                          ORDER BY ingest_ts DESC
                        ) AS rn
                      FROM `{PROJECT_ID}.{RAW_DATASET}.predictions`
                    )
                    SELECT
                      id                   AS prediction_id,
                      TIMESTAMP(ingest_ts) AS prediction_ts,
                      trip_id,
                      route_id,
                      stop_id,
                      stop_sequence,
                      direction_id,
                      arrival_time    AS arrival_time_predicted,
                      departure_time  AS departure_time_predicted,
                      schedule_relationship,
                      status,
                      update_type,
                      revenue,
                      route_hint,
                      snapshot_date,
                      ingest_ts
                    FROM ranked
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    fact_vehicle = BigQueryInsertJobOperator(
        task_id="build_fact_vehicle_position",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_vehicle_position` AS
                    WITH ranked AS (
                      SELECT
                        *,
                        ROW_NUMBER() OVER (
                          PARTITION BY id, updated_at, snapshot_date
                          ORDER BY ingest_ts DESC
                        ) AS rn
                      FROM `{PROJECT_ID}.{RAW_DATASET}.vehicles`
                    )
                    SELECT
                      id                         AS vehicle_id,
                      TIMESTAMP(updated_at)      AS event_ts,
                      trip_id,
                      route_id,
                      stop_id,
                      direction_id,
                      SAFE_CAST(latitude AS FLOAT64)  AS latitude,
                      SAFE_CAST(longitude AS FLOAT64) AS longitude,
                      SAFE_CAST(speed AS FLOAT64)     AS speed,
                      bearing,
                      current_status,
                      current_stop_sequence,
                      occupancy_status,
                      revenue,
                      label,
                      snapshot_date,
                      ingest_ts
                    FROM ranked
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    fact_alert = BigQueryInsertJobOperator(
        task_id="build_fact_alert",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_alert` AS
                    WITH ranked AS (
                      SELECT
                        *,
                        ROW_NUMBER() OVER (
                          PARTITION BY id, snapshot_date
                          ORDER BY ingest_ts DESC
                        ) AS rn
                      FROM `{PROJECT_ID}.{RAW_DATASET}.alerts`
                    )
                    SELECT
                      id              AS alert_id,
                      type,
                      cause,
                      effect,
                      severity,
                      lifecycle,
                      timeframe,
                      TIMESTAMP(created_at)  AS created_at,
                      TIMESTAMP(updated_at)  AS updated_at,
                      description,
                      short_header,
                      header,
                      service_effect,
                      active_period,
                      informed_entity,
                      closed_timestamp,
                      duration_certainty,
                      snapshot_date,
                      ingest_ts
                    FROM ranked
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    # fact_delay_stop: join schedule + prediction, compute delay_seconds + label
    fact_delay = BigQueryInsertJobOperator(
        task_id="build_fact_delay_stop",
        gcp_conn_id=GCP_BQ_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_delay_stop` AS
                    WITH joined AS (
                      SELECT
                        p.prediction_id,
                        p.prediction_ts,
                        p.route_id,
                        p.trip_id,
                        p.stop_id,
                        p.stop_sequence,
                        p.direction_id,
                        p.arrival_time_predicted,
                        s.arrival_time_scheduled,
                        p.snapshot_date,
                        p.ingest_ts
                      FROM `{PROJECT_ID}.{CORE_DATASET}.fact_prediction_stop` p
                      LEFT JOIN `{PROJECT_ID}.{CORE_DATASET}.fact_schedule_stop` s
                        USING (trip_id, route_id, stop_id, stop_sequence, direction_id, snapshot_date)
                    )
                    SELECT
                      DATE(prediction_ts) AS service_date,
                      route_id,
                      trip_id,
                      stop_id,
                      direction_id,
                      stop_sequence,
                      prediction_ts,
                      arrival_time_scheduled,
                      arrival_time_predicted,
                      TIMESTAMP_DIFF(
                        TIMESTAMP(CONCAT(CAST(snapshot_date AS STRING), ' ', arrival_time_predicted)),
                        TIMESTAMP(CONCAT(CAST(snapshot_date AS STRING), ' ', arrival_time_scheduled)),
                        SECOND
                      ) AS delay_seconds,
                      CASE WHEN
                        TIMESTAMP_DIFF(
                          TIMESTAMP(CONCAT(CAST(snapshot_date AS STRING), ' ', arrival_time_predicted)),
                          TIMESTAMP(CONCAT(CAST(snapshot_date AS STRING), ' ', arrival_time_scheduled)),
                          SECOND
                        ) > 300
                      THEN 1 ELSE 0 END AS is_delayed_5min,
                      snapshot_date,
                      ingest_ts
                    FROM joined
                    WHERE arrival_time_scheduled IS NOT NULL
                      AND arrival_time_predicted IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
    )

    # ───────────── DEPENDENCIES ─────────────

    core_dims = [
        dim_line,
        dim_route,
        dim_stop,
        dim_trip,
        dim_route_pattern,
        dim_shape,
        dim_facility,
    ]

    core_facts = [
        fact_schedule,
        fact_prediction,
        fact_vehicle,
        fact_alert,
    ]

    # All dimensions must complete before each core fact
    for f in core_facts:
        core_dims >> f

    # All core facts must complete before delay fact
    core_facts >> fact_delay


mbta_build_core_model_dag = mbta_build_core_model()
